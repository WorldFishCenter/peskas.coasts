#' Prepare Predicted Tracks for Effort Aggregation
#'
#' @description
#' Enriches predicted fishing track points with time-based effort, H3 cell
#' assignment, and per-trip total hours (used downstream for the fidelity
#' metric). For each trip the inter-ping time interval (`dt_hours`) is
#' computed from consecutive timestamps and capped at 4 hours to guard against
#' GPS-off gaps inflating effort estimates.
#'
#' @param df Data frame of predicted fishing points with columns `trip`,
#'   `timestamp`, `latitude`, `longitude`.
#' @param h3_res Integer (0–15). H3 resolution for cell assignment.
#'
#' @return The input data frame with additional columns:
#'   \describe{
#'     \item{`year`}{Integer year extracted from `timestamp`.}
#'     \item{`dt_hours`}{Interval in hours to the previous ping within the
#'       same trip (0 for the first ping; capped at 4).}
#'     \item{`h3_index`}{H3 cell identifier at resolution `h3_res`.}
#'     \item{`trip_total_hours`}{Total fishing hours for the trip across all
#'       H3 cells (sum of `dt_hours` within the trip). Used to compute the
#'       per-cell trip share for the fidelity metric.}
#'   }
#'
#' @keywords internal
prepare_tracks_for_effort <- function(df, h3_res) {
  df |>
    dplyr::mutate(
      trip = as.character(.data$trip),
      timestamp = lubridate::as_datetime(.data$timestamp),
      year = lubridate::year(.data$timestamp)
    ) |>
    dplyr::arrange(.data$trip, .data$timestamp) |>
    dplyr::group_by(.data$trip) |>
    dplyr::mutate(
      dt_hours = as.numeric(
        difftime(.data$timestamp, dplyr::lag(.data$timestamp), units = "hours")
      ),
      dt_hours = dplyr::coalesce(.data$dt_hours, 0),
      dt_hours = pmin(.data$dt_hours, 4),
      trip_total_hours = sum(.data$dt_hours, na.rm = TRUE)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      h3_index = h3jsr::point_to_cell(
        cbind(.data$longitude, .data$latitude),
        res = h3_res
      )
    )
}


#' Aggregate Predicted Fishing Tracks into an H3 Effort Grid
#'
#' @description
#' Downloads all per-trip predicted fishing track files produced by
#' [predict_pds_tracks()] and aggregates them into an H3 hexagonal grid of
#' cumulative fishing effort. The result is uploaded as a versioned parquet
#' file to the country-level cloud storage bucket.
#'
#' @details
#' Predicted track files contain fishing-only GPS points (columns: `trip`,
#' `timestamp`, `latitude`, `longitude`). This function:
#'
#' 1. Lists all files under `conf$pds$pds_tracks_predicted$file_prefix` in the
#'    PDS bucket.
#' 2. Downloads only **new** files (incremental via manifest) in parallel using
#'    `furrr`.
#' 3. Prepares each track with [prepare_tracks_for_effort()], which computes
#'    per-ping time intervals (`dt_hours`), assigns H3 cell indices, and
#'    records per-trip total hours for fidelity computation.
#' 4. Runs a **two-pass aggregation**: first a trip × cell summary to compute
#'    the fidelity components (`avg_fidelity_sum`, `n_trips_for_fidelity`),
#'    then a cell-level summary for effort totals.
#' 5. Merges with the previously stored grid and uploads the updated version.
#'
#' ## Grid schema
#'
#' The grid includes a `year` column for temporal effort maps (see
#' [plot_effort_map()]). An all-time aggregate is obtained by summing over
#' `year`. Primary effort columns:
#'
#' - `fishing_hours`: accumulated fishing time (sum of capped inter-ping
#'   intervals). This is the primary effort metric.
#' - `unique_trips`: count of distinct trips contributing to the cell.
#' - `n_active_days`: count of distinct calendar days with fishing activity.
#' - `first_active_date` / `last_active_date`: date range for inferring the
#'   study period length (`n_total_days`) downstream.
#' - `avg_fidelity_sum`: sum of per-trip fidelity values (fraction of each
#'   trip's total fishing hours spent in this cell). Divide by
#'   `n_trips_for_fidelity` to get `avg_fidelity` ∈ [0, 1].
#' - `n_trips_for_fidelity`: number of trips contributing to `avg_fidelity_sum`.
#' - `fishing_pings`: raw GPS point count (retained for QA; not used as a
#'   primary metric because ping frequency is irregular).
#'
#' **Multi-resolution support:** passing different `h3_res` values writes to
#' separate cloud prefixes (e.g. `predicted-pds-h3_grid_r9`,
#' `predicted-pds-h3_grid_r7`), so grids at multiple resolutions can coexist.
#' Use [rollup_h3_resolution()] to derive coarser views from a stored fine grid,
#' or pass a coarser `h3_res` directly to recompute from raw tracks.
#' [derive_fishing_grounds()] can further roll up to any resolution before
#' extracting contiguous fishing ground polygons.
#'
#' @param log_threshold The logging threshold to use. Default is `logger::DEBUG`.
#' @param h3_res Integer (0–15). H3 resolution level for the output grid.
#'   Default is `9` (~174 m edge length). Different resolutions write to
#'   separate cloud prefixes.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return Invisibly returns the merged H3 grid data frame (columns:
#'   `h3_index`, `year`, `fishing_hours`, `unique_trips`, `n_active_days`,
#'   `first_active_date`, `last_active_date`, `avg_fidelity_sum`,
#'   `n_trips_for_fidelity`, `fishing_pings`), or `NULL` if there was nothing
#'   to process.
#'
#' @seealso [predict_pds_tracks()], [derive_fishing_grounds()],
#'   [rollup_h3_resolution()], [plot_effort_map()]
#'
#' @keywords workflow modeling
#' @export
aggregate_pds_effort <- function(
  log_threshold = logger::DEBUG,
  h3_res = 9L,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  pds_opts <- resolve_storage_opts(conf, "pds")
  country_opts <- resolve_storage_opts(conf, "country")

  file_prefix <- conf$pds$pds_tracks_predicted$file_prefix
  # Resolution-specific prefix so grids at different h3_res don't share manifests
  grid_prefix <- paste0(conf$pds$pds_tracks_h3_grid$file_prefix, "_r", h3_res)
  manifest_name <- paste0(grid_prefix, "/aggregated_manifest.rds")

  # --- Model version ---
  model_version <- ssfaitk::ssfaitk_version()[[1]]
  logger::log_info("Model version: {model_version}")

  logger::log_info("Listing predicted track files...")
  cloud_storage_authenticate(conf$pds_storage$google$key, pds_opts)

  predicted_files <- googleCloudStorageR::gcs_list_objects(
    bucket = pds_opts$bucket,
    prefix = file_prefix
  )$name

  if (length(predicted_files) == 0) {
    logger::log_info("No predicted tracks found in bucket")
    return(invisible(NULL))
  }

  # --- Load existing grid and manifest if available ---
  existing_grid <- NULL
  already_aggregated <- character(0)

  manifest_local <- file.path(tempdir(), "aggregated_manifest.rds")
  grid_local <- file.path(tempdir(), "existing_grid.parquet")

  tryCatch(
    {
      download_cloud_file(
        name = manifest_name,
        provider = conf$storage$google$key,
        options = country_opts,
        file = manifest_local
      )
      already_aggregated <- readr::read_rds(manifest_local)

      grid_cloud_name <- cloud_object_name(
        prefix = grid_prefix,
        provider = conf$storage$google$key,
        version = "latest",
        extension = "parquet",
        options = country_opts
      )
      download_cloud_file(
        name = grid_cloud_name,
        provider = conf$storage$google$key,
        options = country_opts,
        file = grid_local
      )
      existing_grid <- arrow::read_parquet(grid_local)
      logger::log_info("Loaded existing grid with {nrow(existing_grid)} rows")
    },
    error = function(e) {
      logger::log_info("No existing grid found, building from scratch")
    }
  )

  # --- Detect version change → force full rebuild ---
  version_changed <- length(already_aggregated) > 0 &&
    !any(grepl(paste0("v", model_version), already_aggregated))

  if (version_changed) {
    logger::log_info(
      "Model version changed to v{model_version}, rebuilding grid from scratch"
    )
    already_aggregated <- character(0)
    existing_grid <- NULL
  }

  # --- Filter to only new files ---
  new_files <- setdiff(predicted_files, already_aggregated)

  if (length(new_files) == 0) {
    logger::log_info("No new tracks to aggregate, grid is up to date")
    return(invisible(NULL))
  }

  logger::log_info(
    "{length(new_files)} new files to aggregate (skipping {length(already_aggregated)} already done)"
  )

  # --- Download only new files ---
  workers <- parallel::detectCores() - 1
  logger::log_info("Downloading new tracks with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  new_tracks <- furrr::future_map(
    new_files,
    function(f) {
      local_file <- file.path(tempdir(), basename(f))
      tryCatch(
        {
          download_cloud_file(
            name = f,
            provider = conf$pds_storage$google$key,
            options = pds_opts,
            file = local_file
          )
          data <- arrow::read_parquet(local_file)
          unlink(local_file)
          data
        },
        error = function(e) {
          logger::log_warn("Skipping {f}: {conditionMessage(e)}")
          NULL
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  ) |>
    purrr::compact() |>
    dplyr::bind_rows()

  future::plan(future::sequential)

  # Force HTTP/1.1 to avoid HTTP/2 framing errors on GCS uploads in CI.
  # curl constant: CURL_HTTP_VERSION_1_1 = 2 (not HTTP/2 which would be 3).
  httr::set_config(httr::config(http_version = 2L))

  if (nrow(new_tracks) == 0) {
    logger::log_info("All new files were empty, nothing to aggregate")
    saveRDS(c(already_aggregated, new_files), manifest_local)
    upload_cloud_file(
      file = manifest_local,
      name = manifest_name,
      provider = conf$storage$google$key,
      options = country_opts
    )
    unlink(manifest_local)
    return(invisible(NULL))
  }

  n_trips <- dplyr::n_distinct(new_tracks$trip)
  logger::log_info(
    "Aggregating {nrow(new_tracks)} new fishing points from {n_trips} trips to H3 res {h3_res}"
  )

  # --- Prepare tracks: compute dt_hours, year, h3_index, trip_total_hours ---
  prepared <- prepare_tracks_for_effort(new_tracks, h3_res)

  # Step 1: trip × cell summary — needed to compute per-trip fidelity (avg_trip_share)
  trip_cell <- prepared |>
    dplyr::group_by(.data$trip, .data$h3_index, .data$year) |>
    dplyr::summarise(
      cell_hours = sum(.data$dt_hours, na.rm = TRUE),
      trip_total = dplyr::first(.data$trip_total_hours),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      trip_share = dplyr::if_else(
        .data$trip_total > 0,
        .data$cell_hours / .data$trip_total,
        NA_real_
      )
    )

  # Fidelity per cell: sum and count of trip_share (stored raw for correct merge)
  cell_fidelity <- trip_cell |>
    dplyr::group_by(.data$h3_index, .data$year) |>
    dplyr::summarise(
      avg_fidelity_sum = sum(.data$trip_share, na.rm = TRUE),
      n_trips_for_fidelity = sum(!is.na(.data$trip_share)),
      .groups = "drop"
    )

  # Step 2: main cell aggregation from full prepared data
  new_grid <- prepared |>
    dplyr::group_by(.data$h3_index, .data$year) |>
    dplyr::summarise(
      fishing_hours = sum(.data$dt_hours, na.rm = TRUE),
      unique_trips = dplyr::n_distinct(.data$trip),
      n_active_days = dplyr::n_distinct(as.Date(.data$timestamp)),
      first_active_date = min(as.Date(.data$timestamp)),
      last_active_date = max(as.Date(.data$timestamp)),
      fishing_pings = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::left_join(cell_fidelity, by = c("h3_index", "year"))

  # --- Merge with existing grid ---
  if (!is.null(existing_grid) && nrow(existing_grid) > 0) {
    logger::log_info(
      "Merging with existing grid ({nrow(existing_grid)} rows)"
    )

    h3_grid <- dplyr::bind_rows(existing_grid, new_grid) |>
      dplyr::group_by(.data$h3_index, .data$year) |>
      dplyr::summarise(
        fishing_hours = sum(.data$fishing_hours),
        unique_trips = sum(.data$unique_trips),
        n_active_days = sum(.data$n_active_days, na.rm = TRUE),
        first_active_date = min(.data$first_active_date, na.rm = TRUE),
        last_active_date = max(.data$last_active_date, na.rm = TRUE),
        avg_fidelity_sum = sum(.data$avg_fidelity_sum, na.rm = TRUE),
        n_trips_for_fidelity = sum(.data$n_trips_for_fidelity, na.rm = TRUE),
        fishing_pings = sum(.data$fishing_pings),
        .groups = "drop"
      )
  } else {
    h3_grid <- new_grid
  }

  # --- Upload updated grid ---
  output_filename <- grid_prefix |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    h3_grid,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  n_cells <- dplyr::n_distinct(h3_grid$h3_index)
  logger::log_info(
    "Uploading H3 grid ({n_cells} cells, {nrow(h3_grid)} rows) to cloud storage..."
  )
  upload_cloud_file(
    file = output_filename,
    provider = conf$storage$google$key,
    options = country_opts
  )
  unlink(output_filename)

  # --- Upload updated manifest ---
  saveRDS(c(already_aggregated, new_files), manifest_local)
  upload_cloud_file(
    file = manifest_local,
    name = manifest_name,
    provider = conf$storage$google$key,
    options = country_opts
  )
  unlink(manifest_local)

  logger::log_success(
    "H3 grid updated: {n_cells} cells ({length(new_files)} new files aggregated)"
  )

  invisible(h3_grid)
}


#' Project Fishing GPS Points to a Metric CRS
#'
#' @description
#' Converts a data frame of GPS fishing observations to a projected `sf` POINT
#' object. Rows with missing coordinates are dropped. The result is in a metric
#' CRS suitable for distance-based operations such as grid creation and spatial
#' joins.
#'
#' @param df A data frame containing GPS fishing point records.
#' @param lat_col Character. Name of the latitude column. Default is `"lat"`.
#' @param lon_col Character. Name of the longitude column. Default is `"lon"`.
#' @param crs_projected Integer. EPSG code of the target projected CRS.
#'   Default is `32632` (UTM zone 32N). Choose a zone that covers your study
#'   area for accurate metric distances.
#'
#' @return An `sf` POINT object in the requested projected CRS.
#'
#' @seealso [create_reference_grid()], [aggregate_daily_effort()]
#'
#' @keywords preprocessing
#' @export
prep_fishing_points <- function(
  df,
  lat_col = "lat",
  lon_col = "lon",
  crs_projected = 32632
) {
  if (!all(c(lat_col, lon_col) %in% names(df))) {
    stop(
      "Latitude or longitude columns not found: ",
      paste(c(lat_col, lon_col), collapse = ", ")
    )
  }

  df |>
    dplyr::filter(!is.na(.data[[lat_col]]) & !is.na(.data[[lon_col]])) |>
    sf::st_as_sf(coords = c(lon_col, lat_col), crs = 4326) |>
    sf::st_transform(crs = crs_projected)
}


#' Create a Deterministic Reference Grid
#'
#' @description
#' Generates a regular square or hexagonal spatial grid over a study area
#' bounding box and assigns each cell a stable unique identifier. Create the
#' grid once and reuse it across pipeline runs so that `cell_id` values remain
#' consistent over time.
#'
#' @param study_area_bbox An `sf` polygon or bounding box defining the spatial
#'   extent of the grid.
#' @param cell_size_meters Numeric. Grid cell size in metres. Default is `500`.
#' @param hex Logical. If `TRUE` (default), creates hexagonal cells; if
#'   `FALSE`, creates square cells.
#'
#' @return An `sf` polygon object with a `cell_id` column containing a unique
#'   identifier for each cell (format: `"GRID_<n>"`).
#'
#' @seealso [prep_fishing_points()], [aggregate_daily_effort()]
#'
#' @keywords preprocessing
#' @export
create_reference_grid <- function(
  study_area_bbox,
  cell_size_meters = 500,
  hex = TRUE
) {
  grid_sfc <- sf::st_make_grid(
    study_area_bbox,
    cellsize = cell_size_meters,
    square = !hex
  )

  sf::st_sf(
    cell_id = paste0("GRID_", seq_along(grid_sfc)),
    geometry = grid_sfc
  )
}


#' Aggregate GPS Points to a Reference Grid
#'
#' @description
#' Spatially joins projected GPS fishing points to a reference grid and counts
#' the number of fishing pings per cell. Points that fall outside the grid
#' extent are silently dropped.
#'
#' @param points_sf Projected `sf` POINT object, as returned by
#'   [prep_fishing_points()].
#' @param reference_grid_sf `sf` polygon grid, as returned by
#'   [create_reference_grid()].
#'
#' @return A data frame with columns `cell_id` and `fishing_pings`.
#'
#' @seealso [prep_fishing_points()], [create_reference_grid()]
#'
#' @keywords preprocessing
#' @export
aggregate_daily_effort <- function(points_sf, reference_grid_sf) {
  sf::st_join(points_sf, reference_grid_sf, join = sf::st_intersects) |>
    sf::st_drop_geometry() |>
    dplyr::filter(!is.na(.data$cell_id)) |>
    dplyr::group_by(.data$cell_id) |>
    dplyr::summarise(fishing_pings = dplyr::n(), .groups = "drop")
}


#' Assign H3 Hexagon Indices to GPS Points
#'
#' @description
#' Adds an `h3_index` column to a GPS data frame by mapping each coordinate to
#' its containing H3 hexagon at the specified resolution. Rows with missing
#' coordinates are dropped. The data frame is returned in its original
#' unprojected (WGS84) form with the index appended.
#'
#' @param df A data frame with GPS coordinates.
#' @param lat_col Character. Name of the latitude column. Default is `"lat"`.
#' @param lon_col Character. Name of the longitude column. Default is `"lon"`.
#' @param h3_res Integer (0–15). H3 resolution level. Default is `9`
#'   (~174 m edge length). Higher values produce smaller, finer cells.
#'
#' @return The input data frame (minus rows with missing coordinates) with an
#'   additional `h3_index` character column.
#'
#' @seealso [aggregate_h3_effort()], [rollup_h3_resolution()]
#'
#' @keywords preprocessing
#' @export
assign_h3_indices <- function(
  df,
  lat_col = "lat",
  lon_col = "lon",
  h3_res = 9
) {
  df_clean <- df |>
    dplyr::filter(!is.na(.data[[lat_col]]) & !is.na(.data[[lon_col]]))

  points_sf <- sf::st_as_sf(df_clean, coords = c(lon_col, lat_col), crs = 4326)
  df_clean$h3_index <- h3jsr::point_to_cell(points_sf, res = h3_res)
  df_clean
}


#' Aggregate Fishing Effort by H3 Hexagon
#'
#' @description
#' Summarises GPS fishing pings by H3 hexagon index, computing total ping
#' count and number of unique trips per cell.
#'
#' @param df_with_h3 A data frame with an `h3_index` column and a `Trip`
#'   column, as produced by [assign_h3_indices()].
#'
#' @return A data frame with columns `h3_index`, `fishing_pings`, and
#'   `unique_vessels`.
#'
#' @seealso [assign_h3_indices()], [rollup_h3_resolution()]
#'
#' @keywords preprocessing
#' @export
aggregate_h3_effort <- function(df_with_h3) {
  df_with_h3 |>
    dplyr::group_by(.data$h3_index) |>
    dplyr::summarise(
      fishing_pings = dplyr::n(),
      unique_vessels = dplyr::n_distinct(.data$Trip),
      .groups = "drop"
    )
}


#' Roll Up H3 Fishing Effort to a Coarser Resolution
#'
#' @description
#' Re-aggregates fishing effort from a fine H3 resolution to a coarser parent
#' resolution by mapping each cell to its containing parent hexagon and summing
#' ping counts. Useful for multi-scale analysis without rerunning the full
#' spatial join.
#'
#' @param aggregated_df A data frame with columns `h3_index` and
#'   `fishing_pings`, as returned by [aggregate_h3_effort()].
#' @param target_res Integer. The target H3 resolution. Must be lower (coarser)
#'   than the resolution used to create `aggregated_df`.
#'
#' @return A data frame with columns `parent_h3_index` and
#'   `total_fishing_pings`.
#'
#' @seealso [assign_h3_indices()], [aggregate_h3_effort()]
#'
#' @keywords preprocessing
#' @export
rollup_h3_resolution <- function(aggregated_df, target_res) {
  aggregated_df |>
    dplyr::mutate(
      parent_h3_index = h3jsr::get_parent(.data$h3_index, res = target_res)
    ) |>
    dplyr::group_by(.data$parent_h3_index) |>
    dplyr::summarise(
      total_fishing_pings = sum(.data$fishing_pings),
      .groups = "drop"
    )
}

#' Convert an H3 Effort Summary to a Spatial Grid
#'
#' @description
#' Attaches hexagonal polygon geometries to an H3 effort summary table,
#' returning an `sf` object ready for mapping or further spatial analysis.
#' Polygons are derived from the `h3_index` column using WGS84 (EPSG 4326).
#'
#' @param h3_summary_df A data frame with an `h3_index` column, as returned by
#'   [aggregate_h3_effort()] or [rollup_h3_resolution()]. All other columns are
#'   preserved in the output.
#'
#' @return An `sf` polygon object in WGS84 (EPSG 4326) with one row per H3
#'   cell and a `geometry` column containing the hexagon boundaries.
#'
#' @seealso [aggregate_h3_effort()], [rollup_h3_resolution()]
#'
#' @keywords preprocessing
#' @export
create_spatial_grid <- function(h3_summary_df = NULL) {
  hex_geoms <- h3jsr::cell_to_polygon(h3_summary_df$h3_index, simple = TRUE)
  sf_grid <- sf::st_sf(h3_summary_df, geometry = hex_geoms, crs = 4326)
  return(sf_grid)
}
