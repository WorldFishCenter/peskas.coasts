#' Fetch and Clean a Single PDS Track for Fishing Prediction
#'
#' @param trip_id Character. Trip ID to fetch.
#' @param conf Configuration list, as returned by [read_config()].
#' @return A cleaned data frame with columns `trip`, `timestamp`, `latitude`,
#'   `longitude`, or `NULL` if the track is unavailable or too short.
#' @keywords internal
#' @export

fetch_track_for_prediction <- function(trip_id, conf) {
  tryCatch(
    {
      raw <- get_trip_points(
        token = conf$pds$token,
        secret = conf$pds$secret,
        id = trip_id,
        deviceInfo = TRUE
      )

      if (is.null(raw) || nrow(raw) == 0) {
        return(NULL)
      }

      cleaned <- raw |>
        janitor::clean_names() |>
        dplyr::select(
          "trip",
          timestamp = "time",
          latitude = "lat",
          longitude = "lng"
        )

      if (nrow(cleaned) < 5) {
        return(NULL)
      }

      cleaned
    },
    error = function(e) {
      logger::log_error("Trip {trip_id}: fetch failed - {conditionMessage(e)}")
      NULL
    }
  )
}


#' Predict Fishing Activity and Upload Results for a Single Trip
#'
#' @param track_df Data frame of cleaned track points with columns `trip`,
#'   `timestamp`, `latitude`, `longitude`.
#' @param trip_id Character. Trip ID.
#' @param file_prefix Character. Cloud storage object prefix for predicted tracks.
#' @param model_version Character. Model version string from `ssfaitk`.
#' @param provider Character. Cloud storage provider key (e.g. `"gcs"`).
#' @param pds_opts Named list. Cloud storage options for the PDS bucket.
#' @param shore_distance Numeric. Shore distance in km to use for prediction.
#' @return A named list with `trip`, `status`, and optionally `n_points` or
#'   `message`.
#' @keywords internal
predict_and_upload_track <- function(
  track_df,
  trip_id,
  file_prefix,
  model_version,
  provider,
  pds_opts,
  shore_distance = 0.25
) {
  tmp_file <- file.path(
    tempdir(),
    glue::glue("trip_{trip_id}_v{model_version}.parquet")
  )
  on.exit(if (file.exists(tmp_file)) file.remove(tmp_file), add = TRUE)

  logger::log_info(
    "Using shore distance of {shore_distance} km for trip {trip_id}"
  )
  tryCatch(
    {
      predictions <- ssfaitk::effort_predict_statistical(
        df = track_df,
        filter = TRUE,
        config = list(shore_min_distance_km = shore_distance)
      ) |>
        dplyr::filter(
          .data$is_on_land == FALSE &
            .data$is_fishing == 1L &
            .data$is_near_shore == 0
        ) |>
        dplyr::select(
          "trip",
          "timestamp",
          "latitude",
          "longitude"
        )

      if (nrow(predictions) == 0) {
        logger::log_info("Trip {trip_id}: no fishing activity detected")

        arrow::write_parquet(predictions, sink = tmp_file)
        googleCloudStorageR::gcs_upload(
          file = tmp_file,
          bucket = pds_opts$bucket,
          name = glue::glue(
            "{file_prefix}/trip_{trip_id}_v{model_version}.parquet"
          )
        )

        return(list(trip = trip_id, status = "no_fishing"))
      }
      arrow::write_parquet(predictions, sink = tmp_file)

      googleCloudStorageR::gcs_upload(
        file = tmp_file,
        bucket = pds_opts$bucket,
        name = glue::glue(
          "{file_prefix}/trip_{trip_id}_v{model_version}.parquet"
        )
      )

      logger::log_info(
        "Trip {trip_id}: uploaded {nrow(predictions)} fishing points"
      )
      list(trip = trip_id, status = "success", n_points = nrow(predictions))
    },
    error = function(e) {
      logger::log_error(
        "Trip {trip_id}: predict/upload failed - {conditionMessage(e)}"
      )
      list(trip = trip_id, status = "error", message = conditionMessage(e))
    }
  )
}


#' Predict Fishing Activity from PDS Tracks
#'
#' @description
#' Downloads PDS GPS tracks and applies a statistical model to predict fishing
#' activity, uploading results to cloud storage. Implements version-aware
#' incremental processing: trips already predicted with the current model
#' version are skipped, while files from outdated model versions are deleted
#' and reprocessed.
#'
#' @details
#' The pipeline runs in two stages:
#' 1. **Parallel fetch** (I/O-bound): Downloads raw track points for all new
#'    trips using multiple workers.
#' 2. **Sequential predict + upload** (Python-bound): Applies the `ssfaitk`
#'    statistical model and uploads fishing-only points as parquet files to the
#'    PDS storage bucket.
#'
#' Requires the `ssfaitk` package for fishing activity classification and a
#' working Python environment accessible via `reticulate`. Set the
#' `RETICULATE_PYTHON` environment variable to specify the Python interpreter
#' path when running in CI or container environments.
#'
#' @param log_threshold The logging threshold to use. Default is `logger::DEBUG`.
#' @param date_from Character. Start date for trip retrieval in "YYYY-MM-DD"
#'   format. Default is `"2023-01-01"`.
#' @param n_workers Integer or NULL. Number of parallel workers for the fetch
#'   stage. Defaults to `parallel::detectCores() - 1`.
#' @param batch_size Integer. Number of tracks to process per prediction batch.
#'   Default is 500.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return Invisibly returns a data frame summarising the outcome for each trip
#'   (columns: `trip`, `status`).
#'
#' @seealso [get_trips()], [get_trip_points()], [resolve_storage_opts()]
#'
#' @keywords workflow modeling
#' @export
predict_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  date_from = "2023-01-01",
  n_workers = NULL,
  batch_size = 500L,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  coasts_opts <- resolve_storage_opts(conf, "coasts")
  pds_opts <- resolve_storage_opts(conf, "pds")

  # --- Python environment ---
  # If RETICULATE_PYTHON is set (Docker ENV or CI), reticulate respects it
  # automatically when Python is first initialised. If it is not set (local
  # development), we detect the active Python and lock it in via use_python()
  # *before* ssfaitk triggers any Python initialisation.
  python_path <- Sys.getenv("RETICULATE_PYTHON", unset = NA_character_)
  if (is.na(python_path) || !nzchar(python_path)) {
    python_path <- tryCatch(
      reticulate::py_config()$python,
      error = function(e) {
        stop(
          "Cannot detect Python. Set the RETICULATE_PYTHON environment variable."
        )
      }
    )
    reticulate::use_python(python_path, required = TRUE)
  }
  logger::log_info("Using Python: {python_path}")

  # --- Model version ---
  model_version <- ssfaitk::ssfaitk_version()[[1]]
  logger::log_info("Model version: {model_version}")

  # --- Load device registry ---
  logger::log_info("Loading device registry...")
  devices <- conf$metadata$airtable$name |>
    cloud_object_name(
      provider = conf$storage$google$key,
      extension = "rds",
      options = coasts_opts
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = coasts_opts
    ) |>
    readr::read_rds() |>
    purrr::pluck("devices") |>
    dplyr::filter(.data$customer_name %in% conf$pds$customers)

  # --- Fetch trip IDs from PDS API ---
  logger::log_info("Fetching trips from PDS API (from {date_from})...")
  all_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = date_from,
    dateTo = Sys.Date(),
    deviceInfo = TRUE,
    imeis = unique(devices$imei)
  ) |>
    dplyr::pull("Trip") |>
    unique() |>
    as.character()

  logger::log_info("Total trips to consider: {length(all_trips)}")

  # --- Version check: skip up-to-date trips, delete outdated files ---
  file_prefix <- conf$pds$pds_tracks_predicted$file_prefix

  existing_files <- tryCatch(
    googleCloudStorageR::gcs_list_objects(
      bucket = pds_opts$bucket,
      prefix = file_prefix
    ),
    error = function(e) {
      logger::log_warn("Could not list bucket: {conditionMessage(e)}")
      NULL
    }
  )

  already_done <- character(0)

  if (
    !is.null(existing_files) &&
      "name" %in% names(existing_files) &&
      nrow(existing_files) > 0
  ) {
    existing_parsed <- existing_files |>
      dplyr::mutate(
        trip_id = stringr::str_extract(.data$name, "(?<=trip_)\\d+"),
        file_version = stringr::str_extract(
          .data$name,
          "(?<=_v)\\d+\\.\\d+\\.\\d+"
        )
      )

    already_done <- existing_parsed |>
      dplyr::filter(.data$file_version == model_version) |>
      dplyr::pull(.data$trip_id)

    outdated_files <- existing_parsed |>
      dplyr::filter(.data$file_version != model_version)

    if (nrow(outdated_files) > 0) {
      logger::log_info(
        "Deleting {nrow(outdated_files)} files from previous model versions"
      )
      purrr::walk(outdated_files$name, \(f) {
        tryCatch(
          {
            googleCloudStorageR::gcs_delete_object(f, bucket = pds_opts$bucket)
            logger::log_debug("Deleted: {f}")
          },
          error = function(e) {
            logger::log_warn("Failed to delete {f}: {conditionMessage(e)}")
          }
        )
      })
    }
  }

  trips_to_process <- setdiff(all_trips, already_done)

  logger::log_info(
    "Skipping {length(already_done)} trips at v{model_version}, {length(trips_to_process)} to process"
  )

  if (length(trips_to_process) == 0) {
    logger::log_info("Nothing to process, all trips up to date")
    return(invisible(NULL))
  }

  # --- Stage 1: Parallel fetch (I/O bound, no Python) ---
  workers <- n_workers %||% max(1L, parallel::detectCores() - 1L)
  logger::log_info(
    "Fetching {length(trips_to_process)} trips with {workers} workers"
  )

  future::plan(future::multisession, workers = workers)

  all_tracks <- furrr::future_map(
    trips_to_process,
    \(tid) fetch_track_for_prediction(tid, conf),
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  ) |>
    purrr::set_names(trips_to_process) |>
    purrr::compact()

  future::plan(future::sequential)

  fetched_ids <- names(all_tracks)
  failed_fetch_ids <- setdiff(trips_to_process, fetched_ids)

  logger::log_info(
    "Fetched {length(all_tracks)}/{length(trips_to_process)} tracks"
  )

  if (length(failed_fetch_ids) > 0) {
    logger::log_warn(
      "{length(failed_fetch_ids)} trips failed to fetch or were empty"
    )
  }

  # --- Stage 2: Sequential predict + upload in batches (Python bound) ---
  track_batches <- split(
    fetched_ids,
    ceiling(seq_along(fetched_ids) / batch_size)
  )

  logger::log_info(
    "Predicting and uploading in {length(track_batches)} batch(es)"
  )

  all_results <- list()

  for (i in seq_along(track_batches)) {
    batch_ids <- track_batches[[i]]
    logger::log_info(
      "Batch {i}/{length(track_batches)}: {length(batch_ids)} tracks"
    )

    batch_results <- purrr::map(batch_ids, \(trip_id) {
      predict_and_upload_track(
        track_df = all_tracks[[trip_id]],
        trip_id = trip_id,
        file_prefix = file_prefix,
        model_version = model_version,
        provider = conf$pds_storage$google$key,
        pds_opts = pds_opts
      )
    })

    all_results <- c(all_results, batch_results)
    all_tracks[batch_ids] <- NULL
    gc()
  }

  # --- Add fetch failures to results ---
  fetch_fail_results <- purrr::map(
    failed_fetch_ids,
    \(tid) list(trip = tid, status = "fetch_failed_or_empty")
  )
  all_results <- c(all_results, fetch_fail_results)

  # --- Summary ---
  results_df <- dplyr::bind_rows(
    purrr::map(all_results, \(x) {
      data.frame(trip = as.character(x$trip), status = x$status)
    })
  )

  logger::log_info("Pipeline complete. Summary:")
  results_df |>
    dplyr::count(.data$status) |>
    dplyr::mutate(msg = glue::glue("  {status}: {n}")) |>
    dplyr::pull(.data$msg) |>
    purrr::walk(logger::log_info)

  invisible(results_df)
}


#' Aggregate Predicted Fishing Tracks into an H3 Effort Grid
#'
#' @description
#' Downloads all per-trip predicted fishing track files produced by
#' [predict_pds_tracks()] and aggregates them into a single H3 hexagonal grid
#' of cumulative fishing effort. The result is uploaded as a versioned parquet
#' file to the country-level cloud storage bucket.
#'
#' @details
#' Predicted track files contain fishing-only GPS points (columns: `trip`,
#' `timestamp`, `latitude`, `longitude`). This function:
#' 1. Lists all files under `conf$pds$pds_tracks_predicted$file_prefix` in the
#'    PDS bucket.
#' 2. Downloads them in parallel using `furrr`.
#' 3. Assigns each point to an H3 cell via [assign_h3_indices()].
#' 4. Summarises fishing pings and unique trips per cell across the entire
#'    dataset.
#' 5. Uploads the grid as a versioned parquet file.
#'
#' The grid is rebuilt in full on every run, ensuring that changes to predicted
#' files (e.g. after a model version update) are always reflected.
#'
#' @param log_threshold The logging threshold to use. Default is `logger::DEBUG`.
#' @param h3_res Integer (0–15). H3 resolution level for the output grid.
#'   Default is `9` (~174 m edge length).
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return Invisibly returns NULL (called for side effects).
#'
#' @seealso [predict_pds_tracks()], [assign_h3_indices()]
#'
#' @keywords workflow preprocessing
#' @export
aggregate_pds_effort <- function(
  log_threshold = logger::DEBUG,
  h3_res = 9L,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = "coasts")

  pds_opts <- resolve_storage_opts(conf, "pds")
  country_opts <- resolve_storage_opts(conf, "country")

  file_prefix <- conf$pds$pds_tracks_predicted$file_prefix
  grid_prefix <- conf$pds$pds_tracks_h3_grid$file_prefix
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

      logger::log_info("Loaded existing grid with {nrow(existing_grid)} cells")
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

  if (nrow(new_tracks) == 0) {
    logger::log_info("All new files were empty, nothing to aggregate")
    saveRDS(c(already_aggregated, new_files), manifest_local)
    upload_cloud_file(
      file = manifest_local,
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

  # --- Aggregate new data ---
  new_grid <- assign_h3_indices(
    df = new_tracks,
    lat_col = "latitude",
    lon_col = "longitude",
    h3_res = h3_res
  ) |>
    dplyr::group_by(.data$h3_index) |>
    dplyr::summarise(
      fishing_pings = dplyr::n(),
      unique_trips = dplyr::n_distinct(.data$trip),
      .groups = "drop"
    )

  # --- Merge with existing grid ---
  if (!is.null(existing_grid) && nrow(existing_grid) > 0) {
    logger::log_info("Merging with existing grid ({nrow(existing_grid)} cells)")

    h3_grid <- dplyr::bind_rows(existing_grid, new_grid) |>
      dplyr::group_by(.data$h3_index) |>
      dplyr::summarise(
        fishing_pings = sum(.data$fishing_pings),
        unique_trips = sum(.data$unique_trips),
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

  logger::log_info(
    "Uploading H3 grid ({nrow(h3_grid)} cells) to cloud storage..."
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
    provider = conf$storage$google$key,
    options = country_opts
  )
  unlink(manifest_local)

  logger::log_success(
    "H3 grid updated: {nrow(h3_grid)} cells ({length(new_files)} new files aggregated)"
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

#' Create an interactive Leaflet map of fishing grounds
#'
#' @param fishing_grounds An sf POLYGON object with ground_id and area_km2 columns
#' @param title Map title
#' @return A leaflet htmlwidget
plot_fishing_grounds <- function(fishing_grounds, title = "Fishing Grounds") {
  vals <- fishing_grounds$area_km2

  pal <- leaflet::colorNumeric(
    palette = c(
      "#08306b",
      "#2171b5",
      "#4eb3d3",
      "#ffff99",
      "#fe9929",
      "#d73027"
    ),
    domain = vals,
    na.color = "transparent"
  )

  popup_html <- glue::glue(
    "<div style='font-family: system-ui, sans-serif; font-size: 13px; line-height: 1.5;'>
      <strong style='font-size: 14px;'>{fishing_grounds$ground_id}</strong><br>
      <span style='color: #666;'>Area:</span> {round(vals, 2)} km&sup2;
    </div>"
  )

  leaflet::leaflet(
    fishing_grounds,
    options = leaflet::leafletOptions(zoomSnap = 0.25)
  ) |>
    leaflet::addProviderTiles(
      leaflet::providers$CartoDB.DarkMatter,
      group = "Dark"
    ) |>
    leaflet::addProviderTiles(
      leaflet::providers$CartoDB.Positron,
      group = "Light"
    ) |>
    leaflet::addProviderTiles(
      leaflet::providers$Esri.WorldImagery,
      group = "Satellite"
    ) |>
    leaflet::addPolygons(
      fillColor = ~ pal(vals),
      fillOpacity = 0.7,
      color = "#ffffff",
      weight = 0.5,
      opacity = 0.5,
      popup = popup_html,
      highlightOptions = leaflet::highlightOptions(
        weight = 2,
        color = "#ffffff",
        fillOpacity = 0.9,
        bringToFront = TRUE
      ),
      group = "Fishing Grounds"
    ) |>
    leaflet::addLegend(
      position = "bottomright",
      pal = pal,
      values = vals,
      title = "Area (km²)",
      opacity = 0.85
    ) |>
    leaflet::addLayersControl(
      baseGroups = c("Dark", "Light", "Satellite"),
      overlayGroups = "Fishing Grounds",
      position = "topright",
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) |>
    leaflet::addControl(
      html = glue::glue(
        "<div style='
          background: rgba(0,0,0,0.7);
          color: white;
          padding: 8px 14px;
          border-radius: 6px;
          font-family: system-ui, sans-serif;
          font-size: 15px;
          font-weight: 600;
        '>{title} &middot; {nrow(fishing_grounds)} areas</div>"
      ),
      position = "topleft"
    ) |>
    leaflet::addScaleBar(position = "bottomleft")
}
