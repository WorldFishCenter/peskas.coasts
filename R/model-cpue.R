#' Load All Matched Trips
#'
#' Downloads the `trips-matched` parquet (produced by [merge_survey_trips()])
#' for all countries, retaining only rows with a valid PDS trip ID and
#' non-zero catch weight. The `country` column is preserved so that downstream
#' consumers can filter if needed.
#'
#' @param conf Configuration list, as returned by [read_config()].
#'
#' @return A tibble with columns including `country`, `pds_trip`,
#'   `landing_date`, `catch_taxon`, `catch_kg`.
#'
#' @keywords internal
load_matched_trips <- function(conf) {
  logger::log_info("Downloading trips-matched (all countries) ...")

  matched <- download_parquet_from_cloud(
    prefix = conf$trips$matched,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    dplyr::filter(
      !is.na(.data$pds_trip),
      !is.na(.data$catch_taxon),
      .data$catch_taxon != "",
      !is.na(.data$catch_kg),
      .data$catch_kg > 0
    ) |>
    dplyr::mutate(
      pds_trip = as.character(.data$pds_trip),
      landing_date = as.Date(.data$landing_date),
      catch_taxon = stringr::str_to_title(stringr::str_trim(.data$catch_taxon)),
      catch_kg = as.numeric(.data$catch_kg),
      gear = stringr::str_to_title(stringr::str_trim(.data$gear))
    )

  logger::log_info(
    "Matched trips: {nrow(matched)} catch records |",
    " {dplyr::n_distinct(matched$pds_trip)} unique PDS trips |",
    " {dplyr::n_distinct(matched$country)} countries |",
    " {format(min(matched$landing_date), '%Y-%m-%d')}",
    " -> {format(max(matched$landing_date), '%Y-%m-%d')}"
  )
  matched
}


#' Download Predicted Track Files for Specific Trip IDs
#'
#' Fetches the per-trip predicted fishing track parquet files (produced by
#' [predict_pds_tracks()]) from the PDS storage bucket, filtered to a given
#' set of trip IDs.
#'
#' @param trip_ids Character vector of PDS trip IDs to retrieve.
#' @param conf Configuration list, as returned by [read_config()].
#'
#' @return A tibble of fishing GPS points with columns `trip`, `timestamp`,
#'   `latitude`, `longitude`.
#'
#' @keywords internal
download_predicted_tracks <- function(trip_ids, conf) {
  pds_opts <- resolve_storage_opts(conf, "pds")
  file_prefix <- conf$pds$pds_tracks_predicted$file_prefix

  cloud_storage_authenticate(conf$pds_storage$google$key, pds_opts)

  file_list <- googleCloudStorageR::gcs_list_objects(
    bucket = pds_opts$bucket,
    prefix = file_prefix
  )

  if (is.null(file_list) || nrow(file_list) == 0) {
    stop(
      "No predicted track files found in bucket '",
      pds_opts$bucket,
      "' with prefix '",
      file_prefix,
      "'.\n",
      "Run predict_pds_tracks() first."
    )
  }

  predicted_ids <- as.numeric(
    stringr::str_extract(file_list$name, "trip_(\\d+)_", group = 1)
  )
  file_list <- file_list[predicted_ids %in% as.numeric(trip_ids), ]

  logger::log_info(
    "Downloading {nrow(file_list)} matched track files",
    " (out of {length(predicted_ids)} total predicted)"
  )

  if (nrow(file_list) == 0) {
    stop("No predicted track files found for the requested trip IDs.")
  }

  tracks <- purrr::map(
    file_list$name,
    function(obj_name) {
      tmp <- tempfile(fileext = ".parquet")
      on.exit(if (file.exists(tmp)) file.remove(tmp), add = TRUE)
      tryCatch(
        {
          googleCloudStorageR::gcs_get_object(
            object_name = obj_name,
            bucket = pds_opts$bucket,
            saveToDisk = tmp,
            overwrite = TRUE
          )
          arrow::read_parquet(tmp)
        },
        error = function(e) {
          logger::log_warn("Skipping {obj_name}: {conditionMessage(e)}")
          NULL
        }
      )
    },
    .progress = TRUE
  ) |>
    purrr::compact() |>
    dplyr::bind_rows()

  logger::log_info(
    "Downloaded {format(nrow(tracks), big.mark = ',')} fishing points",
    " across {dplyr::n_distinct(tracks$trip)} trips"
  )
  tracks
}


#' Aggregate Predicted Tracks to Per-Trip H3 Effort
#'
#' Prepares tracks with time intervals and H3 cell assignment via
#' [prepare_tracks_for_effort()], then summarises fishing hours per
#' `(trip, h3_index, year)` combination. Used to build the effort matrix
#' for CPUE estimation.
#'
#' @param tracks A data frame of predicted fishing points with columns
#'   `trip`, `timestamp`, `latitude`, `longitude`.
#' @param h3_res Integer (0-15). H3 resolution for cell assignment.
#'
#' @return A tibble with columns `trip`, `h3_index`, `year`, `fishing_hours`.
#'
#' @keywords internal
aggregate_trip_effort <- function(tracks, h3_res) {
  effort <- prepare_tracks_for_effort(tracks, h3_res) |>
    dplyr::group_by(.data$trip, .data$h3_index, .data$year) |>
    dplyr::summarise(
      fishing_hours = sum(.data$dt_hours, na.rm = TRUE),
      .groups = "drop"
    )

  logger::log_info(
    "Trip effort: {nrow(effort)} rows |",
    " {dplyr::n_distinct(effort$trip)} trips |",
    " {dplyr::n_distinct(effort$h3_index)} H3 cells"
  )
  effort
}


#' Build a Wide Catch Matrix from Matched Trips
#'
#' Pivots matched trip catch records to a wide format with one row per
#' trip and one column per species, ready for joining with effort data.
#' The `country` column is carried through as an identifier.
#'
#' @param matched A tibble as returned by [load_matched_trips()].
#'
#' @return A wide tibble with columns `pds_trip`, `country`, and one numeric
#'   column per species (catch in kg, zero-filled for missing combinations).
#'
#' @keywords internal
build_catch_wide <- function(matched) {
  logger::log_info("Building catch matrix from trips-matched ...")

  catch_wide <- matched |>
    dplyr::group_by(.data$pds_trip, .data$country, .data$gear, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      id_cols = c("pds_trip", "country", "gear"),
      names_from = "catch_taxon",
      values_from = "catch_kg",
      values_fill = 0
    )

  logger::log_info(
    "Catch matrix: {nrow(catch_wide)} trips x",
    " {ncol(catch_wide) - 2} species"
  )
  catch_wide
}


#' Join Per-Trip Effort with Catch Matrix
#'
#' Inner-joins the per-trip H3 effort table with the wide catch matrix on
#' trip ID, producing a combined table suitable for CPUE estimation.
#'
#' @param effort A tibble as returned by [aggregate_trip_effort()].
#' @param catch_wide A wide catch matrix as returned by [build_catch_wide()].
#'
#' @return A tibble combining effort columns (`trip`, `h3_index`, `year`,
#'   `fishing_hours`) with one column per species (catch in kg).
#'
#' @keywords internal
join_effort_catch <- function(effort, catch_wide) {
  logger::log_info("Joining effort and catch on trip ID ...")

  trips <- effort |>
    dplyr::inner_join(catch_wide, by = c("trip" = "pds_trip"))

  logger::log_info(
    "Matched: {nrow(trips)} rows |",
    " {dplyr::n_distinct(trips$trip)} trips |",
    " {dplyr::n_distinct(trips$h3_index)} H3 cells"
  )

  if (nrow(trips) == 0) {
    logger::log_warn(
      "No matches -- check that predicted tracks cover trips-matched trips."
    )
  }

  trips
}


#' Identify Top-N Species by Total Catch
#'
#' @param trips Combined effort + catch tibble.
#' @param meta_cols Character vector of non-species column names to exclude.
#' @param top_n Integer. Number of top species to return.
#'
#' @return Character vector of species names ordered by descending total catch.
#'
#' @keywords internal
.top_species <- function(trips, meta_cols, top_n) {
  species_cols <- setdiff(colnames(trips), meta_cols)

  species_totals <- trips |>
    dplyr::summarise(dplyr::across(
      dplyr::all_of(species_cols),
      \(x) sum(x, na.rm = TRUE)
    )) |>
    tidyr::pivot_longer(
      dplyr::everything(),
      names_to = "species",
      values_to = "total_kg"
    ) |>
    dplyr::filter(.data$total_kg > 0) |>
    dplyr::arrange(dplyr::desc(.data$total_kg))

  logger::log_info("Catch totals from matched trips:")
  print(as.data.frame(species_totals))

  top_sp <- species_totals |>
    dplyr::slice_head(n = top_n) |>
    dplyr::pull("species")

  logger::log_info("Modelling: {paste(top_sp, collapse = ', ')}")
  top_sp
}


#' Attach Trip Counts, Cell Centroids, and Apply Minimum-Trips Filter
#'
#' @param cpue_long Long tibble with columns `h3_index`, `country`, `cpue`,
#'   `species`.
#' @param trips Combined effort + catch tibble (used to count trips per cell).
#' @param min_trips Integer. Minimum unique trips per H3 cell x country.
#'
#' @return Filtered tibble with additional columns `n_trips`, `lon`, `lat`.
#'
#' @keywords internal
.finalise_cpue <- function(cpue_long, trips, min_trips) {
  trip_counts <- trips |>
    dplyr::group_by(.data$h3_index, .data$country) |>
    dplyr::summarise(n_trips = dplyr::n_distinct(.data$trip), .groups = "drop")

  centroids <- h3jsr::cell_to_point(unique(cpue_long$h3_index)) |>
    sf::st_coordinates() |>
    tibble::as_tibble() |>
    dplyr::rename(lon = "X", lat = "Y") |>
    dplyr::mutate(h3_index = unique(cpue_long$h3_index))

  cpue_df <- cpue_long |>
    dplyr::left_join(trip_counts, by = c("h3_index", "country")) |>
    dplyr::left_join(centroids, by = "h3_index") |>
    dplyr::filter(.data$n_trips >= min_trips, .data$cpue > 0)

  logger::log_info("CPUE table: {nrow(cpue_df)} rows (min_trips = {min_trips})")
  cpue_df
}


#' Run the Weighted CPUE Model
#'
#' Computes CPUE as the ratio of total catch to total fishing hours per H3
#' cell for each of the top-N species:
#' `CPUE_h = sum(catch_kg for trips visiting h) / sum(fishing_hours in h)`.
#' Robust with sparse data; always produces a result.
#'
#' @param trips Combined effort + catch tibble from [join_effort_catch()].
#' @param top_n Integer. Number of top species to model.
#' @param min_trips Integer. Minimum unique trips per H3 cell to retain.
#'
#' @return A named list with elements `cpue` (tibble) and `species`
#'   (character vector).
#'
#' @keywords internal
run_weighted_cpue <- function(trips, top_n, min_trips) {
  logger::log_info("Running weighted CPUE model (catch / effort per cell) ...")

  meta_cols <- c("trip", "h3_index", "country", "gear", "year", "fishing_hours")
  top_species <- .top_species(trips, meta_cols, top_n)

  catch_per_trip <- trips |>
    dplyr::group_by(.data$trip) |>
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(top_species),
        \(x) dplyr::first(x[!is.na(x)])
      ),
      .groups = "drop"
    )

  cpue_long <- purrr::map(top_species, function(sp) {
    trips |>
      dplyr::select("trip", "h3_index", "country", "fishing_hours") |>
      dplyr::left_join(
        catch_per_trip |> dplyr::select("trip", dplyr::all_of(sp)),
        by = "trip"
      ) |>
      dplyr::group_by(.data$h3_index, .data$country) |>
      dplyr::summarise(
        total_catch = sum(.data[[sp]], na.rm = TRUE),
        total_hours = sum(.data$fishing_hours, na.rm = TRUE),
        .groups = "drop"
      ) |>
      dplyr::mutate(
        cpue = dplyr::if_else(
          .data$total_hours > 0,
          .data$total_catch / .data$total_hours,
          0
        ),
        species = sp
      ) |>
      dplyr::select("h3_index", "country", "cpue", "species")
  }) |>
    dplyr::bind_rows()

  logger::log_info(
    "Weighted CPUE: {sum(cpue_long$cpue > 0)} non-zero cell x species combinations"
  )

  cpue_df <- .finalise_cpue(cpue_long, trips, min_trips)
  list(cpue = cpue_df, species = top_species)
}


#' Run the NNLS CPUE Model
#'
#' Solves `min ||Xq - y||^2 s.t. q >= 0` for each species, spatially
#' deconvolving catch into per-cell CPUE estimates. More statistically
#' rigorous than the weighted method but requires an overdetermined system
#' (rule of thumb: at least 3x more trips than H3 cells).
#'
#' @param trips Combined effort + catch tibble from [join_effort_catch()].
#' @param top_n Integer. Number of top species to model.
#' @param min_trips Integer. Minimum unique trips per H3 cell to retain.
#'
#' @return A named list with elements `cpue` (tibble) and `species`
#'   (character vector).
#'
#' @keywords internal
run_nnls_cpue <- function(trips, top_n, min_trips) {
  logger::log_info("Running NNLS CPUE model ...")

  meta_cols <- c("trip", "h3_index", "country", "year", "fishing_hours")
  top_species <- .top_species(trips, meta_cols, top_n)

  X_wide <- trips |>
    dplyr::distinct(.data$trip, .data$h3_index, .data$fishing_hours) |>
    tidyr::pivot_wider(
      id_cols = "trip",
      names_from = "h3_index",
      values_from = "fishing_hours",
      values_fn = sum,
      values_fill = 0
    )

  trip_ids <- X_wide$trip
  h3_cells <- setdiff(colnames(X_wide), "trip")
  X <- as.matrix(X_wide[, h3_cells])

  catch_per_trip <- trips |>
    dplyr::group_by(.data$trip) |>
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(top_species),
        \(x) dplyr::first(x[!is.na(x)])
      ),
      .groups = "drop"
    ) |>
    dplyr::arrange(match(.data$trip, trip_ids))

  Y <- as.matrix(dplyr::select(catch_per_trip, dplyr::all_of(top_species)))

  logger::log_info("Design matrix: {nrow(X)} trips x {ncol(X)} H3 cells")

  if (ncol(X) >= nrow(X)) {
    logger::log_warn(
      "Under-determined: {ncol(X)} cells >= {nrow(X)} trips.",
      " NNLS will produce sparse results. Consider method = 'weighted'."
    )
  }

  cell_country <- trips |>
    dplyr::distinct(.data$h3_index, .data$country)

  cpue_long <- purrr::map(top_species, function(sp) {
    y <- Y[, sp]
    fit <- nnls::nnls(X, y)
    logger::log_info(
      "  {sp}: residual = {round(sqrt(sum(fit$residuals^2)), 2)},",
      " non-zero cells = {sum(fit$x > 0)}"
    )
    tibble::tibble(h3_index = h3_cells, cpue = fit$x, species = sp)
  }) |>
    dplyr::bind_rows() |>
    dplyr::left_join(cell_country, by = "h3_index")

  cpue_df <- .finalise_cpue(cpue_long, trips, min_trips)
  list(cpue = cpue_df, species = top_species)
}


#' Run the Full Spatial CPUE Pipeline
#'
#' @description
#' Estimates spatial Catch Per Unit Effort (CPUE) by combining:
#' - **Matched trips** (`trips-matched` parquet, from [merge_survey_trips()]):
#'   validated catch records already linked to PDS trip IDs (all countries).
#' - **Predicted track files** (from [predict_pds_tracks()]): downloaded only
#'   for matched trips, to build the per-trip H3 effort matrix for the CPUE
#'   model.
#'
#' @details
#' Two CPUE estimation methods are available:
#'
#' - **`"weighted"`** (default, recommended for sparse data): direct
#'   catch-to-effort ratio per H3 cell and country. Robust when trips are few.
#'   `CPUE_h = sum(catch_kg) / sum(fishing_hours)` across all trips
#'   visiting cell `h` within each country.
#'
#' - **`"nnls"`** (for denser data, >= ~200 trips): solves
#'   `min ||Xq - y||^2 s.t. q >= 0` across all cells simultaneously.
#'   More statistically rigorous but requires an overdetermined system
#'   (rule of thumb: trips >> H3 cells).
#'
#' The CPUE result table includes a `country` column so users can filter by
#' country downstream. It is uploaded as a versioned Parquet file to the
#' cloud bucket under the `pds_cpue` file prefix.
#'
#' @param h3_res Integer (0-15). H3 resolution for spatial aggregation.
#'   Default is `9L` (~174 m edge). Use `5`-`6` for very sparse data.
#' @param top_n Integer. Number of top species (by total catch) to model.
#'   Default is `5L`.
#' @param min_trips Integer. Minimum unique trips per H3 cell x country
#'   required to retain a cell in the CPUE output. Default is `3L`.
#' @param method Character. CPUE estimation method: `"weighted"` (default)
#'   or `"nnls"`. See Details.
#' @param log_threshold The logging threshold to use. Default is
#'   `logger::DEBUG`.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`.
#'
#' @return Invisibly returns a named list:
#'   \describe{
#'     \item{`effort_matched`}{Per-trip H3 effort for matched trips.}
#'     \item{`cpue`}{CPUE table (tibble with `h3_index`, `country`, `cpue`,
#'       `species`, `n_trips`, `lon`, `lat`).}
#'     \item{`trips`}{Combined effort + catch matrix.}
#'   }
#'
#' @seealso [aggregate_pds_effort()], [predict_pds_tracks()],
#'   [merge_survey_trips()]
#'
#' @keywords workflow modeling
#' @export
model_cpue <- function(
  h3_res = 9L,
  top_n = 5L,
  min_trips = 3L,
  method = c("weighted", "nnls"),
  log_threshold = logger::DEBUG,
  package = "coasts"
) {
  method <- match.arg(method)
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  logger::log_info(
    "=== CPUE pipeline | H3 res = {h3_res} | method = {method} ==="
  )

  # -- Matched trips (catch + pds_trip ID, all countries) -----------------------
  matched <- load_matched_trips(conf)

  # -- Matched tracks -> per-trip effort for CPUE model --------------------------
  logger::log_info("Downloading matched predicted tracks for CPUE model ...")
  tracks_m <- download_predicted_tracks(unique(matched$pds_trip), conf)
  effort_m <- aggregate_trip_effort(tracks_m, h3_res)
  rm(tracks_m)

  # -- Catch matrix + join -------------------------------------------------------
  catch_wide <- build_catch_wide(matched)
  rm(matched)

  trips <- join_effort_catch(effort_m, catch_wide)

  if (nrow(trips) == 0) {
    logger::log_warn("Pipeline stopped: no matched trips.")
    return(invisible(NULL))
  }

  # -- CPUE estimation -----------------------------------------------------------
  result <- if (method == "weighted") {
    run_weighted_cpue(trips, top_n, min_trips)
  } else {
    run_nnls_cpue(trips, top_n, min_trips)
  }

  cpue_df <- result$cpue
  species <- result$species

  # -- Upload CPUE table to cloud ------------------------------------------------
  logger::log_info(
    "Uploading CPUE table ({nrow(cpue_df)} rows) to cloud storage ..."
  )
  upload_parquet_to_cloud(
    data = cpue_df,
    prefix = paste0(conf$pds$pds_cpue$file_prefix, "_r", h3_res),
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("=== CPUE pipeline complete ===")

  invisible(list(effort_matched = effort_m, cpue = cpue_df, trips = trips))
}
