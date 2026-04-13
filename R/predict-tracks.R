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
#' @param max_trip_days Numeric or NULL. Trips whose timestamp range exceeds
#'   this number of days are skipped before prediction (they are recorded as
#'   `"skipped_too_long"` in the summary). This guards against corrupted or
#'   never-reset device tracks that would cause extreme memory and CPU usage.
#'   Default is `5`. Set to `NULL` to disable the filter.
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
  max_trip_days = 5,
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

  raw_fetched_ids <- names(all_tracks)
  failed_fetch_ids <- setdiff(trips_to_process, raw_fetched_ids)

  logger::log_info(
    "Fetched {length(all_tracks)}/{length(trips_to_process)} tracks"
  )

  if (length(failed_fetch_ids) > 0) {
    logger::log_warn(
      "{length(failed_fetch_ids)} trips failed to fetch or were empty"
    )
  }

  # --- Filter out excessively long trips before prediction ---
  skipped_ids <- character(0)
  if (!is.null(max_trip_days)) {
    trip_spans <- purrr::map_dbl(all_tracks, \(df) {
      as.numeric(difftime(max(df$timestamp), min(df$timestamp), units = "days"))
    })
    skipped_ids <- names(trip_spans)[trip_spans > max_trip_days]
    if (length(skipped_ids) > 0) {
      logger::log_warn(
        "Skipping {length(skipped_ids)} trip(s) spanning > {max_trip_days} days",
        " (IDs: {paste(skipped_ids, collapse = ', ')})"
      )
      all_tracks[skipped_ids] <- NULL
    }
  }

  fetched_ids <- names(all_tracks)

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

  # --- Add fetch failures and skipped trips to results ---
  fetch_fail_results <- purrr::map(
    failed_fetch_ids,
    \(tid) list(trip = tid, status = "fetch_failed_or_empty")
  )
  skipped_results <- purrr::map(
    skipped_ids,
    \(tid) list(trip = tid, status = "skipped_too_long")
  )
  all_results <- c(all_results, fetch_fail_results, skipped_results)

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
