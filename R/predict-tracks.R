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


#' Read the Trip Identifier Out of a Predicted Track Object Name
#'
#' @description
#' The counterpart to the `trip_{id}_v{version}.parquet` names
#' [predict_and_upload_track()] writes, kept beside them so that the convention
#' is stated once rather than re-derived by every reader.
#'
#' @param names Character vector of cloud object names.
#'
#' @return Character vector of trip identifiers, `NA` where a name does not
#'   follow the convention.
#'
#' @seealso [aggregate_pds_effort()], [download_predicted_tracks()]
#'
#' @keywords internal
predicted_track_trip_id <- function(names) {
  stringr::str_extract(names, "(?<=trip_)\\d+")
}


#' Delete Predicted Track Files from Cloud Storage
#'
#' @description
#' Removes predicted-track objects one by one, logging rather than raising when
#' an individual delete fails so that a single bad object cannot abort the run.
#'
#' @param names Character vector of cloud object names to delete.
#' @param bucket Character. Name of the bucket holding the objects.
#'
#' @return Invisibly `NULL`.
#'
#' @keywords internal
delete_predicted_files <- function(names, bucket) {
  purrr::walk(names, \(f) {
    tryCatch(
      {
        googleCloudStorageR::gcs_delete_object(f, bucket = bucket)
        logger::log_debug("Deleted: {f}")
      },
      error = function(e) {
        logger::log_warn("Failed to delete {f}: {conditionMessage(e)}")
      }
    )
  })
  invisible(NULL)
}


#' Delete Predicted Track Files Within a Run's Deletion Budget
#'
#' @description
#' Deletes objects only while the run's total deletions stay within
#' `max_delete_frac` of the store, and reports how much of the budget is left.
#'
#' @details
#' [predict_pds_tracks()] deletes on two occasions — trips the API no longer
#' lists, and refreshes that produced nothing — and the cap is meant to bound
#' what one run can remove *in total*. Checking each occasion separately against
#' the same store size would let a single run delete nearly twice the documented
#' share, so the budget is carried between them: what has already gone counts
#' against what may still go.
#'
#' @param names Character vector of cloud object names to delete.
#' @param bucket Character. Name of the bucket holding the objects.
#' @param spent Integer. Objects already deleted in this run.
#' @param total Integer. Size of the store the budget is a fraction of.
#' @param max_delete_frac Numeric. Share of `total` this run may delete.
#' @param reason Character. What is being deleted, for the log line.
#'
#' @return The number of objects deleted so far in this run, including these.
#'
#' @keywords internal
delete_within_budget <- function(
  names,
  bucket,
  spent,
  total,
  max_delete_frac,
  reason
) {
  if (length(names) == 0) {
    return(spent)
  }

  budget <- max_delete_frac * max(total, 1L)

  if (spent + length(names) > budget) {
    logger::log_warn(
      "Keeping {length(names)} file(s) that would be deleted as {reason}:",
      " that would put this run at {spent + length(names)} of {total} files,",
      " over the {round(100 * max_delete_frac, 1)}% a single run may remove.",
      " This looks like an incomplete trip listing or an API outage rather",
      " than individual bad trips"
    )
    return(spent)
  }

  logger::log_info("Deleting {length(names)} file(s): {reason}")
  delete_predicted_files(names, bucket)
  spent + length(names)
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
#' ## Keeping the store in step with PDS
#'
#' PDS revises trips after we have read them: it retires identifiers whose
#' points move to a new trip, and reassigns others so the same identifier comes
#' to describe a different window. Skipping every trip already predicted at the
#' current model version would freeze those first readings in place, leaving the
#' store holding several identifiers for one physical track — which
#' [aggregate_pds_effort()] then counts as several vessels. Two passes keep the
#' store aligned with the API:
#'
#' - trips whose PDS `Updated` timestamp is newer than the file we wrote are
#'   re-fetched and re-predicted, replacing the stale snapshot. Where the fetch
#'   comes back empty or fails, the stale file is deleted instead: PDS has said
#'   the trip changed, so keeping a snapshot from before it did would preserve
#'   the very duplicate this pass exists to remove, and queue the trip again on
#'   every future run. The trip is still listed, so a later run reads it as new
#'   and the file returns if the failure was transient;
#' - files for trips the API no longer lists are deleted, provided their
#'   identifier falls inside the range the listing covered — identifiers rise
#'   over time, so a file from before `date_from` is missing from the listing
#'   for a reason that has nothing to do with retirement.
#'
#' Both deletions are skipped, with a warning, when they would exceed
#' `max_delete_frac` of the store, so a truncated listing or an API outage
#' cannot empty it. Both passes leave [aggregate_pds_effort()] with changed
#' inputs, which makes it withdraw the affected trips from its effort store and
#' read them again.
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
#' @param refresh_updated Logical. Re-predict trips PDS modified after we
#'   fetched them. Default is `TRUE`. Set to `FALSE` to restore the previous
#'   fetch-once behaviour.
#' @param max_refresh Integer or NULL. Cap on how many stale trips are refreshed
#'   per run, most recently modified first. `NULL` (default) refreshes all of
#'   them; set it when working through a backlog so a scheduled run stays within
#'   its time budget.
#' @param max_delete_frac Numeric between 0 and 1. Largest share of the current
#'   predicted files that may be deleted in one run, applied separately to
#'   unlisted trips and to failed refreshes. Above it nothing is deleted, on the
#'   assumption the trip listing is incomplete or the API is unwell. Default is
#'   `0.1`; `0` disables deletion entirely. Note the listing is filtered to the
#'   IMEIs of `conf$pds$customers`, so a device leaving the Airtable registry
#'   makes its trips look retired -- the cap is what keeps a registry change
#'   from clearing a fleet's history in one run.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return Invisibly returns a data frame summarising the outcome for each trip
#'   (columns: `trip`, `status`).
#'
#' @seealso [get_trips()], [get_trip_points()], [resolve_storage_opts()],
#'   [aggregate_pds_effort()]
#'
#' @keywords workflow modeling
#' @export
predict_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  date_from = "2023-01-01",
  n_workers = NULL,
  batch_size = 500L,
  max_trip_days = 5,
  refresh_updated = TRUE,
  max_refresh = NULL,
  max_delete_frac = 0.1,
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
  # `updated` is kept so that trips revised after we predicted them can be
  # refreshed instead of frozen at their first reading.
  logger::log_info("Fetching trips from PDS API (from {date_from})...")
  pds_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = date_from,
    dateTo = Sys.Date(),
    deviceInfo = TRUE,
    imeis = unique(devices$imei)
  ) |>
    janitor::clean_names() |>
    dplyr::transmute(
      trip = as.character(.data$trip),
      pds_updated = lubridate::as_datetime(.data$updated)
    ) |>
    # A trip can be listed more than once; keep its latest revision, since
    # taking whichever row arrived first would hide a revision behind an
    # earlier timestamp and leave the trip permanently unrefreshed.
    dplyr::slice_max(
      .data$pds_updated,
      by = "trip",
      with_ties = FALSE
    )

  all_trips <- pds_trips$trip

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
  refresh_trips <- character(0)
  refresh_files <- tibble::tibble(trip_id = character(0), name = character(0))
  n_current_files <- 0L
  # Deletions are budgeted across the whole run, not per occasion — see
  # [delete_within_budget()].
  deleted_this_run <- 0L

  if (
    !is.null(existing_files) &&
      "name" %in% names(existing_files) &&
      nrow(existing_files) > 0
  ) {
    existing_parsed <- existing_files |>
      dplyr::mutate(
        trip_id = predicted_track_trip_id(.data$name),
        file_version = stringr::str_extract(
          .data$name,
          "(?<=_v)\\d+\\.\\d+\\.\\d+"
        ),
        fetched = lubridate::as_datetime(.data$updated)
      )

    current_files <- existing_parsed |>
      dplyr::filter(.data$file_version == model_version)

    already_done <- current_files$trip_id
    n_current_files <- nrow(current_files)

    outdated_files <- existing_parsed |>
      dplyr::filter(.data$file_version != model_version)

    if (nrow(outdated_files) > 0) {
      logger::log_info(
        "Deleting {nrow(outdated_files)} files from previous model versions"
      )
      delete_predicted_files(outdated_files$name, pds_opts$bucket)
    }

    # --- Trips the API no longer lists → their points now live under another
    # trip id, so the file is a duplicate that would be counted forever ---
    # Only files inside the window just queried can be judged: PDS identifiers
    # increase over time, so a file whose identifier falls outside the range the
    # listing returned simply predates `date_from` (or follows `dateTo`) and is
    # absent for that reason, not because the trip was retired.
    listed_range <- range(as.numeric(all_trips), na.rm = TRUE)
    retired_files <- current_files |>
      dplyr::filter(
        !(.data$trip_id %in% all_trips),
        as.numeric(.data$trip_id) >= listed_range[1],
        as.numeric(.data$trip_id) <= listed_range[2]
      )

    if (nrow(retired_files) > 0) {
      spent <- deleted_this_run
      deleted_this_run <- delete_within_budget(
        retired_files$name,
        bucket = pds_opts$bucket,
        spent = deleted_this_run,
        total = n_current_files,
        max_delete_frac = max_delete_frac,
        reason = "their trip is no longer listed by PDS"
      )
      if (deleted_this_run > spent) {
        already_done <- setdiff(already_done, retired_files$trip_id)
      }
    }

    # --- Trips PDS revised after we fetched them → re-predict, since our
    # snapshot may describe a window that has since moved to another trip ---
    if (isTRUE(refresh_updated)) {
      stale_files <- current_files |>
        dplyr::inner_join(pds_trips, by = c("trip_id" = "trip")) |>
        dplyr::filter(.data$pds_updated > .data$fetched) |>
        dplyr::arrange(dplyr::desc(.data$pds_updated))

      if (!is.null(max_refresh) && nrow(stale_files) > max_refresh) {
        logger::log_info(
          "{nrow(stale_files)} trips were revised by PDS since we fetched them,",
          " refreshing the {max_refresh} most recent (max_refresh)"
        )
        stale_files <- dplyr::slice_head(stale_files, n = max_refresh)
      }

      refresh_trips <- stale_files$trip_id
      refresh_files <- dplyr::select(stale_files, "trip_id", "name")

      if (length(refresh_trips) > 0) {
        logger::log_info(
          "Re-predicting {length(refresh_trips)} trips revised by PDS since they were fetched"
        )
        already_done <- setdiff(already_done, refresh_trips)
      }
    }
  }

  trips_to_process <- setdiff(all_trips, already_done)

  logger::log_info(
    "Skipping {length(already_done)} trips at v{model_version},",
    " {length(trips_to_process)} to process",
    " ({length(refresh_trips)} of them refreshed after a PDS revision)"
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

  # --- Refreshes that produced nothing ---
  # A trip queued for refresh whose fetch failed keeps its old file, and would
  # be queued again on every future run: PDS says the trip changed, we still
  # hold the snapshot from before it did. That snapshot is the stale window this
  # release removes, so it goes. The trip is still listed, so a later run picks
  # it up as new and the file comes back if the failure was transient.
  # Trips skipped for length are left alone: they can never be predicted, and
  # deleting their file would drop real effort for good.
  unrefreshed <- results_df |>
    dplyr::filter(
      .data$trip %in% refresh_trips,
      !(.data$status %in% c("success", "no_fishing", "skipped_too_long"))
    )
  stuck <- intersect(refresh_trips, results_df$trip[
    results_df$status == "skipped_too_long"
  ])

  if (length(stuck) > 0) {
    logger::log_warn(
      "{length(stuck)} trip(s) revised by PDS are too long to predict;",
      " their files stay as they are and will be queued again next run"
    )
  }

  if (nrow(unrefreshed) > 0) {
    deleted_this_run <- delete_within_budget(
      refresh_files$name[refresh_files$trip_id %in% unrefreshed$trip],
      bucket = pds_opts$bucket,
      spent = deleted_this_run,
      total = n_current_files,
      max_delete_frac = max_delete_frac,
      reason = "their refresh produced nothing, so they are not re-fetched forever"
    )
  }

  logger::log_info("Pipeline complete. Summary:")
  results_df |>
    dplyr::count(.data$status) |>
    dplyr::mutate(msg = glue::glue("  {status}: {n}")) |>
    dplyr::pull(.data$msg) |>
    purrr::walk(logger::log_info)

  invisible(results_df)
}
