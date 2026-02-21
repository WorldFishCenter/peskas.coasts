#' Ingest Pelagic Data Systems (PDS) Trip Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS).
#' It performs the following operations:
#' 1. Retrieves device metadata from configured cloud storage
#' 2. Filters devices by last seen date (>= 2023-01-01)
#' 3. Downloads all trip data from PDS API (2023-01-01 to present)
#' 4. Filters trips to match active device IMEIs
#' 5. Converts the data to parquet format
#' 6. Uploads the processed file to configured cloud storage
#'
#' @details
#' The function requires specific configuration in the `conf.yml` file with the following structure:
#'
#' ```yaml
#' pds:
#'   token: "your_pds_token"               # PDS API token
#'   secret: "your_pds_secret"             # PDS API secret
#'   pds_trips:
#'     file_prefix: "pds_trips"            # Prefix for output files
#' metadata:
#'   airtable:
#'     name: "metadata_file_prefix"        # Prefix for metadata file in cloud storage
#' storage:
#'   google:                               # Storage provider configuration
#'     key: "google"                       # Storage provider identifier
#'     options:
#'       project: "project-id"             # Cloud project ID
#'       bucket: "bucket-name"             # Storage bucket name
#'       service_account_key: "path/to/key.json"
#' ```
#'
#' The function processes trips as follows:
#' - Downloads device metadata RDS file from cloud storage
#' - Converts device `last_seen` timestamps from Unix milliseconds to Date format
#' - Filters to devices active since 2023-01-01
#' - Retrieves all trips from PDS API (with `deviceInfo` and `withLastSeen` options enabled)
#' - Filters trips to match active device IMEIs
#' - Saves as compressed parquet file (LZ4 compression, level 12)
#' - Uploads to configured cloud storage
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates a versioned parquet file locally with filtered trip data
#'   - Uploads file to configured cloud storage
#'   - Generates logs of the process
#'
#' @note
#' The PDS API does not support IMEI filtering in the request, so all trips are
#' retrieved and filtered locally. This ensures reliable data retrieval but may
#' result in downloading more data than needed.
#'
#' @examples
#' \dontrun{
#' # Run with default debug logging
#' ingest_pds_trips()
#'
#' # Run with info-level logging only
#' ingest_pds_trips(logger::INFO)
#' }
#'
#' @seealso
#' * [get_trips()] for details on the PDS trip data retrieval process
#' * [cloud_object_name()] for generating cloud storage object names
#' * [download_cloud_file()] for downloading files from cloud storage
#' * [upload_cloud_file()] for uploading files to cloud storage
#'
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_trips <- function(log_threshold = logger::DEBUG, package = "coasts") {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  assets <- conf$metadata$airtable$name |>
    cloud_object_name(
      provider = conf$storage$google$key,
      extension = "rds",
      options = conf$storage$google$options
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds()

  assets$devices <- assets$devices |>
    dplyr::mutate(
      last_seen = as.Date(as.POSIXct(
        .data$last_seen / 1000,
        origin = "1970-01-01"
      ))
    ) |>
    dplyr::filter(.data$last_seen >= "2023-01-01")

  boats_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = "2023-01-01",
    dateTo = Sys.Date(),
    #imeis = unique(assets$devices$imei),
    deviceInfo = TRUE,
    withLastSeen = TRUE
  ) |>
    dplyr::filter(.data$IMEI %in% as.numeric(unique(assets$devices$imei)))

  filename <- conf$pds$pds_trips$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = boats_trips,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {filename} to cloud storage")
  upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Ingest Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat track data from Pelagic Data Systems (PDS).
#' It downloads and stores only new tracks that haven't been previously uploaded to Google Cloud Storage.
#' Uses parallel processing for improved performance.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param batch_size Optional number of tracks to process. If NULL, processes all new tracks.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return None (invisible). The function performs its operations for side effects.
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  batch_size = NULL,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  # Get trips file from cloud storage
  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    extension = "parquet",
    version = conf$pds$pds_trips$version,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading {pds_trips_parquet}")
  download_cloud_file(
    name = pds_trips_parquet,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  # Read trip IDs
  logger::log_info("Reading trip IDs...")
  trips_data <- arrow::read_parquet(file = pds_trips_parquet) %>%
    dplyr::pull("Trip") %>%
    unique()

  # Clean up downloaded file
  unlink(pds_trips_parquet)

  # List existing files in GCS bucket
  logger::log_info("Checking existing tracks in cloud storage...")
  existing_tracks <-
    googleCloudStorageR::gcs_list_objects(
      bucket = conf$pds_storage$google$options$bucket,
      prefix = conf$pds$pds_tracks$file_prefix
    )$name

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(existing_tracks)
  new_trip_ids <- setdiff(trips_data, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  # Select tracks to process
  process_ids <- if (!is.null(batch_size)) {
    new_trip_ids[1:batch_size]
  } else {
    new_trip_ids
  }
  logger::log_info("Processing {length(process_ids)} new tracks in parallel...")

  # Process tracks in parallel with progress bar
  results <- furrr::future_map(
    process_ids,
    function(trip_id) {
      tryCatch(
        {
          # Create filename for this track
          track_filename <- sprintf(
            "%s_%s.parquet",
            conf$pds$pds_tracks$file_prefix,
            trip_id
          )

          # Get track data
          track_data <- get_trip_points(
            token = conf$pds$token,
            secret = conf$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )

          # Save to parquet
          arrow::write_parquet(
            x = track_data,
            sink = track_filename,
            compression = "lz4",
            compression_level = 12
          )

          # Upload to cloud
          logger::log_info("Uploading track for trip {trip_id}")
          upload_cloud_file(
            file = track_filename,
            provider = conf$pds_storage$google$key,
            options = conf$pds_storage$google$options
          )

          # Clean up local file
          unlink(track_filename)

          list(
            status = "success",
            trip_id = trip_id,
            message = "Successfully processed"
          )
        },
        error = function(e) {
          list(
            status = "error",
            trip_id = trip_id,
            message = e$message
          )
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  # Clean up parallel processing
  future::plan(future::sequential)

  # Summarize results
  successes <- sum(purrr::map_chr(results, "status") == "success")
  failures <- sum(purrr::map_chr(results, "status") == "error")

  logger::log_info(
    "Processing complete. Successfully processed {successes} tracks."
  )
  if (failures > 0) {
    logger::log_warn("Failed to process {failures} tracks.")
    failed_results <- results[purrr::map_chr(results, "status") == "error"]
    failed_trips <- purrr::map_chr(failed_results, "trip_id")
    failed_messages <- purrr::map_chr(failed_results, "message")

    logger::log_warn("Failed trip IDs and reasons:")
    purrr::walk2(
      failed_trips,
      failed_messages,
      ~ logger::log_warn("Trip {.x}: {.y}")
    )
  }
}

#' Extract Trip IDs from Track Filenames
#'
#' @param filenames Character vector of track filenames
#' @return Character vector of trip IDs
#' @keywords internal
extract_trip_ids_from_filenames <- function(filenames) {
  if (length(filenames) == 0) {
    return(character(0))
  }
  # Assuming filenames are in format: pds-tracks_TRIPID.parquet
  gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
}

#' Process Single PDS Track
#'
#' @param trip_id Character. The ID of the trip to process.
#' @param conf List. Configuration parameters.
#' @return List with processing status and details.
#' @keywords internal
process_single_track <- function(trip_id, conf) {
  tryCatch(
    {
      # Create filename for this track
      track_filename <- sprintf(
        "%s_%s.parquet",
        conf$pds$pds_tracks$file_prefix,
        trip_id
      )

      # Get track data
      track_data <- get_trip_points(
        token = conf$pds$token,
        secret = conf$pds$secret,
        id = trip_id,
        deviceInfo = TRUE
      )

      # Save to parquet
      arrow::write_parquet(
        x = track_data,
        sink = track_filename,
        compression = "lz4",
        compression_level = 12
      )

      # Upload to cloud
      logger::log_info("Uploading track for trip {trip_id}")
      upload_cloud_file(
        file = track_filename,
        provider = conf$pds_storage$google$key,
        options = conf$pds_storage$google$options
      )

      # Clean up local file
      unlink(track_filename)

      list(
        status = "success",
        trip_id = trip_id,
        message = "Successfully processed"
      )
    },
    error = function(e) {
      list(
        status = "error",
        trip_id = trip_id,
        message = e$message
      )
    }
  )
}


#' Backup Pelagic Tracks (Fallback)
#'
#' @description
#' Fallback pipeline to refresh the bound tracks parquet when the main ingestion is unavailable. It:
#' 1. Loads Airtable assets, keeping East Africa devices (Zanzibar, Kenya, Mozambique, Malawi, Tanzania) active since 2024-01-01
#' 2. Pulls the last 90 days of PDS trips and filters to those IMEIs
#' 3. Reads the latest bound tracks parquet from cloud, finds new trip IDs, and downloads missing tracks in parallel
#' 4. Aggregates points to 10-minute medians, binds with existing tracks, deduplicates, and uploads a versioned parquet back to cloud storage
#'
#' @return None (invisible). Updates the cloud parquet with any new track data.
#'
#' @examples
#' \dontrun{
#' backup_tracks()
#' }
#'
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @keywords workflow ingestion
#' @export
backup_tracks <- function(package = "coasts") {
  conf <- read_config(package = package)

  assets <- conf$metadata$airtable$name |>
    cloud_object_name(
      provider = conf$storage$google$key,
      extension = "rds",
      options = conf$storage$google$options
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds()

  assets$devices <-
    assets$devices |>
    dplyr::filter(stringr::str_detect(
      .data$customer_name,
      "Zanzibar|Kenya|Mozambique|Malawi|Tanzania"
    )) |>
    dplyr::mutate(
      last_seen = as.Date(as.POSIXct(
        .data$last_seen / 1000,
        origin = "1970-01-01"
      ))
    ) |>
    dplyr::filter(.data$last_seen >= "2024-01-01")

  boats_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = Sys.Date() - 90,
    dateTo = Sys.Date(),
    #imeis = unique(assets$devices$imei),
    deviceInfo = TRUE,
    withLastSeen = TRUE
  ) |>
    dplyr::filter(.data$IMEI %in% as.numeric(unique(assets$devices$imei)))

  logger::log_info("Download latest binded tracks dataframe ...")
  latest_df <-
    download_cloud_file(
      name = paste0(conf$tracks_app$all_tracks$file_prefix, ".parquet"),
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    arrow::read_parquet()

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(unique(latest_df$Trip))
  new_trip_ids <- setdiff(boats_trips$Trip, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info(
    "Processing {length(new_trip_ids)} new tracks in parallel..."
  )

  tracks_list <- furrr::future_map(
    new_trip_ids,
    function(trip_id) {
      tryCatch(
        {
          track_data <- get_trip_points(
            token = conf$pds$token,
            secret = conf$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )
        },
        error = function(e) {
          logger::log_error("Error processing trip {trip_id}: {e$message}")
          NULL
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  tracks_df <- tracks_list |>
    purrr::compact() |>
    dplyr::bind_rows() |>
    dplyr::select("Time", "Trip", "Lat", "Lng") |>
    dplyr::mutate(
      Time = lubridate::floor_date(.data$Time, unit = "10 minutes")
    ) |>
    dplyr::group_by(.data$Trip, .data$Time) |>
    dplyr::summarise(
      Lat = stats::median(.data$Lat),
      Lng = stats::median(.data$Lng),
      .groups = "drop"
    )

  future::plan(future::sequential)

  tracks_df <-
    boats_trips |>
    dplyr::select("Trip", "IMEI") |>
    dplyr::distinct() |>
    dplyr::right_join(tracks_df, by = "Trip") |>
    dplyr::select("IMEI", "Trip", "Time", "Lat", "Lng")

  binded_tracks <-
    dplyr::bind_rows(tracks_df, latest_df) |>
    dplyr::distinct() |>
    dplyr::filter(!is.na(.data$IMEI))

  logger::log_info("Converting data to Parquet format...")

  # Write parquet file
  arrow::write_parquet(
    x = binded_tracks,
    sink = paste0(conf$tracks_app$all_tracks$file_prefix, ".parquet"),
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Upload backup tracks to cloud")

  upload_cloud_file(
    file = paste0(conf$tracks_app$all_tracks$file_prefix, ".parquet"),
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
