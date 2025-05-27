#' Preprocess Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' Downloads raw GPS tracks and creates a gridded summary of fishing activity.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param grid_size Numeric. Size of grid cells in meters (100, 250, 500, or 1000).
#'
#' @return None (invisible). Creates and uploads preprocessed files.
#'
#' @keywords workflow preprocessing
#' @export
preprocess_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  grid_size = 500
) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get already preprocessed tracks
  logger::log_info("Checking existing preprocessed tracks...")
  preprocessed_filename <- cloud_object_name(
    prefix = paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed"),
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_tracks$version,
    options = pars$storage$google$options
  )

  # Get preprocessed trip IDs if file exists
  preprocessed_trips <- tryCatch(
    {
      download_cloud_file(
        name = preprocessed_filename,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
      preprocessed_data <- arrow::read_parquet(preprocessed_filename)
      unique(preprocessed_data$Trip)
    },
    error = function(e) {
      logger::log_info("No existing preprocessed tracks file found")
      character(0)
    }
  )

  # List raw tracks
  logger::log_info("Listing raw tracks...")
  raw_tracks <- googleCloudStorageR::gcs_list_objects(
    bucket = pars$pds_storage$google$options$bucket,
    prefix = pars$pds$pds_tracks$file_prefix
  )$name

  raw_trip_ids <- extract_trip_ids_from_filenames(raw_tracks)
  new_trip_ids <- setdiff(raw_trip_ids, preprocessed_trips)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to preprocess")
    return(invisible())
  }

  # Get raw tracks that need preprocessing
  new_tracks <- raw_tracks[raw_trip_ids %in% new_trip_ids]

  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info("Processing {length(new_tracks)} tracks in parallel...")
  new_processed_data <- furrr::future_map_dfr(
    new_tracks,
    function(track_file) {
      download_cloud_file(
        name = track_file,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      track_data <- arrow::read_parquet(track_file) %>%
        preprocess_track_data(grid_size = grid_size)

      unlink(track_file)
      track_data
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  future::plan(future::sequential)

  # Combine with existing preprocessed data if it exists
  final_data <- if (length(preprocessed_trips) > 0) {
    dplyr::bind_rows(preprocessed_data, new_processed_data)
  } else {
    new_processed_data
  }

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    final_data,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  unlink(output_filename)
  if (exists("preprocessed_filename")) unlink(preprocessed_filename)

  logger::log_success("Track preprocessing complete")

  grid_summaries <- generate_track_summaries(final_data)

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    grid_summaries,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Preprocess Track Data into Spatial Grid Summary
#'
#' @description
#' This function processes GPS track data into a spatial grid summary, calculating time spent
#' and other metrics for each grid cell. The grid size can be specified to analyze spatial
#' patterns at different scales.
#'
#' @param data A data frame containing GPS track data with columns:
#'   - Trip: Unique trip identifier
#'   - Time: Timestamp of the GPS point
#'   - Lat: Latitude
#'   - Lng: Longitude
#'   - Speed (M/S): Speed in meters per second
#'   - Range (Meters): Range in meters
#'   - Heading: Heading in degrees
#'
#' @param grid_size Numeric. Size of grid cells in meters. Must be one of:
#'   - 100: ~100m grid cells
#'   - 250: ~250m grid cells
#'   - 500: ~500m grid cells (default)
#'   - 1000: ~1km grid cells
#'
#' @return A tibble with the following columns:
#'   - Trip: Trip identifier
#'   - lat_grid: Latitude of grid cell center
#'   - lng_grid: Longitude of grid cell center
#'   - time_spent_mins: Total time spent in grid cell in minutes
#'   - mean_speed: Average speed in grid cell (M/S)
#'   - mean_range: Average range in grid cell (Meters)
#'   - first_seen: First timestamp in grid cell
#'   - last_seen: Last timestamp in grid cell
#'   - n_points: Number of GPS points in grid cell
#'
#' @details
#' The function creates a grid by rounding coordinates based on the specified grid size.
#' Grid sizes are approximate due to the conversion from meters to degrees, with calculations
#' based on 1 degree ≈ 111km at the equator. Time spent is calculated using the time
#' differences between consecutive points.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' # Process tracks with 500m grid (default)
#' result_500m <- preprocess_track_data(tracks_data)
#'
#' # Use 100m grid for finer resolution
#' result_100m <- preprocess_track_data(tracks_data, grid_size = 100)
#'
#' # Use 1km grid for broader patterns
#' result_1km <- preprocess_track_data(tracks_data, grid_size = 1000)
#' }
#'
#' @keywords preprocessing
#' @export
preprocess_track_data <- function(data, grid_size = 500) {
  # Define grid size in meters to degrees (approximately)
  # 1 degree = 111km at equator
  grid_degrees <- switch(
    as.character(grid_size),
    "100" = 0.001, # ~100m
    "250" = 0.0025, # ~250m
    "500" = 0.005, # ~500m
    "1000" = 0.01, # ~1km
    stop("grid_size must be one of: 100, 250, 500, 1000")
  )

  data %>%
    dplyr::select(
      "Trip",
      "Time",
      "Lat",
      "Lng",
      "Speed (M/S)",
      "Range (Meters)",
      "Heading"
    ) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$Time) %>%
    dplyr::mutate(
      # Create grid cells based on selected size
      lat_grid = round(.data$Lat / grid_degrees, 0) * grid_degrees,
      lng_grid = round(.data$Lng / grid_degrees, 0) * grid_degrees,

      # Calculate time spent (difference with next point)
      time_diff = as.numeric(difftime(
        dplyr::lead(.data$Time),
        .data$Time,
        units = "mins"
      )),
      # For last point in series, use difference with previous point
      time_diff = dplyr::if_else(
        is.na(.data$time_diff),
        as.numeric(difftime(
          .data$Time,
          dplyr::lag(.data$Time),
          units = "mins"
        )),
        .data$time_diff
      )
    ) %>%
    # Group by trip and grid cell
    dplyr::group_by(.data$Trip, .data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      time_spent_mins = sum(.data$time_diff, na.rm = TRUE),
      mean_speed = mean(.data$`Speed (M/S)`, na.rm = TRUE),
      mean_range = mean(.data$`Range (Meters)`, na.rm = TRUE),
      first_seen = min(.data$Time),
      last_seen = max(.data$Time),
      n_points = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::filter(.data$time_spent_mins > 0) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$first_seen) %>%
    dplyr::filter(
      !(dplyr::row_number() %in% c(1, 2, dplyr::n() - 1, dplyr::n()))
    ) %>%
    dplyr::ungroup()
}

#' Generate Grid Summaries for Track Data
#'
#' @description
#' Processes GPS track data into 1km grid summaries for visualization and analysis.
#'
#' @param data Preprocessed track data
#' @param min_hours Minimum hours threshold for filtering (default: 0.15)
#' @param max_hours Maximum hours threshold for filtering (default: 10)
#'
#' @return A dataframe with grid summary statistics
#'
#' @keywords preprocessing
#' @export
generate_track_summaries <- function(data, min_hours = 0.15, max_hours = 15) {
  data %>%
    # First summarize by current grid (500m)
    dplyr::group_by(.data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$time_spent_mins),
      avg_speed = mean(.data$mean_speed),
      avg_range = mean(.data$mean_range),
      visits = dplyr::n_distinct(.data$Trip),
      total_points = sum(.data$n_points),
      .groups = "drop"
    ) %>%
    # Then regrid to 1km
    dplyr::mutate(
      lat_grid_1km = round(.data$lat_grid / 0.01) * 0.01,
      lng_grid_1km = round(.data$lng_grid / 0.01) * 0.01
    ) %>%
    dplyr::group_by(.data$lat_grid_1km, .data$lng_grid_1km) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$avg_time_mins),
      avg_speed = mean(.data$avg_speed),
      avg_range = mean(.data$avg_range),
      total_visits = sum(.data$visits),
      original_cells = dplyr::n(),
      total_points = sum(.data$total_points),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      avg_time_hours = .data$avg_time_mins / 60
    ) %>%
    dplyr::filter(
      .data$avg_time_hours >= min_hours,
      .data$avg_time_hours <= max_hours
    ) |>
    dplyr::select(-"avg_time_mins")
}
