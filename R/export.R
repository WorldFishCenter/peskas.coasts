#' Export Geospatial Data and Regional Metrics to MongoDB
#'
#' @description
#' Downloads and processes geospatial data from two sources (Kenya and Zanzibar),
#' harmonizes their structures, applies currency conversions to regional time series
#' metrics, and exports all data to MongoDB collections with appropriate indexing.
#'
#' @details
#' This function performs several key operations:
#'
#' **Geospatial Data Processing:**
#' 1. Downloads GeoJSON files for Kenya and Zanzibar regions from cloud storage
#' 2. Reads and combines the regional boundary data into a single dataset
#' 3. Uploads combined geospatial data to MongoDB with 2dsphere indexing for spatial queries
#'
#' **Regional Metrics Processing:**
#' 4. Downloads monthly summary parquet files for both countries from cloud storage
#' 5. Applies currency conversion factors to monetary metrics:
#'    - Zanzibar: multiplies values by 0.00037 (TZS to USD conversion)
#'    - Kenya: multiplies values by 0.0077 (KES to USD conversion)
#' 6. Converts the following monetary fields: mean_rpue, mean_rpua, mean_price_kg
#' 7. Uploads regional metrics to MongoDB without geospatial indexing
#'
#' **PDS Track Grid Processing:**
#' 8. Downloads PDS track grid summaries from cloud storage
#' 9. Uploads grid summaries to a separate MongoDB collection
#'
#' The function requires appropriate configuration parameters for cloud storage access
#' and MongoDB connection details, typically loaded via `read_config()`.
#'
#' @return None (invisible). Creates and uploads data to three MongoDB collections:
#'   - Regional boundary geometries (with 2dsphere index)
#'   - Regional time series metrics (currency-converted)
#'   - PDS track grid summaries
#'
#' @examples
#' \dontrun{
#' # Export all geospatial data and metrics to MongoDB
#' export_geos()
#' }
#'
#' @importFrom logger log_info log_success
#' @importFrom purrr map walk set_names
#' @importFrom dplyr select mutate bind_rows case_when %>%
#' @importFrom sf read_sf
#' @importFrom arrow read_parquet
#'
#' @seealso
#' \code{\link{mdb_collection_push}} for MongoDB upload functionality
#' \code{\link{download_cloud_file}} for cloud storage operations
#' \code{\link{read_config}} for configuration management
#'
#' @keywords database export geospatial mongodb timeseries
#' @export
export_geos <- function() {
  # Load configuration settings
  conf <- read_config()
  logger::log_info("Loading geospatial data from cloud storage...")

  # Step 1: Download and read geospatial files from cloud storage
  maps <-
    c(
      "KE_regions",
      "ZAN_regions",
      #"CO_regions",
      "MOZ_regions"
    ) |>
    purrr::set_names() |>
    purrr::map(
      ~ cloud_object_name(
        prefix = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options,
        extension = "geojson",
        version = "latest"
      )
    ) |>
    purrr::walk(
      ~ download_cloud_file(
        name = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ) |>
    purrr::map(
      ~ sf::read_sf(.x)
    ) |>
    dplyr::bind_rows()

  # Step 2: Download and read time series data files from cloud storage
  series <-
    c(
      "kenya_monthly_summaries_map",
      "zanzibar_monthly_summaries_map",
      "mozambique_monthly_summaries_map"
    ) |>
    purrr::set_names() |>
    purrr::map(
      ~ cloud_object_name(
        prefix = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options,
        extension = "parquet",
        version = "latest"
      )
    ) |>
    purrr::walk(
      ~ download_cloud_file(
        name = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ) |>
    purrr::map(
      ~ arrow::read_parquet(.x)
    ) |>
    dplyr::bind_rows() |>
    dplyr::mutate(
      mean_rpue = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_rpue * 0.00037,
        country == "kenya" ~ .data$mean_rpue * 0.0077,
        country == "mozambique" ~ .data$mean_rpue * 0.016,
        TRUE ~ .data$mean_rpue
      ),
      mean_rpua = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_rpua * 0.00037,
        country == "kenya" ~ .data$mean_rpua * 0.0077,
        country == "mozambique" ~ .data$mean_rpue * 0.016,
        TRUE ~ .data$mean_rpue
      ),
      mean_price_kg = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_price_kg * 0.00037,
        country == "kenya" ~ .data$mean_price_kg * 0.0077,
        country == "mozambique" ~ .data$mean_price_kg * 0.016,
        TRUE ~ .data$mean_price_kg
      ),
    )

  # Step 6: Push combined geospatial data to MongoDB with 2dsphere indexing
  logger::log_info("Pushing combined geospatial data to MongoDB...")
  mdb_collection_push(
    data = maps,
    connection_string = conf$storage$mongodb$coasts_portal$connection_string,
    collection_name = conf$storage$mongodb$coasts_portal$collection$wio_map,
    db_name = conf$storage$mongodb$coasts_portal$database_name,
    geo = TRUE # Create 2dsphere index on geometry field
  )

  logger::log_info("Pushing regional time series metrics to MongoDB...")
  mdb_collection_push(
    data = series,
    connection_string = conf$storage$mongodb$coasts_portal$connection_string,
    collection_name = conf$storage$mongodb$coasts_portal$collection$regional_metrics,
    db_name = conf$storage$mongodb$coasts_portal$database_name,
    geo = FALSE # No geospatial indexing needed for time series
  )

  # Step 7: Download and process PDS track grid summaries
  logger::log_info("Downloading PDS track grid summaries...")
  grid_summaries <-
    download_parquet_from_cloud(
      prefix = "pds-tracks-grid_summaries",
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Step 8: Push grid summaries to MongoDB (without geospatial indexing)
  logger::log_info("Pushing PDS track grid summaries to MongoDB...")
  mdb_collection_push(
    data = grid_summaries,
    connection_string = conf$storage$mongodb$coasts_portal$connection_string,
    collection_name = conf$storage$mongodb$coasts_portal$collection$pds_grids,
    db_name = conf$storage$mongodb$coasts_portal$database_name,
    geo = FALSE # No geospatial indexing needed
  )

  logger::log_success("Successfully exported geospatial data to MongoDB")
  return(invisible(NULL))
}


#' Export Fisher Performance Statistics to MongoDB
#'
#' @description
#' Combines catch event data with GPS tracking trips to create fisher performance
#' metrics and exports them to MongoDB for analysis.
#'
#' @details
#' This function integrates two data sources:
#' 1. Downloads catch events from tracks-app MongoDB (fisher-reported landings)
#' 2. Retrieves corresponding GPS tracking data from PDS API
#' 3. Matches catch events to tracking trips by date and device ID
#' 4. Calculates fishing efficiency metrics (CPUE, fuel efficiency, search patterns)
#' 5. Exports fisher statistics and performance metrics to MongoDB collections
#'
#' Performance metrics include:
#' - CPUE (kg per hour, kg per km traveled)
#' - Search efficiency (distance vs range ratios)
#' - Fuel efficiency estimates
#' - Trip categorization (nearshore/mid-range/offshore)
#'
#' @return None (invisible). Creates and uploads data to two MongoDB collections:
#'   - Fisher catch statistics (aggregated summaries)
#'   - Trip-level performance metrics
#'
#' @examples
#' \dontrun{
#' # Export fisher statistics and performance metrics
#' export_fishers_stats()
#' }
#'
#' @seealso
#' \code{\link{export_geos}} for exporting geospatial data
#' \code{\link{mdb_collection_push}} for MongoDB upload functionality
#'
#' @keywords database export fisheries performance
#' @export
export_fishers_stats <- function() {
  conf <- read_config()

  trips <-
    mdb_collection_pull(
      connection_string = conf$storage$mongodb$tracks_app$connection_string,
      collection_name = conf$storage$mongodb$tracks_app$collection$catch_events,
      db_name = conf$storage$mongodb$tracks_app$database_name
    ) |>
    dplyr::as_tibble() |>
    dplyr::filter(is.na(.data$isAdminSubmission)) |>
    dplyr::select(
      -c("photos", "gps_photo", "community", "reportedAt", "createdAt")
    ) |>
    dplyr::mutate(
      quantity = dplyr::if_else(.data$catch_outcome == "0", 0, .data$quantity),
      date = lubridate::date(.data$date),
      date = lubridate::as_datetime(.data$date)
    )

  fishers_stats <-
    unique(trips$imei) |>
    purrr::map_dfr(get_fisher_summaries, catch_events = trips) |>
    dplyr::arrange(.data$imei, .data$date) |>
    dplyr::relocate("imei", .after = "date")

  pds_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = min(fishers_stats$date),
    dateTo = Sys.Date(),
    imeis = unique(fishers_stats$imei),
    deviceInfo = TRUE
  ) |>
    janitor::clean_names() |>
    dplyr::mutate(
      imei = as.character(.data$imei),
      trip = as.character(.data$trip)
    ) |>
    dplyr::rename(tripId = "trip")

  already_matched <-
    fishers_stats |>
    dplyr::inner_join(
      pds_trips,
      by = c("tripId", "imei")
    )

  unique_trips <-
    fishers_stats |>
    # exclude already matched trips
    dplyr::filter(!.data$tripId %in% unique(already_matched$tripId)) |>
    dplyr::group_by(.data$date, .data$imei) |>
    dplyr::mutate(
      unique_trip_per_day = dplyr::n_distinct(.data$tripId) == 1,
    ) |>
    dplyr::ungroup() %>%
    dplyr::filter(.data$unique_trip_per_day == TRUE) |>
    dplyr::select(-"unique_trip_per_day", trip = "tripId")

  unique_pds_trips <-
    pds_trips %>%
    # exclude already matched trips
    dplyr::filter(!.data$tripId %in% unique(already_matched$tripId)) |>
    # We assume the landing date to be the same as the date when the trip ended
    dplyr::mutate(
      date = lubridate::as_date(.data$ended),
      tripId = as.character(.data$tripId)
    ) %>%
    dplyr::group_by(.data$date, .data$imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n_distinct(.data$tripId) == 1) |>
    dplyr::ungroup() %>%
    dplyr::filter(.data$unique_trip_per_day == TRUE) |>
    dplyr::select(-"unique_trip_per_day")

  logger::log_info("Merging datasets datasets...")
  # Only join when we have one landing and one tracking per day, otherwise we
  # cannot do guarantee that the landing corresponds to a trip
  merged_trips <- dplyr::full_join(
    unique_trips,
    unique_pds_trips,
    by = c("date", "imei")
  ) |>
    # add already matched
    dplyr::bind_rows(already_matched) |>
    dplyr::filter(!is.na(.data$trip) & !is.na(.data$tripId))

  performance_metrics <-
    merged_trips |>
    dplyr::select(
      "tripId",
      "imei",
      "started",
      "ended",
      "duration_seconds",
      "range_meters",
      "distance_meters",
      "fishGroup",
      "catch_kg"
    ) |>
    dplyr::mutate(
      trip_duration = as.numeric(difftime(
        .data$ended,
        .data$started,
        units = "hours"
      ))
    ) |>
    dplyr::mutate(
      # Basic efficiency metrics
      cpue_kg_per_hour = .data$catch_kg / .data$trip_duration,
      cpue_kg_per_km = .data$catch_kg / (.data$distance_meters / 1000),
      # Search efficiency
      search_ratio = .data$distance_meters / .data$range_meters,
      catch_per_search = .data$catch_kg / .data$search_ratio,
      # Fuel estimates (rough)
      est_fuel_liters = (.data$distance_meters / 1000) * 0.4, # ~0.4 liters/km
      kg_per_liter = .data$catch_kg / .data$est_fuel_liters,
      # Time efficiency
      hour_of_day = lubridate::hour(.data$started),
      # Distance categories for comparison
      trip_type = dplyr::case_when(
        .data$range_meters / 1000 < 5 ~ "nearshore",
        .data$range_meters / 1000 < 20 ~ "mid-range",
        TRUE ~ "offshore"
      )
    ) |>
    dplyr::select(
      "tripId",
      "imei",
      "started",
      "ended",
      "trip_duration",
      "cpue_kg_per_hour",
      "cpue_kg_per_km",
      "search_ratio",
      "catch_per_search",
      "est_fuel_liters",
      "kg_per_liter",
      "hour_of_day",
      "trip_type"
    ) |>
    tidyr::pivot_longer(
      cols = c(
        "trip_duration",
        "cpue_kg_per_hour",
        "cpue_kg_per_km",
        "search_ratio",
        "catch_per_search",
        "est_fuel_liters",
        "kg_per_liter",
        "hour_of_day"
      ),
      names_to = "metric",
      values_to = "value"
    )

  # Step 6: Push combined geospatial data to MongoDB with 2dsphere indexing
  logger::log_info("Pushing combined geospatial data to MongoDB...")
  mdb_collection_push(
    data = fishers_stats,
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = conf$storage$mongodb$tracks_app$collection$stats,
    db_name = conf$storage$mongodb$tracks_app$database_name,
    geo = FALSE
  )

  logger::log_info("Pushing regional time series metrics to MongoDB...")
  mdb_collection_push(
    data = performance_metrics,
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = conf$storage$mongodb$tracks_app$collection$performances,
    db_name = conf$storage$mongodb$tracks_app$database_name,
    geo = FALSE
  )
}
