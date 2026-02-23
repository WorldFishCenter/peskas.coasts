#' Export Geospatial Data and Regional Metrics to MongoDB
#'
#' @description
#' Downloads and processes geospatial data from different countries,
#' harmonizes their structures, applies currency conversions to regional time series
#' metrics, and exports all data to MongoDB collections with appropriate indexing.
#'
#' @details
#' This function performs several key operations:
#'
#' **Geospatial Data Processing:**
#' 1. Downloads GeoJSON files for Kenya, Mozambique and Zanzibar regions from cloud storage
#' 2. Reads and combines the regional boundary data into a single dataset
#' 3. Uploads combined geospatial data to MongoDB with 2dsphere indexing for spatial queries
#'
#' **Regional Metrics Processing:**
#' 4. Downloads monthly summary parquet files for both countries from cloud storage
#' 5. Applies currency conversion factors to monetary metrics
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
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @seealso
#' \code{\link{mdb_collection_push}} for MongoDB upload functionality
#' \code{\link{download_cloud_file}} for cloud storage operations
#' \code{\link{read_config}} for configuration management
#'
#' @keywords database export geospatial mongodb timeseries
#' @export
export_geos <- function(package = "coasts") {
  # Load configuration settings
  conf <- read_config(package = package)
  logger::log_info("Loading geospatial data from cloud storage...")

  # Step 1: Download and read geospatial files from cloud storage
  maps <-
    c(
      "KEN_boundaries_gaul",
      "ZAN_boundaries_gaul",
      #"CO_regions",
      "MOZ_boundaries_gaul"
    ) |>
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
    purrr::flatten_chr() |>
    purrr::set_names() |>
    purrr::map(
      ~ sf::read_sf(.x)
    ) |>
    dplyr::bind_rows(.id = "source") |>
    dplyr::select(dplyr::any_of(c(
      "source",
      "iso3_code",
      "gaul1_name",
      "gaul2_name",
      "geometry"
    )))

  # define Gaul1 and Gaul2
  maps_gaul2 <- maps |>
    dplyr::filter(stringr::str_detect(.data$source, "gaul2")) |>
    dplyr::select(-"source")

  maps_gaul1 <- maps |>
    dplyr::filter(stringr::str_detect(.data$source, "gaul1")) |>
    dplyr::select(-c("source", "gaul2_name"))

  map_list <- list(maps_gaul1, maps_gaul2)
  # Step 2: Download and read time series data files from cloud storage and define gaul1 and gaul2 resolution
  series_gaul2 <-
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
      # mean_rpua = dplyr::case_when(
      #   country == "zanzibar" ~ .data$mean_rpua * 0.00037,
      #   country == "kenya" ~ .data$mean_rpua * 0.0077,
      #   country == "mozambique" ~ .data$mean_rpue * 0.016,
      #   TRUE ~ .data$mean_rpue
      # ),
      mean_price_kg = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_price_kg * 0.00039,
        country == "kenya" ~ .data$mean_price_kg * 0.0078,
        country == "mozambique" ~ .data$mean_price_kg * 0.016,
        TRUE ~ .data$mean_price_kg
      ),
    ) |>
    #TODO: ensure there are no data in the future and then remove this filter
    dplyr::filter(.data$date <= Sys.Date())

  series_gaul1 <-
    series_gaul2 |>
    dplyr::select(-c("gaul_2_name")) |>
    dplyr::group_by(.data$country, .data$gaul1_name, .data$date) |>
    dplyr::summarise(
      dplyr::across(dplyr::everything(), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    )

  series_list <- list(series_gaul1, series_gaul2)

  # Step 6: Push combined geospatial data to MongoDB with 2dsphere indexing
  map_collection_names <- c(
    conf$storage$mongodb$coasts_portal$collection$wio_gaul1,
    conf$storage$mongodb$coasts_portal$collection$wio_gaul2
  )

  series_collection_names <- c(
    conf$storage$mongodb$coasts_portal$collection$metrics_gaul1,
    conf$storage$mongodb$coasts_portal$collection$metrics_gaul2
  )

  logger::log_info("Pushing combined geospatial data to MongoDB...")
  purrr::walk2(
    .x = map_list,
    .y = map_collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$coasts_portal$connection_string,
        collection_name = .y,
        db_name = conf$storage$mongodb$coasts_portal$database_name,
        geo = TRUE
      )
    }
  )

  logger::log_info("Pushing regional time series metrics to MongoDB...")
  purrr::walk2(
    .x = series_list,
    .y = series_collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$coasts_portal$connection_string,
        collection_name = .y,
        db_name = conf$storage$mongodb$coasts_portal$database_name,
        geo = FALSE
      )
    }
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
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @seealso
#' \code{\link{export_geos}} for exporting geospatial data
#' \code{\link{mdb_collection_push}} for MongoDB upload functionality
#'
#' @keywords database export fisheries performance
#' @export
export_fishers_stats <- function(package = "coasts") {
  conf <- read_config(package = package)

  self_registered_users_df <-
    mdb_collection_pull(
      connection_string = conf$storage$mongodb$tracks_app$connection_string,
      collection_name = conf$storage$mongodb$tracks_app$collection$users,
      db_name = conf$storage$mongodb$tracks_app$database_name
    ) |>
    dplyr::as_tibble() |>
    dplyr::mutate(
      registrationType = dplyr::if_else(
        !is.na(.data$IMEI),
        "imei-registered",
        "self-registered"
      )
    ) |>
    dplyr::filter(.data$registrationType == "self-registered") |>
    dplyr::select(dplyr::any_of("username"))

  self_registered_users <-
    if (length(self_registered_users_df) == 0) {
      character(0)
    } else {
      self_registered_users_df |>
        dplyr::pull() |>
        unique()
    }

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
    ) |>
    dplyr::mutate(
      user_id = dplyr::if_else(
        .data$username %in% self_registered_users,
        .data$username,
        .data$imei
      )
    )

  fishers_stats <-
    unique(trips$user_id) |>
    purrr::map_dfr(get_fisher_summaries, catch_events = trips) |>
    dplyr::distinct() |>
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
  logger::log_info("Pushing fisher statistics to MongoDB...")
  mdb_collection_push(
    data = fishers_stats,
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = conf$storage$mongodb$tracks_app$collection$stats,
    db_name = conf$storage$mongodb$tracks_app$database_name,
    geo = FALSE
  )

  logger::log_info("Pushing performance_metrics MongoDB...")
  mdb_collection_push(
    data = performance_metrics,
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = conf$storage$mongodb$tracks_app$collection$performances,
    db_name = conf$storage$mongodb$tracks_app$database_name,
    geo = FALSE
  )
}

#' Export Summary Data to MongoDB
#'
#' @description
#' Downloads previously summarized WorldFish survey data from cloud storage, incorporates
#' modeled aggregated estimates, and exports everything to MongoDB collections for use
#' in data portals. The function also generates geographic regional summaries.
#'
#' @details
#' The function performs the following operations:
#' - Downloads five summary datasets from cloud storage:
#'   - Monthly summaries: Aggregated catch metrics by district and month
#'   - Taxa summaries: Species-specific metrics in long format
#'   - Districts summaries: District-level indicators over time
#'   - Gear summaries: Performance metrics by gear type
#'   - Grid summaries: Spatial grid data from vessel tracking
#' - Downloads aggregated catch estimates from the modeling step
#' - Creates geographic regional summaries using the monthly data
#' - Joins aggregated estimates (fishing trips, catch tonnage, revenue) to monthly summaries
#' - Transforms monthly summaries to long format for portal consumption
#' - Uploads all datasets to specified MongoDB collections
#'
#' The function expects the summary files to be named with the pattern:
#' `{file_prefix}_{table_name}.parquet` where table_name is one of:
#' monthly_summaries, taxa_summaries, districts_summaries, gear_summaries, grid_summaries
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return NULL (invisible). The function uploads data to MongoDB as a side effect.
#'
#' @examples
#' \dontrun{
#' # Export WF summary data with default debug logging
#' export_portal()
#'
#' # Export with info-level logging only
#' export_portal(logger::INFO)
#' }
#'
#' @seealso
#' * [summarize_data()] for generating the summary datasets
#' * [download_parquet_from_cloud()] for retrieving data from cloud storage
#' * [mdb_collection_push()] for uploading data to MongoDB
#'
#' @keywords workflow export
#' @export
export_portal <- function(log_threshold = logger::DEBUG, package = "coasts") {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  map <- cloud_object_name(
    prefix = conf$metadata$map_boundaries$prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts,
    version = "latest",
    extension = "geojson"
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts
    ) |>
    sf::st_read() |>
    dplyr::select(
      -c("map_code", "gaul0_code", "gaul0_name", "continent", "disp_en")
    )

  # Download each parquet file
  data_summaries <- list()

  table_names <- c(
    "monthly_summaries",
    "taxa_summaries",
    "districts_summaries",
    "gear_summaries",
    "grid_summaries"
  )

  for (name in table_names) {
    prefix <- conf$surveys$summaries$file_prefix %>%
      paste0("_", name)

    data_summaries[[name]] <- download_parquet_from_cloud(
      prefix = prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )
  }

  # Create geographic summaries
  region_monthly_summaries <-
    data_summaries$monthly_summaries |>
    dplyr::left_join(map, by = c("gaul_2_name" = "gaul2_name")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date) |>
    dplyr::summarise(
      gaul_1_name = dplyr::first(.data$gaul1_name),
      mean_cpue = stats::median(.data$mean_cpue_day, na.rm = TRUE),
      mean_rpue = stats::median(.data$mean_rpue_day, na.rm = TRUE),
      mean_price_kg = stats::median(.data$mean_price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      date = format(.data$date, "%Y-%m-%dT%H:%M:%SZ"),
      country = conf$country
    ) |>
    dplyr::select(
      "country",
      gaul1_name = "gaul_1_name",
      "gaul_2_name",
      "date",
      "mean_cpue",
      "mean_rpue",
      "mean_price_kg"
    )

  upload_parquet_to_cloud(
    data = region_monthly_summaries,
    prefix = paste0(conf$country, "_monthly_summaries_map"),
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts
  )

  logger::log_info("Downloading aggregated catch data from cloud storage...")
  aggregated_filename <- cloud_object_name(
    prefix = conf$surveys$aggregated$file_prefix,
    provider = conf$storage$google$key,
    extension = "rds",
    options = conf$storage$google$options
  )

  download_cloud_file(
    name = aggregated_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  aggregated_data <- readr::read_rds(aggregated_filename)

  monthly_aggregated <-
    aggregated_data$district_totals |>
    dplyr::mutate(estimated_catch_tn = .data$estimated_total_catch_kg / 1000) |>
    dplyr::select(
      "gaul_2_name",
      "date" = "date_month",
      "estimated_fishing_trips" = "estimated_total_trips",
      "estimated_catch_tn",
      "estimated_revenue" = "estimated_total_revenue"
    )

  # Transform monthly summaries to long format for portal
  monthly_summaries <-
    data_summaries$monthly_summaries |>
    dplyr::left_join(monthly_aggregated, by = c("gaul_2_name", "date")) |>
    dplyr::relocate(
      "estimated_fishing_trips",
      .after = "date"
    ) |>
    dplyr::select(-c("mean_cpue_day", "mean_rpue_day")) |> # Drop map-specific metrics
    tidyr::pivot_longer(
      -c("date", "gaul_2_name"),
      names_to = "metric",
      values_to = "value"
    )

  districts_summaries <-
    data_summaries$districts_summaries |>
    dplyr::full_join(monthly_aggregated, by = c("gaul_2_name", "date")) |>
    dplyr::select(dplyr::where(~ !all(is.na(.))), -"estimated_fishing_trips") |>
    tidyr::pivot_longer(
      -c("date", "gaul_2_name"),
      names_to = "indicator",
      values_to = "value"
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = data_summaries$taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = data_summaries$gear_summaries,
    grid_summaries = data_summaries$grid_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_summaries = conf$storage$mongodb$databases$dashboard$collections$monthly_summaries,
    taxa_summaries = conf$storage$mongodb$databases$dashboard$collections$taxa_summaries,
    districts_summaries = conf$storage$mongodb$databases$dashboard$collections$districts_summaries,
    gear_summaries = conf$storage$mongodb$databases$dashboard$collections$gear_summaries,
    grid_summaries = conf$storage$mongodb$databases$dashboard$collections$grid_summaries
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$connection_strings$main,
        collection_name = .y,
        db_name = conf$storage$mongodb$databases$dashboard$database_name
      )
    }
  )
}
