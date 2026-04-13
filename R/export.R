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
      "KEN_boundaries_gaul1",
      "ZAN_boundaries_gaul1",
      "MOZ_boundaries_gaul1",
      "KEN_boundaries_gaul2",
      "ZAN_boundaries_gaul2",
      "MOZ_boundaries_gaul2"
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
    prefix = conf$metadata$map_boundaries$gaul2,
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


#' Export Spatial Pipeline Outputs as Web-Ready JSON / GeoJSON
#'
#' @description
#' Reads the pre-computed H3 effort grid and CPUE Parquet files from cloud
#' storage, derives fishing grounds, and uploads web-ready files that can be
#' fetched directly by a Next.js / DeckGL application.
#'
#' Three versioned files are produced (following the package's `add_version()`
#' convention, i.e. `prefix__YYYYMMDDHHMMSS__extension`):
#' - `pds-h3-effort-r{h3_res}__*__json`: JSON array for `H3HexagonLayer`
#' - `pds-cpue-r{h3_res}__*__json`: JSON array for `H3HexagonLayer`
#' - `pds-fishing-grounds__*__geojson`: GeoJSON `FeatureCollection` for `GeoJsonLayer`
#'
#' @details
#' Files follow the same versioning convention as other pipeline outputs
#' (`add_version()`). The consuming Next.js app uses `cloud_object_name()` or an
#' equivalent GCS listing + timestamp sort to discover the latest version.
#'
#' Schema for each file:
#' - **Effort JSON** (per H3 cell × year):
#'   `[{h3_index, year, fishing_hours, unique_trips, n_active_days,
#'   avg_fidelity, constancy, avg_hours_per_day, avg_visits_per_day,
#'   hours_per_trip}]`
#' - **CPUE JSON**: `[{h3_index, country, species, cpue, n_trips, lon, lat}]`
#' - **Fishing grounds GeoJSON** (same metrics, plus ground-specific):
#'   `{ground_id, area_km2, n_cells, fishing_hours, unique_trips, n_active_days,
#'   avg_fidelity, constancy, avg_hours_per_day, avg_visits_per_day,
#'   hours_per_trip, fishing_hours_per_km2, unique_trips_per_km2,
#'   hours_per_day_per_km2}`
#'
#' @param h3_res Integer. H3 resolution of the effort grid to export.
#'   Default is `9L`.
#' @param min_trips_grounds Integer. Minimum unique trips per H3 cell to
#'   include in fishing grounds derivation. Default is `3L`.
#' @param log_threshold Logging threshold. Default is `logger::DEBUG`.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`.
#'
#' @return Invisibly `NULL`. Uploads three files to GCS as a side effect.
#'
#' @seealso [aggregate_pds_effort()], [model_cpue()], [derive_fishing_grounds()]
#'
#' @keywords workflow export
#' @export
export_pds_spatial <- function(
  h3_res = 9L,
  min_trips_grounds = 3L,
  log_threshold = logger::DEBUG,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)
  coasts_opts <- resolve_storage_opts(conf, "coasts")
  cloud_storage_authenticate(conf$storage$google$key, coasts_opts)

  logger::log_info("=== Web spatial export | H3 res = {h3_res} ===")

  # -- Effort grid ---------------------------------------------------------------
  grid_prefix <- paste0(conf$pds$pds_tracks_h3_grid$file_prefix, "_r", h3_res)
  logger::log_info("Loading effort grid ({grid_prefix}) ...")
  effort <- tryCatch(
    download_parquet_from_cloud(
      prefix = grid_prefix,
      provider = conf$storage$google$key,
      options = coasts_opts
    ),
    error = function(e) {
      logger::log_warn(
        "Effort grid not found -- skipping export ({conditionMessage(e)})"
      )
      NULL
    }
  )
  if (is.null(effort)) {
    return(invisible(NULL))
  }
  logger::log_info(
    "Effort grid: {nrow(effort)} rows |",
    " {dplyr::n_distinct(effort$h3_index)} cells"
  )

  # Compute n_total_days from the stored date range so per-day metrics are
  # consistent between cell and ground exports.
  n_total_days <- if (
    all(c("first_active_date", "last_active_date") %in% names(effort)) &&
      !all(is.na(effort$first_active_date))
  ) {
    as.numeric(
      max(effort$last_active_date, na.rm = TRUE) -
        min(effort$first_active_date, na.rm = TRUE)
    ) +
      1
  } else {
    length(unique(effort$year)) * 365L
  }
  logger::log_info("Study period inferred: {round(n_total_days)} days")

  # Derive informative per-cell metrics; drop internal / QA columns.
  effort_export <- effort |>
    dplyr::mutate(
      avg_fidelity = dplyr::if_else(
        .data$n_trips_for_fidelity > 0,
        .data$avg_fidelity_sum / .data$n_trips_for_fidelity,
        NA_real_
      ),
      constancy = .data$n_active_days / n_total_days,
      avg_hours_per_day = .data$fishing_hours / n_total_days,
      avg_visits_per_day = .data$unique_trips / n_total_days,
      hours_per_trip = dplyr::if_else(
        .data$unique_trips > 0,
        .data$fishing_hours / .data$unique_trips,
        NA_real_
      )
    ) |>
    dplyr::select(
      "h3_index",
      "year",
      "fishing_hours",
      "unique_trips",
      "n_active_days",
      "avg_fidelity",
      "constancy",
      "avg_hours_per_day",
      "avg_visits_per_day",
      "hours_per_trip"
    )

  effort_prefix <- paste0(conf$pds$portal$effort$file_prefix, h3_res)
  effort_filename <- add_version(effort_prefix, extension = "json")
  on.exit(
    if (file.exists(effort_filename)) file.remove(effort_filename),
    add = TRUE
  )
  writeLines(
    jsonlite::toJSON(effort_export, auto_unbox = TRUE),
    effort_filename
  )
  upload_cloud_file(
    file = effort_filename,
    provider = conf$storage$google$key,
    options = coasts_opts
  )
  logger::log_info("Uploaded: {basename(effort_filename)}")

  # -- CPUE ----------------------------------------------------------------------
  cpue_prefix <- paste0(conf$pds$pds_cpue$file_prefix, "_r", h3_res)
  cpue <- tryCatch(
    {
      logger::log_info("Loading CPUE ({cpue_prefix}) ...")
      download_parquet_from_cloud(
        prefix = cpue_prefix,
        provider = conf$storage$google$key,
        options = coasts_opts
      )
    },
    error = function(e) {
      logger::log_warn(
        "CPUE file not found -- skipping ({conditionMessage(e)})"
      )
      NULL
    }
  )

  if (!is.null(cpue)) {
    logger::log_info("CPUE: {nrow(cpue)} rows")
    cpue_filename <- add_version(
      paste0(conf$pds$portal$cpue$file_prefix, h3_res),
      extension = "json"
    )
    on.exit(
      if (file.exists(cpue_filename)) file.remove(cpue_filename),
      add = TRUE
    )
    writeLines(
      jsonlite::toJSON(cpue, auto_unbox = TRUE, digits = 6),
      cpue_filename
    )
    upload_cloud_file(
      file = cpue_filename,
      provider = conf$storage$google$key,
      options = coasts_opts
    )
    logger::log_info("Uploaded: {basename(cpue_filename)}")
  }

  # -- Fishing grounds (GeoJSON) -------------------------------------------------
  logger::log_info(
    "Deriving fishing grounds (min_trips = {min_trips_grounds}) ..."
  )
  grounds <- derive_fishing_grounds(effort, min_trips = min_trips_grounds)

  if (!is.null(grounds)) {
    logger::log_info("Fishing grounds: {nrow(grounds)} polygons")

    # Explicit column selection mirrors the cell-level export: same informative
    # metrics, plus ground-specific ones (area, n_cells, per-km² densities).
    grounds_export <- dplyr::select(
      grounds,
      "ground_id",
      "area_km2",
      "fishing_hours",
      "unique_trips",
      "n_active_days",
      "avg_fidelity",
      "constancy",
      "avg_hours_per_day",
      "avg_visits_per_day",
      "hours_per_trip",
      "fishing_hours_per_km2",
      "unique_trips_per_km2",
      "hours_per_day_per_km2",
      "geometry"
    )

    grounds_filename <- add_version(
      conf$pds$portal$fishing_grounds$file_prefix,
      extension = "geojson"
    )
    on.exit(
      if (file.exists(grounds_filename)) file.remove(grounds_filename),
      add = TRUE
    )
    writeLines(geojsonsf::sf_geojson(grounds_export), grounds_filename)
    upload_cloud_file(
      file = grounds_filename,
      provider = conf$storage$google$key,
      options = coasts_opts
    )
    logger::log_info("Uploaded: {basename(grounds_filename)}")
  } else {
    logger::log_warn(
      "No fishing grounds passed min_trips = {min_trips_grounds}",
      " -- skipping GeoJSON upload."
    )
  }

  logger::log_info("=== Web spatial export complete ===")
  invisible(NULL)
}
