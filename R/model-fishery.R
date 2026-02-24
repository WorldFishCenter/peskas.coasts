#' Prepare Boat Registry Data from Metadata
#'
#' @description
#' Processes boat registry data from metadata table to create a summary
#' of total boats by district. This data is used for scaling GPS sample data
#' to fleet-wide estimates.
#'
#' @details
#' The function takes boat metadata and creates a district-level summary of boat counts.
#' This is essential for calculating sampling rates and extrapolating fleet-wide activity
#' from GPS-tracked samples.
#'
#' @param boats_table Data frame containing boat registry information with:
#'   - District: District name for each boat
#'   - Additional boat metadata (boat ID, registration details, etc.)
#'
#' @return A data frame with boat counts by district:
#'   - district: District name (standardized from metadata)
#'   - total_boats: Total number of boats registered in the district
#'
#' @examples
#' \dontrun{
#' # Get boat registry data from metadata
#' metadata <- get_metadata()
#' boat_registry <- prepare_boat_registry(boats_table = metadata$boats)
#'
#' # View boat counts by district
#' print(boat_registry)
#' }
#'
#' @seealso
#' * [estimate_fleet_activity()] for using boat registry in fleet estimates
#'
#' @keywords workflow preprocessing
#' @export
prepare_boat_registry <- function(boats_table = NULL) {
  boat_registry <- boats_table |>
    dplyr::group_by(.data$District) |>
    dplyr::summarise(
      n_boats = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::rename(
      district = "District",
      total_boats = "n_boats"
    )

  return(boat_registry)
}

#' Process Trip Data with District Information
#'
#' @description
#' Retrieves and processes trip data from the PDS API by adding district information
#' from community metadata. Filters out unrealistic trips longer than 48 hours to
#' remove outliers and prepares data for monthly statistics calculation.
#'
#' @details
#' The function:
#' - Calls `get_trips()` API to retrieve trip data for specified IMEIs
#' - Converts timestamps to landing dates and monthly periods
#' - Calculates trip duration in hours from seconds
#' - Joins with community metadata to add district information
#' - Filters out unrealistic trips (>48 hours)
#' - Standardizes column names and structure
#'
#' @param conf Configuration parameters containing:
#'   - pds$token: API token for PDS access
#'   - pds$secret: API secret for PDS access
#' @param devices_table Data frame containing device metadata with at minimum
#'   an `imei` column. Used to join district information to trips.
#' @param imei_list Vector of IMEI numbers for devices to retrieve trip data for
#'
#' @return A processed data frame with:
#'   - district: District name (from community metadata)
#'   - community: Community name
#'   - trip: Trip identifier
#'   - boat: Boat identifier
#'   - landing_date: Date of trip landing
#'   - date_month: First day of landing month
#'   - duration_hrs: Trip duration in hours
#'
#' @examples
#' \dontrun{
#' # Process trip data with parameters and device list
#' conf <- read_config()
#' trips_stats <- process_trip_data(
#'   conf = conf,
#'   imei_list = imei_list
#' )
#'
#' # Check data structure
#' str(trips_stats)
#' }
#'
#' @seealso
#' * [calculate_monthly_trip_stats()] for using processed trip data
#' * [get_trips()] for the underlying API call
#' * [get_metadata()] for community metadata retrieval
#'
#' @keywords workflow preprocessing
#' @export
process_trip_data <- function(
  conf = NULL,
  devices_table = NULL,
  imei_list = NULL
) {
  # Get metadata within the function
  boats_trips <-
    get_trips(
      token = conf$pds$token,
      secret = conf$pds$secret,
      dateFrom = "2023-01-01",
      dateTo = Sys.Date(),
      imeis = imei_list
    )

  trips_stats <-
    boats_trips |>
    dplyr::mutate(
      landing_date = lubridate::floor_date(.data$Ended, "day"),
      date_month = lubridate::floor_date(.data$landing_date, "month"),
      duration_hrs = .data$`Duration (Seconds)` / 3600
    ) |>
    dplyr::select(
      community = "Community",
      trip = "Trip",
      boat = "Boat",
      .data$landing_date,
      .data$date_month,
      .data$duration_hrs
    ) |>
    # Exclude trips longer than 2 days
    dplyr::filter(.data$duration_hrs <= 48) |>
    dplyr::left_join(
      devices_table |>
        dplyr::select("community", "gaul_2_name") |>
        dplyr::distinct(),
      by = "community"
    ) |>
    dplyr::select(
      "gaul_2_name",
      "community",
      "trip",
      "boat",
      "landing_date",
      "date_month",
      "duration_hrs"
    )

  return(trips_stats)
}

#' Calculate Monthly Trip Statistics by District
#'
#' @description
#' Calculates monthly fishing activity statistics by district based on GPS-tracked boats.
#' These are estimates from a sample of the total fleet and can be used to extrapolate
#' fleet-wide fishing activity when combined with boat registry data.
#'
#' @details
#' The function processes trip data to generate monthly summaries including:
#' - Number of trips recorded from GPS-tracked boats
#' - Number of unique boats tracked per month
#' - Average trip duration in hours
#' - Average trips per boat per month (key metric for fleet scaling)
#'
#' These metrics are particularly useful for estimating total fishing effort when
#' GPS trackers are only installed on a subset of the fishing fleet.
#'
#' @param trips_data A data frame containing trip information with columns:
#'   - district: District name
#'   - date_month: Month as date (first day of month)
#'   - duration_hrs: Trip duration in hours
#'   - boat: Unique boat identifier
#'
#' @return A data frame with monthly statistics by district containing:
#'   - district: District name
#'   - date_month: Month as date
#'   - sample_total_trips: Total trips recorded from tracked boats
#'   - sample_boats_tracked: Number of unique boats tracked
#'   - avg_trip_duration_hrs: Average trip duration in hours
#'   - avg_trips_per_boat_per_month: Average trips per boat (for scaling to fleet)
#'
#' @examples
#' \dontrun{
#' # Calculate monthly statistics
#' monthly_stats <- calculate_monthly_trip_stats(trips_stats)
#'
#' # View results
#' print(monthly_stats)
#' }
#'
#' @seealso
#' * [estimate_fleet_activity()] for scaling to total fleet
#' * [calculate_district_totals()] for catch and revenue estimates
#'
#' @keywords workflow analysis
#' @export
calculate_monthly_trip_stats <- function(trips_data) {
  trips_data |>
    dplyr::group_by(.data$gaul_2_name, .data$date_month) |>
    dplyr::summarise(
      # Sample statistics from GPS-tracked boats
      sample_total_trips = dplyr::n(),
      sample_boats_tracked = dplyr::n_distinct(.data$boat),

      # Key metrics that can be applied to full fleet
      avg_trip_duration_hrs = mean(.data$duration_hrs, na.rm = TRUE),
      avg_trips_per_boat_per_month = dplyr::n() / dplyr::n_distinct(.data$boat),

      .groups = "drop"
    ) |>
    dplyr::arrange(.data$gaul_2_name, .data$date_month)
}

#' Estimate Fleet-Wide Activity from Sample Data
#'
#' @description
#' Estimates total fleet activity by scaling sample-based trip statistics using
#' boat registry data. Calculates sampling rates and confidence levels for the estimates.
#'
#' @details
#' This function combines monthly trip statistics from GPS-tracked boats with
#' boat registry data to estimate fleet-wide fishing activity. It calculates:
#' - Estimated total trips if all boats were tracked
#' - Sampling rate (tracked boats / total boats)
#' - Confidence levels based on sampling coverage
#'
#' Confidence levels are assigned as:
#' - High: ≥30% of fleet tracked
#' - Medium: 10-29% of fleet tracked
#' - Low: <10% of fleet tracked
#'
#' @param monthly_stats Data frame from `calculate_monthly_trip_stats()` containing:
#'   - district: District name
#'   - date_month: Month as date
#'   - sample_total_trips: Total trips from tracked boats
#'   - sample_boats_tracked: Number of tracked boats
#'   - avg_trip_duration_hrs: Average trip duration
#'   - avg_trips_per_boat_per_month: Average trips per boat
#' @param boat_registry Data frame with columns:
#'   - district: District name (must match monthly_stats)
#'   - total_boats: Total number of boats registered in district
#'
#' @return A data frame combining monthly statistics with fleet estimates:
#'   - district: District name
#'   - date_month: Month as date
#'   - sample_total_trips: Total trips from tracked boats
#'   - sample_boats_tracked: Number of tracked boats
#'   - avg_trip_duration_hrs: Average trip duration
#'   - avg_trips_per_boat_per_month: Average trips per boat
#'   - estimated_total_trips: Estimated trips for entire fleet
#'   - sampling_rate: Proportion of fleet tracked (0-1)
#'   - estimate_confidence: Confidence level ("High", "Medium", "Low")
#'
#' @examples
#' \dontrun{
#' # First calculate monthly stats
#' monthly_stats <- calculate_monthly_trip_stats(trips_data)
#'
#' # Then estimate fleet activity
#' fleet_estimates <- estimate_fleet_activity(monthly_stats, boat_registry)
#'
#' # Check confidence levels
#' fleet_estimates |>
#'   dplyr::count(estimate_confidence)
#' }
#'
#' @seealso
#' * [calculate_monthly_trip_stats()] for generating input monthly statistics
#' * [calculate_district_totals()] for combining with catch/revenue data
#'
#' @keywords workflow modeling
#' @export
estimate_fleet_activity <- function(monthly_stats, boat_registry) {
  monthly_stats |>
    dplyr::left_join(boat_registry, by = "gaul_2_name") |>
    dplyr::mutate(
      # Estimate total trips if all boats were tracked
      estimated_total_trips = .data$avg_trips_per_boat_per_month *
        .data$total_boats,

      # Calculate sampling rate
      sampling_rate = .data$sample_boats_tracked / .data$total_boats,

      # Confidence in estimates (higher sampling rate = more confidence)
      estimate_confidence = dplyr::case_when(
        .data$sampling_rate >= 0.3 ~ "High",
        .data$sampling_rate >= 0.1 ~ "Medium",
        TRUE ~ "Low"
      )
    )
}

#' Calculate District-Level Total Catch and Revenue
#'
#' @description
#' Combines fleet activity estimates with catch and revenue data to calculate
#' total catch and revenue by district and month. Uses fleet-wide trip estimates
#' to scale up from sample-based averages.
#'
#' @details
#' This function merges fleet activity data with monthly catch/revenue summaries
#' to estimate total district-level fishing production. The calculations are:
#' - Total catch = mean catch per trip × estimated total trips
#' - Total revenue = mean revenue per trip × estimated total trips
#'
#' Only districts with catch data are included in the results.
#'
#' @param fleet_estimates Data frame from `estimate_fleet_activity()` containing:
#'   - district: District name
#'   - date_month: Month as date
#'   - estimated_total_trips: Estimated trips for entire fleet
#'   - sampling_rate: Proportion of fleet tracked
#'   - Other fleet statistics
#' @param monthly_summaries Data frame with catch/revenue data containing:
#'   - district: District name (must match fleet_estimates)
#'   - date: Month as date (will be matched to date_month)
#'   - metric: Metric name (filtered for mean_catch_kg and mean_catch_price)
#'   - value: Metric value
#'
#' @return A data frame with district-level totals:
#'   - district: District name
#'   - date_month: Month as date
#'   - sample_total_trips: Trips from tracked boats
#'   - estimated_total_trips: Estimated trips for entire fleet
#'   - sampling_rate: Proportion of fleet tracked
#'   - mean_catch_kg: Average catch per trip
#'   - mean_catch_price: Average revenue per trip
#'   - estimated_total_catch_kg: Estimated total catch for district
#'   - estimated_total_revenue: Estimated total revenue for district
#'
#' @examples
#' \dontrun{
#' # Calculate the full pipeline
#' monthly_stats <- calculate_monthly_trip_stats(trips_data)
#' fleet_estimates <- estimate_fleet_activity(monthly_stats, boat_registry)
#' district_totals <- calculate_district_totals(fleet_estimates, monthly_summaries)
#'
#' # View total catch by district
#' district_totals |>
#'   dplyr::group_by(district) |>
#'   dplyr::summarise(total_annual_catch = sum(estimated_total_catch_kg, na.rm = TRUE))
#' }
#'
#' @seealso
#' * [calculate_monthly_trip_stats()] for trip statistics
#' * [estimate_fleet_activity()] for fleet-wide estimates
#'
#' @keywords workflow analysis
#' @export
calculate_district_totals <- function(fleet_estimates, monthly_summaries) {
  fleet_estimates |>
    # Join with catch/revenue data
    dplyr::full_join(
      monthly_summaries,
      by = c("gaul_2_name" = "gaul_2_name", "date_month" = "date")
    ) |>
    # Calculate total estimates based on fleet-wide projections
    dplyr::mutate(
      # Total catch estimates (mean catch per trip × estimated total trips)
      estimated_total_catch_kg = .data$mean_catch_kg *
        .data$estimated_total_trips,

      # Total revenue estimates (mean revenue per trip × estimated total trips)
      estimated_total_revenue = .data$mean_catch_price *
        .data$estimated_total_trips
    ) |>
    # Select relevant columns
    dplyr::select(
      "gaul_2_name",
      "date_month",
      # Fleet estimates
      "sample_total_trips",
      "estimated_total_trips",
      "sampling_rate",
      # Sample-based averages
      "mean_catch_kg",
      "mean_catch_price",
      # Fleet-wide totals
      "estimated_total_catch_kg",
      "estimated_total_revenue"
    ) |>
    # Filter out rows with missing catch data
    dplyr::filter(!is.na(.data$mean_catch_kg)) |>
    dplyr::arrange(.data$gaul_2_name, .data$date_month)
}

#' Generate Complete Fleet Activity Analysis Pipeline
#'
#' @description
#' Executes the complete fleet activity analysis pipeline from data retrieval through
#' final aggregation and cloud storage upload. This function orchestrates all steps
#' including metadata retrieval, trip processing, fleet estimation, and results storage.
#'
#' @details
#' This function executes the complete analysis pipeline:
#' 1. Reads configuration parameters and retrieves metadata
#' 2. Prepares boat registry from metadata
#' 3. Processes trip data from PDS API using device IMEIs
#' 4. Downloads monthly summaries from cloud storage
#' 5. Calculates monthly trip statistics from GPS data
#' 6. Estimates fleet-wide activity using boat registry
#' 7. Calculates district totals using catch/revenue data
#' 8. Generates annual summary statistics by district
#' 9. Saves aggregated results to RDS file and uploads to cloud storage
#'
#' The function creates a comprehensive analysis that scales GPS-tracked boat data
#' to estimate total fleet activity, catch, and revenue by district and time period.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG).
#'   Controls the verbosity of logging output during pipeline execution.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#' @return Invisibly returns NULL. The function's primary purpose is to:
#'   - Generate and save aggregated analysis results to RDS file
#'   - Upload results to cloud storage
#'   - Log progress and completion status
#'
#' @section Output Files:
#' The function creates an RDS file containing a list with three components:
#' - **fleet_estimates**: Monthly fleet activity estimates by district
#' - **district_totals**: Monthly district-level catch and revenue totals
#' - **annual_summary**: Annual summary statistics by district including:
#'   - months_with_data: Number of months with data
#'   - avg_sampling_rate: Average sampling rate across months
#'   - total_estimated_catch_kg: Total estimated annual catch
#'   - total_estimated_revenue: Total estimated annual revenue
#'   - avg_monthly_catch_kg: Average monthly catch
#'   - avg_monthly_revenue: Average monthly revenue
#'
#' @examples
#' \dontrun{
#' # Run complete fleet analysis pipeline
#' generate_fleet_analysis()
#'
#' # Run with different logging level
#' generate_fleet_analysis(log_threshold = logger::INFO)
#' }
#'
#' @seealso
#' * [prepare_boat_registry()] for boat registry preparation
#' * [process_trip_data()] for trip data processing
#' * [calculate_monthly_trip_stats()] for monthly statistics
#' * [estimate_fleet_activity()] for fleet estimates
#' * [calculate_district_totals()] for final totals
#'
#' @keywords workflow analysis pipeline
#' @export
generate_fleet_analysis <- function(
  log_threshold = logger::DEBUG,
  package = "coasts"
) {
  conf <- read_config(package = package)

  assets <- cloud_object_name(
    prefix = conf$metadata$airtable$assets,
    provider = conf$storage$google$key,
    version = "latest",
    extension = "rds",
    options = conf$storage$google$options_coasts
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts
    ) |>
    readr::read_rds() |>
    purrr::keep_at(c("devices", "geo"))

  devices <- assets$devices |>
    dplyr::filter(.data$customer_name %in% conf$pds$customers)

  regions <-
    assets$geo |>
    dplyr::filter(.data$gaul_2_code %in% c(devices$gaul_2_code))

  boat_registry <-
    regions |>
    dplyr::select("gaul_2_name", "total_boats")

  # Process raw trip data
  trips_stats <- process_trip_data(
    conf = conf,
    devices_table = devices,
    imei_list = unique(devices$imei)
  )

  monthly_summaries <-
    download_parquet_from_cloud(
      prefix = paste0(
        conf$surveys$summaries$file_prefix,
        "_monthly_summaries"
      ),
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Calculate monthly statistics
  monthly_stats <- calculate_monthly_trip_stats(trips_data = trips_stats)

  # Estimate fleet activity
  fleet_estimates <- estimate_fleet_activity(
    monthly_stats = monthly_stats,
    boat_registry = boat_registry
  )

  # Calculate district totals
  district_totals <- calculate_district_totals(
    fleet_estimates = fleet_estimates,
    monthly_summaries = monthly_summaries
  ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date_month = seq(
        min(.data$date_month),
        max(.data$date_month),
        by = "month"
      ),
      fill = list(
        sample_total_trips = NA,
        estimated_total_trips = NA,
        sampling_rate = NA,
        mean_catch_kg = NA,
        mean_catch_price = NA,
        estimated_total_catch_kg = NA,
        estimated_total_revenue = NA
      )
    )

  # Generate annual summary
  annual_summary <-
    district_totals |>
    dplyr::mutate(year = as.character(lubridate::year(.data$date_month))) |>
    dplyr::group_by(.data$year, .data$gaul_2_name) |>
    dplyr::summarise(
      months_with_data = dplyr::n(),
      avg_sampling_rate = mean(.data$sampling_rate, na.rm = TRUE),
      total_estimated_catch_kg = sum(
        .data$estimated_total_catch_kg,
        na.rm = TRUE
      ),
      total_estimated_revenue = sum(
        .data$estimated_total_catch_kg,
        na.rm = TRUE
      ),
      avg_monthly_catch_kg = mean(.data$estimated_total_catch_kg, na.rm = TRUE),
      avg_monthly_revenue = mean(.data$estimated_total_revenue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      total_estimated_catch_kg = dplyr::if_else(
        .data$total_estimated_catch_kg == 0,
        NA_real_,
        .data$total_estimated_catch_kg
      ),
      total_estimated_revenue = dplyr::if_else(
        .data$total_estimated_revenue == 0,
        NA_real_,
        .data$total_estimated_revenue
      )
    )

  # Prepare aggregated results
  aggregated <-
    list(
      fleet_estimates = fleet_estimates,
      district_totals = district_totals,
      annual_summary = annual_summary
    )

  # Save aggregated results
  aggregated_filename <-
    conf$surveys$aggregated$file_prefix |>
    add_version(extension = "rds")

  readr::write_rds(aggregated, aggregated_filename)

  # Upload to cloud storage
  logger::log_info("Uploading fleet analysis results to cloud storage...")
  upload_cloud_file(
    file = aggregated_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
