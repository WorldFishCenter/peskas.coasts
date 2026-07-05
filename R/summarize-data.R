#' Summarize WorldFish Survey Data
#'
#' @description
#' Processes validated survey data from WorldFish sources, filtering out flagged submissions
#' and generating summary datasets for various dimensions:
#' - Monthly summaries with aggregated catch metrics
#' - Taxa summaries with species-specific information
#' - District summaries with submission and effort metrics
#' - Gear summaries with gear-specific performance metrics
#' - Grid summaries from vessel tracking data
#'
#' @details
#' The function performs the following operations:
#' - Retrieves validated WF survey data
#' - Filters for approved validation status
#' - Creates multiple summary datasets:
#'   - Monthly summaries: Average catch, price, CPUE, and RPUE by district and month
#'   - Taxa summaries: Catch metrics by species, district, and month
#'   - District summaries: Submission counts and effort metrics by district
#'   - Gear summaries: Performance metrics by gear type
#'   - Grid summaries: Downloaded from cloud storage
#' - Uploads all summaries to cloud storage as versioned parquet files
#'
#' The metrics calculated include:
#' - Total and mean catch weight
#' - Price per kg of catch
#' - CPUE (Catch Per Unit Effort) - both hourly and daily
#' - RPUE (Revenue Per Unit Effort) - both hourly and daily
#' - Number of submissions and fishers
#' - Trip duration
#'
#' @param exclude_dashboard_ids Optional character vector of `survey_id` values
#'   to keep out of the dashboard summaries (e.g. legacy sources or forms that
#'   should not surface in the portal). Rows matching these ids are dropped from
#'   the input before summarising. Defaults to `NULL`, in which case the list is
#'   read from the package config at `surveys$summaries$exclude_dashboard_ids`
#'   (each downstream package manages its own list). If neither is set, all
#'   surveys are kept.
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return NULL (invisible). The function uploads summary files to cloud storage as a side effect.
#'
#' @examples
#' \dontrun{
#' # Summarize WF data with default debug logging
#' summarize_data()
#'
#' # Summarize with info-level logging only
#' summarize_data(logger::INFO)
#'
#' # Keep specific forms out of the dashboard explicitly (overrides config list)
#' summarize_data(exclude_dashboard_ids = c(
#'   conf$ingestion$wcs$koboform$asset_id,
#'   conf$ingestion$wcs$koboform_kf$asset_id_kf,
#'   "legacy"
#' ))
#' }
#'
#' @seealso
#' * [get_validated_surveys()] for details on the input data format
#' * [get_validation_status()] for retrieving validation information
#' * [upload_cloud_file()] for uploading results to cloud storage
#' * [download_parquet_from_cloud()] for retrieving grid summaries
#'
#' @keywords workflow pipeline mining
#' @export
summarize_data <- function(
  exclude_dashboard_ids = NULL,
  log_threshold = logger::DEBUG,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  # survey_id values to keep out of the dashboard summaries (e.g. legacy sources
  # or forms that should not surface in the portal). When not passed explicitly
  # (e.g. from a GitHub pipeline calling `summarize_data(package = ...)`), fall
  # back to the package's own config so each downstream package manages its own
  # list under `surveys$summaries$exclude_dashboard_ids`. unlist() tolerates both
  # a YAML sequence (character vector) and a nested list, and leaves NULL as NULL.
  if (is.null(exclude_dashboard_ids)) {
    exclude_dashboard_ids <- conf$surveys$summaries$exclude_dashboard_ids
  }
  exclude_dashboard_ids <- unlist(exclude_dashboard_ids, use.names = FALSE)
  if (length(exclude_dashboard_ids)) {
    logger::log_info(
      "Excluding {length(exclude_dashboard_ids)} survey form(s) from dashboard summaries"
    )
  }

  asfis <- coasts::download_parquet_from_cloud(
    prefix = "asfis",
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    janitor::clean_names() |>
    dplyr::select("alpha3_code", "scientific_name", "english_name") |>
    dplyr::distinct()

  # Input: validated trip data (catch-level rows, one row per catch item per trip)
  clean_data <- coasts::download_parquet_from_cloud(
    prefix = file.path(
      conf$api$trips$validated$cloud_path,
      conf$api$trips$validated$file_prefix
    ),
    provider = conf$storage$google$key,
    options = conf$storage$google$options_api
  ) |>
    dplyr::left_join(
      asfis,
      by = c("catch_taxon" = "alpha3_code", "scientific_name")
    ) |>
    dplyr::mutate(catch_taxon = .data$scientific_name) |>
    dplyr::select(
      -c("scientific_name", "english_name")
    ) |>
    # Drop forms excluded from the dashboard; with NULL this keeps all rows
    dplyr::filter(!.data$survey_id %in% exclude_dashboard_ids)

  f_metrics <- calculate_fishery_metrics(data = clean_data)

  # Trip-level intermediate: collapse to one row per trip, add effort metrics.
  # clean_data has multiple rows per trip (one per catch item); trip-level columns
  # (tot_catch_kg, tot_catch_price, n_fishers, etc.) are identical within a trip,
  # so slice(1) is safe and explicit.
  indicators_df <-
    clean_data |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      price_kg = .data$tot_catch_price / .data$tot_catch_kg,
      cpue = .data$tot_catch_kg / .data$n_fishers / .data$trip_duration_hrs,
      rpue = .data$tot_catch_price / .data$n_fishers / .data$trip_duration_hrs,
      cpue_day = .data$tot_catch_kg / .data$n_fishers,
      rpue_day = .data$tot_catch_price / .data$n_fishers
    )

  # Taxon-level intermediate: collapse to one row per trip x taxon,
  # computing per-taxon catch weight and mean length.
  taxa_df <-
    clean_data |>
    dplyr::group_by(.data$trip_id, .data$catch_taxon) |>
    dplyr::summarise(
      dplyr::across(dplyr::everything(), ~ dplyr::first(.x)),
      length_cm = mean(.data$length_cm, na.rm = TRUE),
      taxon_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::relocate("n_catch", .after = "trip_id")

  # --- Summaries (all stored wide until export_wf_data pivots for MongoDB) ---

  monthly_summaries <-
    indicators_df |>
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, "month"),
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::group_by(.data$gaul_2_name, .data$date) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "tot_catch_kg",
          "tot_catch_price",
          "cpue",
          "cpue_day",
          "rpue",
          "rpue_day",
          "price_kg"
        ),
        ~ mean(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    ) |>
    dplyr::rename(
      mean_catch_kg = "tot_catch_kg",
      mean_catch_price = "tot_catch_price",
      mean_cpue = "cpue",
      mean_cpue_day = "cpue_day",
      mean_rpue = "rpue",
      mean_rpue_day = "rpue_day",
      mean_price_kg = "price_kg"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        mean_catch_kg = NA,
        mean_catch_price = NA,
        mean_cpue = NA,
        mean_cpue_day = NA,
        mean_rpue = NA,
        mean_rpue_day = NA,
        mean_price_kg = NA
      )
    )

  # Taxa summaries: total catch and mean length per species x district x month.
  # Stored in long format (metric/value) for portal consumption.
  taxa_summaries <-
    taxa_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$taxon_catch_kg, na.rm = TRUE),
      catch_price = sum(.data$tot_catch_price, na.rm = TRUE),
      mean_length = mean(.data$length_cm, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$catch_taxon
    ) |>
    dplyr::mutate(price_kg = .data$catch_price / .data$catch_kg) |>
    dplyr::select(-c("catch_price")) |>
    tidyr::pivot_longer(
      -c("gaul_2_name", "date", "catch_taxon"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::distinct()

  # Districts summaries: submission counts and effort metrics per district x month.
  # Stored wide; export_wf_data() joins modeled estimates then pivots to long.
  districts_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration_hrs = mean(.data$trip_duration_hrs),
      mean_cpue = mean(.data$cpue, na.rm = TRUE),
      mean_rpue = mean(.data$rpue, na.rm = TRUE),
      mean_price_kg = mean(.data$price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month")
    ) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date))

  # Gear summaries: CPUE/RPUE by gear type x district x month, in long format.
  gear_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date, .data$gear) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      cpue = mean(.data$cpue, na.rm = TRUE),
      rpue = mean(.data$rpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$gear
    ) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date)) |>
    tidyr::pivot_longer(
      -c("gaul_2_name", "date", "gear"),
      names_to = "indicator",
      values_to = "value"
    )

  # Grid summaries: pre-computed spatial grid from PDS tracks, passed through as-is.
  grid_summaries <-
    download_parquet_from_cloud(
      prefix = paste0(conf$pds$pds_tracks$file_prefix, "-grid_summaries"),
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Upload all summaries to cloud storage (versioned parquet files)
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries,
    grid_summaries = grid_summaries
  )

  # Write each data frame to its own parquet file with versioning and upload
  for (name in names(dataframes_to_upload)) {
    filename <- conf$surveys$summaries$file_prefix %>%
      paste0("_", name) %>% # Add the table name to distinguish files
      add_version(extension = "parquet")

    arrow::write_parquet(
      x = dataframes_to_upload[[name]],
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

  upload_parquet_to_cloud(
    data = f_metrics,
    prefix = paste0(conf$country, "_fishery_metrics"),
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts
  )
}

#' Calculate Fishery Metrics
#'
#' Transforms catch-level data into normalized fishery performance indicators.
#' Calculates site-level, gear-specific, and species-specific metrics.
#'
#' @param data A data frame with catch records containing required columns:
#'   submission_id, landing_date, district, gear, catch_outcome, no_men_fishers,
#'   no_women_fishers, no_child_fishers, catch_taxon, catch_price, catch_kg
#'
#' @return A data frame in normalized long format with columns: landing_site,
#'   year_month, metric_type, metric_value, gear_type, species, rank
#'
#' @keywords preprocessing
#' @export
calculate_fishery_metrics <- function(data = NULL) {
  catch_data <- data |>
    #dplyr::filter(.data$catch_outcome == "1") |>
    dplyr::select(
      "trip_id",
      "landing_date",
      "gaul_2_name",
      "gear",
      "trip_duration_hrs",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg",
      "tot_catch_kg",
      "tot_catch_price"
    ) |>
    dplyr::mutate(
      year_month = lubridate::floor_date(.data$landing_date, "month")
    )

  trip_level_data <- catch_data |>
    dplyr::select(
      "trip_id",
      "landing_date",
      "gaul_2_name",
      "gear",
      "n_fishers",
      "year_month",
      trip_total_catch_kg = "tot_catch_kg",
      trip_total_revenue = "tot_catch_price"
    ) |>
    dplyr::distinct()

  site_level_metrics <- trip_level_data |>
    dplyr::group_by(.data$gaul_2_name, .data$year_month) |>
    dplyr::summarise(
      avg_fishers_per_trip = mean(.data$n_fishers, na.rm = TRUE),
      avg_catch_per_trip = mean(.data$trip_total_catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      cols = c(.data$avg_fishers_per_trip, .data$avg_catch_per_trip),
      names_to = "metric_type",
      values_to = "metric_value"
    ) |>
    dplyr::mutate(
      gear_type = NA_character_,
      catch_taxon = NA_character_,
      rank = NA_integer_
    )

  gear_metrics <- trip_level_data |>
    dplyr::group_by(.data$gaul_2_name, .data$year_month) |>
    dplyr::mutate(total_trips = dplyr::n()) |>
    dplyr::add_count(.data$gear, name = "gear_count") |>
    dplyr::slice_max(.data$gear_count, n = 1, with_ties = FALSE) |>
    dplyr::distinct(.data$gaul_2_name, .data$year_month, .keep_all = TRUE) |>
    dplyr::mutate(
      pct_main_gear = (.data$gear_count / .data$total_trips) * 100
    ) |>
    dplyr::select(
      .data$gaul_2_name,
      .data$year_month,
      .data$gear,
      .data$pct_main_gear
    ) |>
    dplyr::ungroup()

  predominant_gear_metrics <- gear_metrics |>
    dplyr::transmute(
      gaul_2_name = .data$gaul_2_name,
      year_month = .data$year_month,
      metric_type = "predominant_gear",
      metric_value = NA_real_,
      gear_type = .data$gear,
      catch_taxon = NA_character_,
      rank = NA_integer_
    )

  pct_main_gear_metrics <- gear_metrics |>
    dplyr::transmute(
      gaul_2_name = .data$gaul_2_name,
      year_month = .data$year_month,
      metric_type = "pct_main_gear",
      metric_value = .data$pct_main_gear,
      gear_type = NA_character_,
      catch_taxon = NA_character_,
      rank = NA_integer_
    )

  cpue_metrics <- trip_level_data |>
    dplyr::mutate(cpue = .data$trip_total_catch_kg / .data$n_fishers) |>
    dplyr::group_by(.data$gaul_2_name, .data$year_month, .data$gear) |>
    dplyr::summarise(
      avg_cpue = mean(.data$cpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::transmute(
      gaul_2_name = .data$gaul_2_name,
      year_month = .data$year_month,
      metric_type = "cpue",
      metric_value = .data$avg_cpue,
      gear_type = .data$gear,
      catch_taxon = NA_character_,
      rank = NA_integer_
    )

  rpue_metrics <- trip_level_data |>
    dplyr::mutate(rpue = .data$trip_total_revenue / .data$n_fishers) |>
    dplyr::group_by(.data$gaul_2_name, .data$year_month, .data$gear) |>
    dplyr::summarise(
      avg_rpue = mean(.data$rpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::transmute(
      gaul_2_name = .data$gaul_2_name,
      year_month = .data$year_month,
      metric_type = "rpue",
      metric_value = .data$avg_rpue,
      gear_type = .data$gear,
      catch_taxon = NA_character_,
      rank = NA_integer_
    )

  species_metrics <- catch_data |>
    dplyr::group_by(.data$gaul_2_name, .data$year_month, .data$catch_taxon) |>
    dplyr::summarise(
      total_species_catch = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$gaul_2_name, .data$year_month) |>
    dplyr::mutate(
      total_site_catch = sum(.data$total_species_catch),
      species_pct = (.data$total_species_catch / .data$total_site_catch) * 100
    ) |>
    dplyr::arrange(
      .data$gaul_2_name,
      .data$year_month,
      dplyr::desc(.data$species_pct)
    ) |>
    dplyr::mutate(rank = dplyr::row_number()) |>
    dplyr::filter(.data$rank <= 2) |>
    dplyr::transmute(
      gaul_2_name = .data$gaul_2_name,
      year_month = .data$year_month,
      metric_type = "species_pct",
      metric_value = .data$species_pct,
      gear_type = NA_character_,
      catch_taxon = .data$catch_taxon,
      rank = .data$rank
    ) |>
    dplyr::ungroup()

  fishery_metrics <- dplyr::bind_rows(
    site_level_metrics,
    predominant_gear_metrics,
    pct_main_gear_metrics,
    cpue_metrics,
    rpue_metrics,
    species_metrics
  ) |>
    dplyr::arrange(.data$gaul_2_name, .data$year_month, .data$metric_type)

  return(fishery_metrics)
}
