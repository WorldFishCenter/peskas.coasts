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
#'   - All monthly summaries: the same recipe over every survey form, ignoring
#'     `exclude_dashboard_ids`; consumed by the multi-country coasts portal
#'   - Taxa summaries: Catch, catch-weighted mean length and price by species,
#'     district, and month
#'   - District summaries: Submission counts, effort, and catch and revenue per
#'     trip by district
#'   - Gear summaries: Performance metrics by gear type
#'   - Grid summaries: Downloaded from cloud storage
#'   - Taxa traits: one row per landed taxon with its FishBase traits (from
#'     the `taxa_enriched` table [enrich_taxa()] writes to the hub bucket)
#'   - Length summaries: recorded catch per length class, species, gear,
#'     district and month. Where a country's API rows carry the mean length
#'     of the fish measured (Kenya's KEFS), the classes hold those averages
#'   - Gear taxa summaries: recorded catch and trips per gear, species,
#'     district and month
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
#'   to keep out of the **country dashboard** summaries (e.g. legacy sources or
#'   forms that should not surface there). Rows matching these ids are dropped
#'   from `monthly_summaries`, `taxa_summaries`, `districts_summaries`,
#'   `gear_summaries`, `length_summaries` and `gear_taxa_summaries`. They are **kept** in `all_monthly_summaries` and in
#'   `<country>_fishery_metrics`, which feed the multi-country coasts portal.
#'   Defaults to `NULL`, in which case the list is
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

  country_opts <- resolve_storage_opts(conf, "country")

  # The ASFIS species list is held per country, not in the hub: measured
  # 2026-08-13, one `asfis` object each in mozambique-dev, mozambique-prod,
  # kenya-dev and zanzibar-dev, and none in peskas-coasts or peskas-coasts-dev.
  # A country adopting this function has to seed its own bucket.
  asfis <- coasts::download_parquet_from_cloud(
    prefix = "asfis",
    provider = conf$storage$google$key,
    options = country_opts
  ) |>
    janitor::clean_names() |>
    dplyr::select("alpha3_code", "scientific_name", "english_name") |>
    dplyr::distinct()

  # Input: validated trip data (catch-level rows, one row per catch item per trip)
  validated <- coasts::download_parquet_from_cloud(
    prefix = file.path(
      conf$api$trips$validated$cloud_path,
      conf$api$trips$validated$file_prefix
    ),
    provider = conf$storage$google$key,
    options = resolve_storage_opts(conf, "api", error_if_missing = TRUE)
  ) |>
    dplyr::left_join(
      asfis,
      by = c("catch_taxon" = "alpha3_code", "scientific_name")
    )

  # The summaries label a taxon by its scientific name; the FishBase traits are
  # keyed by the ASFIS code, which the relabelling below drops.
  taxa_names <- validated |>
    dplyr::filter(!is.na(.data$catch_taxon), !is.na(.data$scientific_name)) |>
    dplyr::distinct(
      alpha3_code = .data$catch_taxon,
      catch_taxon = .data$scientific_name,
      .data$english_name
    )

  all_data <- validated |>
    dplyr::mutate(catch_taxon = .data$scientific_name) |>
    dplyr::select(
      -c("scientific_name", "english_name")
    )

  # A landing with no date belongs to no month, so it can take no part in any
  # summary below -- and a single undated row is enough to make `min(date)`
  # non-finite and abort every `tidyr::complete()` with
  # `'from' must be a finite number`. Timor's frozen v1 form carries one such
  # trip (2026-09-10); the other three countries have none.
  undated <- sum(is.na(all_data$landing_date))
  if (undated > 0) {
    logger::log_warn("Dropping {undated} row(s) with no landing_date")
    all_data <- dplyr::filter(all_data, !is.na(.data$landing_date))
  }

  # The exclusion is dashboard-only. `all_data` keeps every form and feeds the
  # multi-country coasts portal (`<country>_fishery_metrics` and, via
  # export_portal(), `<country>_monthly_summaries_map`); `dash_data` is the
  # filtered frame behind the country dashboard summaries. With no exclusions
  # the two frames are identical.
  dash_data <- all_data |>
    dplyr::filter(!.data$survey_id %in% exclude_dashboard_ids)

  f_metrics <- calculate_fishery_metrics(data = all_data)

  # Trip-level intermediate: collapse to one row per trip, add effort metrics.
  # The input has multiple rows per trip (one per catch item); trip-level columns
  # (tot_catch_kg, tot_catch_price, n_fishers, etc.) are identical within a trip,
  # so slice(1) is safe and explicit.
  trip_indicators <- function(data) {
    data |>
      dplyr::group_by(.data$trip_id) |>
      dplyr::slice(1) |>
      dplyr::ungroup() |>
      dplyr::mutate(
        price_kg = .data$tot_catch_price / .data$tot_catch_kg,
        cpue = .data$tot_catch_kg / .data$n_fishers / .data$trip_duration_hrs,
        rpue = .data$tot_catch_price /
          .data$n_fishers /
          .data$trip_duration_hrs,
        cpue_day = .data$tot_catch_kg / .data$n_fishers,
        rpue_day = .data$tot_catch_price / .data$n_fishers
      ) |>
      # A trip recorded with zero fishers makes catch-per-fisher undefined, not
      # infinite, and `Inf` survives `mean()` and `median()` into the portal as
      # a metric no axis can plot. Timor has 273 such trips (2026-09-10); the
      # other three countries have none, so their output is unchanged.
      dplyr::mutate(dplyr::across(
        c("price_kg", "cpue", "rpue", "cpue_day", "rpue_day"),
        ~ dplyr::if_else(is.finite(.x), .x, NA_real_)
      ))
  }

  indicators_df <- trip_indicators(dash_data)

  # One row per trip x taxon. The sums precede `across()`, whose results later
  # expressions would otherwise see. A trip-level price belongs to a taxon only
  # when the trip landed nothing else.
  taxa_df <-
    dash_data |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::mutate(single_taxon = dplyr::n_distinct(.data$catch_taxon) == 1) |>
    dplyr::group_by(.data$trip_id, .data$catch_taxon) |>
    dplyr::summarise(
      taxon_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      taxon_catch_price = stat_or_na(.data$catch_price, sum),
      length_cm = catch_weighted_length(.data$length_cm, .data$catch_kg),
      dplyr::across(
        -c("length_cm", "catch_kg", "catch_price"),
        ~ dplyr::first(.x)
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      taxon_catch_price = dplyr::coalesce(
        .data$taxon_catch_price,
        dplyr::if_else(.data$single_taxon, .data$tot_catch_price, NA_real_)
      )
    ) |>
    dplyr::relocate("n_catch", .after = "trip_id")

  # --- Summaries (all stored wide until export_wf_data pivots for MongoDB) ---

  # One recipe, two frames: `monthly_summaries` (dashboard, filtered) and
  # `all_monthly_summaries` (coasts portal, unfiltered). Sharing the function
  # keeps their schema identical, which export_geos() depends on.
  monthly_from <- function(indicators) {
    indicators |>
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
  }

  monthly_summaries <- monthly_from(indicators_df)

  # Unfiltered twin, consumed by export_portal() for the coasts portal only.
  # Named `all_monthly_summaries`, NOT `monthly_summaries_all`:
  # cloud_object_name() matches by string prefix and then takes max(updated),
  # so a `<prefix>_monthly_summaries` read (see model_fishery_metrics()) would
  # also match a `_monthly_summaries_all` object and could return the wrong file.
  all_monthly_summaries <- monthly_from(trip_indicators(all_data))

  # Taxa summaries: total catch, mean length (weighted by catch) and price per
  # species x district x month. The price covers only the catch that carries a
  # value of its own (see taxa_df). Stored in long format (metric/value) for
  # portal consumption.
  taxa_summaries <-
    taxa_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$taxon_catch_kg, na.rm = TRUE),
      catch_price = sum(.data$taxon_catch_price, na.rm = TRUE),
      priced_kg = sum(
        .data$taxon_catch_kg[!is.na(.data$taxon_catch_price)],
        na.rm = TRUE
      ),
      mean_length = catch_weighted_length(.data$length_cm, .data$taxon_catch_kg),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$catch_taxon
    ) |>
    dplyr::mutate(
      price_kg = dplyr::if_else(
        .data$priced_kg > 0,
        .data$catch_price / .data$priced_kg,
        NA_real_
      )
    ) |>
    dplyr::select(-c("catch_price", "priced_kg")) |>
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
      mean_catch_kg = mean(.data$tot_catch_kg, na.rm = TRUE),
      mean_catch_price = mean(.data$tot_catch_price, na.rm = TRUE),
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

  # Length summaries: recorded catch per length class, species, gear, district
  # and month, the input of the length-frequency and size-at-maturity views.
  # `n_trips` counts the trips behind each species x gear x district x month,
  # repeated on each of its length classes.
  length_summaries <-
    dash_data |>
    dplyr::filter(!is.na(.data$length_cm), .data$catch_kg > 0) |>
    dplyr::mutate(
      date = lubridate::as_datetime(
        lubridate::floor_date(.data$landing_date, "month")
      )
    ) |>
    dplyr::group_by(
      .data$gaul_2_name,
      .data$date,
      .data$catch_taxon,
      .data$gear
    ) |>
    dplyr::mutate(n_trips = dplyr::n_distinct(.data$trip_id)) |>
    dplyr::ungroup() |>
    dplyr::mutate(length_class(.data$length_cm)) |>
    dplyr::group_by(
      .data$gaul_2_name,
      .data$date,
      .data$catch_taxon,
      .data$gear,
      .data$length_min,
      .data$length_max,
      .data$n_trips
    ) |>
    dplyr::summarise(catch_kg = sum(.data$catch_kg), .groups = "drop")

  # Gear x taxon summaries: recorded catch and trips per gear, taxon, district
  # and month, for what each gear catches.
  gear_taxa_summaries <-
    taxa_df |>
    dplyr::mutate(
      date = lubridate::as_datetime(
        lubridate::floor_date(.data$landing_date, "month")
      )
    ) |>
    dplyr::group_by(.data$gaul_2_name, .data$date, .data$gear, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$taxon_catch_kg, na.rm = TRUE),
      n_trips = dplyr::n_distinct(.data$trip_id),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$catch_kg > 0)

  # Taxa traits: one row per taxon the country has landed, with the FishBase
  # traits coasts::enrich_taxa() gathers for the whole region. The default
  # prefix keeps a pipeline whose config predates the key running.
  enriched <- tryCatch(
    download_parquet_from_cloud(
      prefix = conf$metadata$fishbase$taxa_enriched$file_prefix %||%
        "taxa-fishbase-enriched",
      provider = conf$storage$google$key,
      options = resolve_storage_opts(conf, "coasts")
    ),
    error = function(e) {
      # The dev hub bucket may not hold the table yet; names alone still let
      # the dashboard list the species.
      logger::log_warn(
        "No FishBase traits, taxa_traits carries names only: {conditionMessage(e)}"
      )
      NULL
    }
  )
  taxa_traits <- if (is.null(enriched)) {
    taxa_names
  } else {
    dplyr::left_join(
      taxa_names,
      summarise_taxa_traits(enriched),
      by = "alpha3_code"
    )
  }

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

  # Grid summaries: pre-computed spatial grid from PDS tracks, passed through
  # as-is. The country bucket is where preprocess_pds_tracks() writes them, so
  # a country with no grid summaries here has not run that step, rather than
  # having them somewhere else.
  grid_summaries <-
    download_parquet_from_cloud(
      prefix = paste0(conf$pds$pds_tracks$file_prefix, "-grid_summaries"),
      provider = conf$storage$google$key,
      options = country_opts
    )

  # Upload all summaries to cloud storage (versioned parquet files)
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    all_monthly_summaries = all_monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries,
    grid_summaries = grid_summaries,
    taxa_traits = taxa_traits,
    length_summaries = length_summaries,
    gear_taxa_summaries = gear_taxa_summaries
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
      options = country_opts
    )
  }

  upload_parquet_to_cloud(
    data = f_metrics,
    prefix = paste0(conf$country, "_fishery_metrics"),
    provider = conf$storage$google$key,
    options = resolve_storage_opts(conf, "coasts")
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

#' A summary statistic `f` of a vector, or NA when every value is missing
#' @noRd
stat_or_na <- function(x, f) {
  if (all(is.na(x))) NA_real_ else f(x, na.rm = TRUE)
}

#' Mean length weighted by catch weight
#'
#' Rows are length classes (or measured fish), so an unweighted mean would give
#' a class holding 1 kg as much say as one holding 100 kg. Falls back to the
#' plain mean when no row has a usable weight, and gives NA with no length.
#' @noRd
catch_weighted_length <- function(length_cm, catch_kg) {
  ok <- !is.na(length_cm) & !is.na(catch_kg) & catch_kg > 0
  if (any(ok)) {
    stats::weighted.mean(length_cm[ok], catch_kg[ok])
  } else if (any(!is.na(length_cm))) {
    mean(length_cm, na.rm = TRUE)
  } else {
    NA_real_
  }
}

#' Length classes shared by the survey forms
#'
#' Countries store a length class as its midpoint (Kenya: the mean of the fish
#' measured), so a fixed set of bins places every form's value in the class it
#' came from. Classes follow the landing forms: 5 cm to 30 cm, 10 cm to 100 cm,
#' then wider bins for the few large fish measured one by one.
#'
#' @param length_cm Numeric vector of lengths.
#' @return A data frame with `length_min` and `length_max` (NA for the open
#'   top class).
#' @noRd
length_class <- function(length_cm) {
  breaks <- c(0, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 125, 150, 200, Inf)
  i <- findInterval(length_cm, breaks)
  upper <- breaks[i + 1]
  data.frame(
    length_min = breaks[i],
    length_max = dplyr::if_else(is.finite(upper), upper, NA_real_)
  )
}

#' Summarise FishBase traits per taxon code
#'
#' `taxa_enriched` holds one row per species a taxon code expands to, so a
#' family-level code carries many species with different traits. This keeps
#' the median and range of vulnerability, counts the threatened (IUCN VU, EN,
#' CR) and CITES-listed species, and reports species-level facts (IUCN
#' category, lengths at maturity and optimum) only when the code resolves to
#' one species.
#'
#' @param enriched The `taxa_enriched` table written by [enrich_taxa()].
#' @return One row per `alpha3_code`.
#' @noRd
summarise_taxa_traits <- function(enriched) {
  # Tables written before coasts 4.15.0 lack these columns.
  # ponytail: drop once every hub bucket has a 4.15.0 taxa_enriched.
  for (col in c("class", "iucn_code", "cites_code", "length_maturity_tl", "length_optimum_tl")) {
    if (!col %in% names(enriched)) enriched[[col]] <- NA
  }
  mode_of <- function(x) {
    x <- x[!is.na(x)]
    if (length(x)) names(sort(table(x), decreasing = TRUE))[1] else NA_character_
  }
  one_species <- function(x) if (length(x) == 1) x else x[NA_integer_]

  enriched |>
    dplyr::distinct(
      .data$alpha3_code,
      .data$species_found,
      .keep_all = TRUE
    ) |>
    dplyr::group_by(.data$alpha3_code) |>
    dplyr::summarise(
      n_species = dplyr::n(),
      vulnerability = stat_or_na(.data$vulnerability_fishing, stats::median),
      vulnerability_min = stat_or_na(.data$vulnerability_fishing, min),
      vulnerability_max = stat_or_na(.data$vulnerability_fishing, max),
      trophic_level = stat_or_na(
        dplyr::coalesce(.data$food_troph, .data$diet_troph),
        stats::median
      ),
      n_threatened = sum(.data$iucn_code %in% c("VU", "EN", "CR")),
      n_cites = sum(!is.na(.data$cites_code)),
      iucn_code = one_species(.data$iucn_code),
      length_maturity_cm = one_species(.data$length_maturity_tl),
      # An optimum below maturity means FishBase's growth and maturity studies
      # disagree (124 of 315 WIO species, 2026-09); keep only a coherent pair.
      length_optimum_cm = one_species(dplyr::if_else(
        .data$length_optimum_tl >= .data$length_maturity_tl,
        .data$length_optimum_tl,
        NA_real_
      )),
      class = mode_of(.data$class),
      .groups = "drop"
    )
}
