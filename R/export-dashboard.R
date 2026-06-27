# Dashboard data export pipeline.
#
# Computes the seven S/A-tier indicator datasets used by the Peskas
# analytical dashboard and the JS portal. Each dataset covers all three
# countries (Kenya, Mozambique, Zanzibar) and is uploaded as a versioned
# parquet to GCS.
#
# Taxa traits (vulnerability, trophic level) are NOT re-queried from
# FishBase here -- they are consumed from the enriched taxa parquet that
# enrich_taxa() (fishbase.R) already produces and uploads. Run enrich_taxa()
# before this function if the enriched snapshot is stale.
#
# Datasets produced:
#   dashboard_weekly    — weekly catch + trip counts (time series)
#   dashboard_top_taxa  — top-10 taxa by biomass
#   dashboard_rarity    — frequency × biomass quadrant + vulnerability
#   dashboard_seasonal  — monthly catch by taxon (top 12)
#   dashboard_cpue      — monthly median CPUE with IQR
#   dashboard_mtl       — mean trophic level by gear, region, month
#   dashboard_vuln      — dominant taxa flagged for high vulnerability (>75)
#
# ── Internal helpers ──────────────────────────────────────────────────────────

#' Preprocess raw validated landings into the dashboard schema
#'
#' @param landings Raw parquet tibble from the API bucket.
#' @param country  Character scalar — country label to attach.
#' @return Tibble with typed and cleaned columns.
#' @keywords internal
preprocess_landings <- function(landings, country) {
  landings |>
    dplyr::filter(
      !is.na(.data$gaul_2_name),
      .data$landing_date >= as.Date("2024-01-01")
    ) |>
    dplyr::mutate(
      country = country,
      landing_date = as.Date(.data$landing_date),
      year = lubridate::year(.data$landing_date),
      month_num = lubridate::month(.data$landing_date),
      month = lubridate::month(.data$landing_date, label = TRUE, abbr = TRUE),
      week_start = lubridate::floor_date(.data$landing_date, "week"),
      province = stringr::str_to_title(stringr::str_trim(.data$gaul_1_name)),
      district = stringr::str_to_title(stringr::str_trim(.data$gaul_2_name)),
      gear = stringr::str_to_title(stringr::str_trim(.data$gear)),
      catch_taxon = stringr::str_to_title(stringr::str_trim(.data$catch_taxon)),
      catch_kg = as.numeric(.data$catch_kg),
      tot_catch_kg = as.numeric(.data$tot_catch_kg),
      trip_duration = as.numeric(.data$trip_duration_hrs),
      n_fishers = as.numeric(.data$n_fishers),
      cpue_kg_hr = dplyr::if_else(
        .data$trip_duration > 0,
        .data$catch_kg / .data$trip_duration,
        NA_real_
      )
    ) |>
    dplyr::filter(
      !is.na(.data$catch_taxon),
      .data$catch_taxon != "",
      !is.na(.data$catch_kg),
      .data$catch_kg > 0
    )
}

#' Collapse preprocessed landings to one row per trip
#' @keywords internal
trip_level <- function(df) {
  df |>
    dplyr::distinct(.data$trip_id, .keep_all = TRUE) |>
    dplyr::select(
      "country",
      "trip_id",
      "landing_date",
      "year",
      "month_num",
      "month",
      "week_start",
      "province",
      "district",
      "gear",
      "n_fishers",
      "trip_duration",
      "tot_catch_kg",
      "cpue_kg_hr"
    )
}

#' Download the enriched taxa snapshot and collapse to one row per code
#'
#' Consumes the parquet produced by [enrich_taxa()] (vulnerability and trophic
#' data keyed by `alpha3_code`). Because a single alpha3 code can expand to
#' several FishBase species (family-level codes), numeric traits are averaged
#' and categorical traits take the first non-`NA` value per code.
#'
#' @param conf Config list from [read_config()].
#' @return Tibble with one row per `catch_taxon` (title-cased alpha3 code) and
#'   columns `vulnerability_fishing`, `vulnerability_climate`, `food_troph`,
#'   `feeding_type`.
#' @keywords internal
download_enriched_taxa <- function(conf) {
  logger::log_info("Downloading enriched taxa snapshot (enrich_taxa output) ...")
  download_parquet_from_cloud(
    prefix = conf$metadata$fishbase$taxa_enriched$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    dplyr::group_by(catch_taxon = stringr::str_to_title(.data$alpha3_code)) |>
    dplyr::summarise(
      vulnerability_fishing = mean(.data$vulnerability_fishing, na.rm = TRUE),
      vulnerability_climate = mean(.data$vulnerability_climate, na.rm = TRUE),
      food_troph = mean(.data$food_troph, na.rm = TRUE),
      feeding_type = dplyr::first(stats::na.omit(.data$feeding_type)),
      .groups = "drop"
    ) |>
    dplyr::mutate(dplyr::across(
      dplyr::where(is.numeric),
      ~ dplyr::if_else(is.nan(.x), NA_real_, .x)
    ))
}

# ── Main export function ──────────────────────────────────────────────────────

#' Export Pre-computed Dashboard Indicator Datasets to GCS
#'
#' Pulls validated landings for all three countries from the unified API
#' bucket (the same source [aggregate_fao()] uses), joins the enriched taxa
#' traits produced by [enrich_taxa()], computes the seven S/A-tier indicator
#' datasets used by the dashboard and JS portal, and uploads each as a
#' versioned parquet to GCS.
#'
#' @details
#' **Datasets uploaded** (one parquet each):
#' \describe{
#'   \item{dashboard_weekly}{Weekly catch (kg) and trip counts per country.}
#'   \item{dashboard_top_taxa}{Top-10 taxa by total biomass per country.}
#'   \item{dashboard_rarity}{Frequency × biomass quadrant with vulnerability.}
#'   \item{dashboard_seasonal}{Monthly catch by taxon (top 12) per country.}
#'   \item{dashboard_cpue}{Monthly median CPUE (kg/hr) with IQR per country.}
#'   \item{dashboard_mtl}{Mean trophic level by gear, region, and month.}
#'   \item{dashboard_vuln}{Dominant taxa flagged for high vulnerability (>75).}
#' }
#'
#' Taxa traits come from the enriched snapshot uploaded by [enrich_taxa()];
#' run that first if the snapshot is missing or stale.
#'
#' @param log_threshold Logging threshold. Default [logger::DEBUG].
#' @param package       Package whose `inst/conf.yml` to read. Default `"coasts"`.
#' @param top_n_taxa    Top taxa retained in the seasonal dataset. Default 12.
#'
#' @return Invisibly `NULL`. Uploads seven parquet files to GCS as a side effect.
#'
#' @examples
#' \dontrun{
#' enrich_taxa()      # refresh taxa traits first if needed
#' export_dashboard()
#' }
#'
#' @seealso [aggregate_fao()], [enrich_taxa()], [export_geos()]
#'
#' @keywords workflow export dashboard
#' @export
export_dashboard <- function(
    log_threshold = logger::DEBUG,
    package = "coasts",
    top_n_taxa = 12
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)
  coasts_opts <- resolve_storage_opts(conf, "coasts")

  # ── Step 1. Enriched taxa traits (single snapshot, all countries) ───────────
  taxa_traits <- download_enriched_taxa(conf)
  logger::log_info(
    "Taxa traits ready: {nrow(taxa_traits)} codes, ",
    "{sum(!is.na(taxa_traits$vulnerability_fishing))} with fishing vulnerability, ",
    "{sum(!is.na(taxa_traits$food_troph))} with trophic level."
  )

  # ── Step 2. Download and preprocess landings for all countries ──────────────
  # Same source and pattern as aggregate_fao(): unified API bucket, country
  # cloud_path as prefix. Schema is identical across countries.
  countries <- list(
    mozambique = conf$api$trips$mozambique$validated$cloud_path,
    kenya = conf$api$trips$kenya$validated$cloud_path,
    zanzibar = conf$api$trips$zanzibar$validated$cloud_path
  )

  logger::log_info("Downloading validated landings for all countries ...")
  all_landings <- purrr::imap(countries, function(.prefix, .name) {
    logger::log_info("=== Country: {.name} ===")
    download_parquet_from_cloud(
      prefix = .prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options,
      bucket_name = conf$api$trips$bucket
    ) |>
      preprocess_landings(country = .name)
  }) |>
    dplyr::bind_rows()

  all_trips <- trip_level(all_landings)

  logger::log_info(
    "Landings loaded: {nrow(all_landings)} records, ",
    "{dplyr::n_distinct(all_landings$trip_id)} trips across ",
    "{dplyr::n_distinct(all_landings$country)} countries."
  )

  # ── Step 3. Compute indicator datasets ──────────────────────────────────────

  # 3a. Weekly landing dynamics ------------------------------------------------
  logger::log_info("Computing weekly landing dynamics ...")
  dashboard_weekly <- all_trips |>
    dplyr::group_by(.data$country, .data$week_start) |>
    dplyr::summarise(
      total_catch_kg = sum(.data$tot_catch_kg, na.rm = TRUE),
      n_trips = dplyr::n(),
      .groups = "drop"
    )

  # 3b. Top taxa by biomass ----------------------------------------------------
  logger::log_info("Computing top taxa ...")
  dashboard_top_taxa <- all_landings |>
    dplyr::group_by(.data$country, .data$catch_taxon) |>
    dplyr::summarise(
      total_kg = sum(.data$catch_kg, na.rm = TRUE),
      n_trips = dplyr::n_distinct(.data$trip_id),
      n_sites = dplyr::n_distinct(.data$district),
      mean_kg = mean(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$country) |>
    dplyr::mutate(pct = .data$total_kg / sum(.data$total_kg) * 100) |>
    dplyr::slice_max(.data$total_kg, n = 10) |>
    dplyr::ungroup()

  # 3c. Frequency × biomass rarity quadrant + vulnerability --------------------
  logger::log_info("Computing rarity quadrant ...")
  dashboard_rarity <- all_landings |>
    dplyr::group_by(.data$country, .data$catch_taxon) |>
    dplyr::summarise(
      frequency = dplyr::n_distinct(.data$trip_id),
      total_kg = sum(.data$catch_kg, na.rm = TRUE),
      n_sites = dplyr::n_distinct(.data$district),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$country) |>
    dplyr::mutate(
      freq_pct = .data$frequency / sum(.data$frequency) * 100,
      biomass_pct = .data$total_kg / sum(.data$total_kg) * 100,
      category = dplyr::case_when(
        .data$freq_pct > stats::median(.data$freq_pct) &
          .data$biomass_pct > stats::median(.data$biomass_pct) ~ "Dominant",
        .data$freq_pct > stats::median(.data$freq_pct) &
          .data$biomass_pct <=
          stats::median(.data$biomass_pct) ~ "Frequent Low Yield",
        .data$freq_pct <= stats::median(.data$freq_pct) &
          .data$biomass_pct >
          stats::median(.data$biomass_pct) ~ "Rare High Yield",
        TRUE ~ "Rare Low Yield"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(taxa_traits, by = "catch_taxon")

  # 3d. Seasonal catch calendar (top N taxa per country) -----------------------
  logger::log_info("Computing seasonal catch calendar ...")
  top_taxa_by_country <- dashboard_top_taxa |>
    dplyr::group_by(.data$country) |>
    dplyr::slice_max(.data$total_kg, n = top_n_taxa) |>
    dplyr::ungroup() |>
    dplyr::select("country", "catch_taxon")

  dashboard_seasonal <- all_landings |>
    dplyr::inner_join(top_taxa_by_country, by = c("country", "catch_taxon")) |>
    dplyr::group_by(
      .data$country,
      .data$catch_taxon,
      .data$month_num,
      .data$month
    ) |>
    dplyr::summarise(
      total_kg = sum(.data$catch_kg, na.rm = TRUE),
      n_trips = dplyr::n_distinct(.data$trip_id),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$country, .data$catch_taxon) |>
    dplyr::mutate(
      scaled_kg = .data$total_kg / max(.data$total_kg, na.rm = TRUE)
    ) |>
    dplyr::ungroup()

  # 3e. Monthly CPUE trend with IQR --------------------------------------------
  logger::log_info("Computing monthly CPUE trend ...")
  dashboard_cpue <- all_trips |>
    dplyr::filter(
      !is.na(.data$cpue_kg_hr),
      is.finite(.data$cpue_kg_hr),
      .data$cpue_kg_hr > 0
    ) |>
    dplyr::group_by(.data$country, .data$month_num, .data$month) |>
    dplyr::summarise(
      median_cpue = stats::median(.data$cpue_kg_hr, na.rm = TRUE),
      q25 = stats::quantile(.data$cpue_kg_hr, 0.25, na.rm = TRUE),
      q75 = stats::quantile(.data$cpue_kg_hr, 0.75, na.rm = TRUE),
      n_trips = dplyr::n(),
      .groups = "drop"
    )

  # 3f. Mean trophic level by gear, region, month ------------------------------
  logger::log_info("Computing mean trophic level indicators ...")
  landings_troph <- all_landings |>
    dplyr::left_join(
      taxa_traits |> dplyr::select("catch_taxon", "food_troph"),
      by = "catch_taxon"
    ) |>
    dplyr::filter(!is.na(.data$food_troph))

  mtl_gear <- landings_troph |>
    dplyr::group_by(.data$country, dimension_value = .data$gear) |>
    dplyr::summarise(
      mtl = stats::weighted.mean(.data$food_troph, .data$catch_kg, na.rm = TRUE),
      n_trips = dplyr::n_distinct(.data$trip_id),
      .groups = "drop"
    ) |>
    dplyr::filter(
      !is.na(.data$dimension_value),
      .data$dimension_value != "",
      .data$n_trips >= 5
    ) |>
    dplyr::mutate(dimension = "gear")

  mtl_region <- landings_troph |>
    dplyr::group_by(.data$country, dimension_value = .data$province) |>
    dplyr::summarise(
      mtl = stats::weighted.mean(.data$food_troph, .data$catch_kg, na.rm = TRUE),
      n_trips = dplyr::n_distinct(.data$trip_id),
      .groups = "drop"
    ) |>
    dplyr::filter(!is.na(.data$dimension_value)) |>
    dplyr::mutate(dimension = "region")

  mtl_month <- landings_troph |>
    dplyr::group_by(.data$country, .data$month_num, .data$month) |>
    dplyr::summarise(
      mtl = stats::weighted.mean(.data$food_troph, .data$catch_kg, na.rm = TRUE),
      q25 = stats::quantile(.data$food_troph, 0.25, na.rm = TRUE),
      q75 = stats::quantile(.data$food_troph, 0.75, na.rm = TRUE),
      n_trips = dplyr::n_distinct(.data$trip_id),
      .groups = "drop"
    ) |>
    dplyr::mutate(dimension = "month", dimension_value = as.character(.data$month))

  dashboard_mtl <- dplyr::bind_rows(mtl_gear, mtl_region, mtl_month) |>
    dplyr::select(
      "country",
      "dimension",
      "dimension_value",
      "mtl",
      "n_trips",
      "month_num",
      "q25",
      "q75"
    )

  # 3g. High-vulnerability dominant species ------------------------------------
  logger::log_info("Computing vulnerability alert dataset ...")
  dashboard_vuln <- dashboard_rarity |>
    dplyr::filter(.data$category == "Dominant") |>
    dplyr::mutate(
      high_fishing_vuln = !is.na(.data$vulnerability_fishing) &
        .data$vulnerability_fishing > 75,
      high_climate_vuln = !is.na(.data$vulnerability_climate) &
        .data$vulnerability_climate > 75
    )

  # ── Step 4. Upload all datasets to GCS ──────────────────────────────────────
  datasets <- list(
    dashboard_weekly = dashboard_weekly,
    dashboard_top_taxa = dashboard_top_taxa,
    dashboard_rarity = dashboard_rarity,
    dashboard_seasonal = dashboard_seasonal,
    dashboard_cpue = dashboard_cpue,
    dashboard_mtl = dashboard_mtl,
    dashboard_vuln = dashboard_vuln
  )

  purrr::iwalk(datasets, function(.data, .name) {
    logger::log_info("Uploading {.name} ({nrow(.data)} rows) ...")
    filename <- add_version(.name, extension = "parquet")
    on.exit(
      if (file.exists(filename)) file.remove(filename),
      add = TRUE
    )
    arrow::write_parquet(.data, filename)
    upload_cloud_file(
      file = filename,
      provider = conf$storage$google$key,
      options = coasts_opts
    )
    logger::log_info("  Uploaded: {basename(filename)}")
  })

  logger::log_success(
    "Dashboard datasets exported: {length(datasets)} parquets uploaded to GCS."
  )
  invisible(NULL)
}
