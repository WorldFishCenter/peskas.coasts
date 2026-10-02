#' Raise catch and revenue with the FAO sample-survey method
#'
#' @description
#' Estimates monthly total catch and revenue per district and fishing unit with
#' the method of OPEN ARTFISH (de Graaf, Stamatopoulos & Jarrett 2017, FAO):
#' boats in the census × days each boat fishes in the month × catch per fishing
#' day. [generate_fleet_analysis()] runs it beside its own raising, so the two
#' come from the same data and can be compared.
#'
#' @details
#' - **Fishing unit**: what the census counts in each country (`category_kind`
#'   of the Airtable `frame` table): vessel type in Mozambique, and in Kenya and
#'   Zanzibar the gear, grouped into its FAO ISSCFG main group (`FAO_category`
#'   in the Airtable `gears` table: gillnets, hooks and lines, seine nets, ...)
#'   as the Toolkit advises, so each unit gets more samples.
#' - **Catch per fishing day**: the trip's total catch (or value); one trip is
#'   one fishing day. Trips recorded as catching nothing count as zero. A
#'   district, unit and month with fewer than five trips takes the catch per
#'   trip of all units in that district and month; `catch_source` and
#'   `revenue_source` say which.
#' - **Days fished**: the survey answer to "days fished last week", scaled to the
#'   month. A district, unit and month with fewer than five answers takes the
#'   district and unit average over all months; `effort_source` says which.
#' - **Too few trips**: a district and month with fewer than ten surveyed trips
#'   is not raised (totals `NA`, quality `too_few_trips`), as in
#'   [calculate_district_totals()].
#' - **Error**: the Toolkit's compound relative error at 90%, flagged `pass`
#'   (up to 15%), `warn` (up to 20%), `fail` or `unknown`.
#' - Only trips from the survey named in `fao.surveys` are used, the one that
#'   asks the weekly question: KEFS in Kenya, WF in Zanzibar and DINAPA in
#'   Mozambique.
#'
#' Revenue stays in local currency, as in [generate_fleet_analysis()].
#'
#' @param conf Country configuration from [read_config()]. Its `fao.surveys` names
#'   the validated survey file carrying the weekly fishing-days answer; without
#'   it the country has no FAO raising.
#' @param assets The Airtable assets snapshot, with its `frame` and `gear_groups`
#'   tables.
#'
#' @return One row per district, fishing unit and month (see
#'   [estimate_catch_fao()]), or `NULL` when the country has no `fao.surveys` or
#'   the raising fails.
#' @keywords internal
raise_catch_fao <- function(conf, assets) {
  if (is.null(conf$fao$surveys)) {
    return(NULL)
  }
  # A failure here must not stop the current estimates from going out.
  tryCatch(
    {
      api <- conf$api$trips$validated
      landings <- download_parquet_from_cloud(
        prefix = paste0(api$cloud_path, "/", api$file_prefix),
        provider = conf$storage$google$key,
        options = resolve_storage_opts(conf, "api", error_if_missing = TRUE)
      )
      surveys <- download_parquet_from_cloud(
        prefix = conf$fao$surveys,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
      trips <- fao_trips(landings, surveys)
      out <- estimate_catch_fao(trips, assets$frame, assets$gear_groups)
      logger::log_info(
        "FAO raising: {sum(out$n_trips)} of {nrow(trips)} trips raised in {nrow(out)} district-unit-months"
      )
      out
    },
    error = function(e) {
      logger::log_error(
        "FAO raising failed, the current estimates go out alone: {conditionMessage(e)}"
      )
      NULL
    }
  )
}

#' FAO totals per district and month, as portal metrics
#'
#' Adds up the fishing units of [raise_catch_fao()] into the three metrics
#' [export_portal()] publishes beside the current method's: catch in tonnes,
#' revenue, and fishing trips (boats × days fished, one trip a day), each only
#' where the catch was raised.
#'
#' @param fao Output of [raise_catch_fao()].
#' @return One row per district and month with `estimated_fishing_trips_fao`,
#'   `estimated_catch_tn_fao`, `estimated_revenue_fao` and `date`.
#' @keywords internal
fao_portal_metrics <- function(fao) {
  fao |>
    dplyr::mutate(
      trips = dplyr::if_else(
        is.na(.data$total_catch_kg),
        NA_real_,
        .data$boats * .data$days_fished
      )
    ) |>
    dplyr::summarise(
      estimated_fishing_trips_fao = stat_or_na(.data$trips, sum),
      estimated_catch_tn_fao = stat_or_na(.data$total_catch_kg, sum) / 1000,
      estimated_revenue_fao = stat_or_na(.data$total_revenue, sum),
      .by = c("gaul_2_name", "date_month")
    ) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date_month), .keep = "unused")
}

#' One row per trip with its catch, value and weekly fishing days
#'
#' @param landings Validated API landings, one row per catch item.
#' @param surveys The country's validated survey table, which carries the
#'   "days fished last week" answer under `fishing_per_week` (Kenya) or
#'   `fishing_days_week`.
#'
#' @return Trips that appear in `surveys`, with `catch_kg`, `revenue`,
#'   `days_week` (0-7, otherwise `NA`) and `date_month`.
#' @keywords internal
fao_trips <- function(landings, surveys) {
  weekly <- surveys |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c(
        days_week = "fishing_per_week",
        days_week = "fishing_days_week"
      ))
    ) |>
    dplyr::mutate(
      submission_id = as.character(.data$submission_id),
      days_week = as.numeric(.data$days_week)
    ) |>
    dplyr::distinct()

  landings |>
    dplyr::distinct(.data$trip_id, .keep_all = TRUE) |>
    dplyr::mutate(submission_id = sub("^TRIP_", "", .data$trip_id)) |>
    # Only this survey asks the weekly question, so this also drops Kenya's WCS trips.
    dplyr::inner_join(weekly, by = "submission_id") |>
    dplyr::mutate(
      no_catch = .data$catch_outcome %in% "0",
      catch_kg = dplyr::if_else(.data$no_catch, 0, .data$tot_catch_kg),
      revenue = dplyr::if_else(.data$no_catch, 0, .data$tot_catch_price),
      days_week = dplyr::if_else(
        .data$days_week %in% 0:7,
        .data$days_week,
        NA_real_
      ),
      date_month = lubridate::floor_date(.data$landing_date, "month")
    )
}

#' Monthly totals per district and fishing unit from trips and the census
#'
#' @param trips Output of [fao_trips()] for one country.
#' @param frame The Airtable `frame` table: boats per district (`gaul_2_code`)
#'   and `standard_name`, counted by gear or by vessel (`category_kind`).
#' @param gear_groups The Airtable `gears` table's `standard_name` and
#'   `fao_category`. A gear without a category stays its own unit.
#' @param min_samples Trips (or weekly answers) a district, unit and month needs
#'   to stand on its own; below it, it borrows as described in [raise_catch_fao()].
#' @param min_district_trips Surveyed trips a district and month needs to be
#'   raised at all.
#'
#' @return One row per district, fishing unit and month that has trips and a
#'   census count.
#' @keywords internal
estimate_catch_fao <- function(
  trips,
  frame,
  gear_groups,
  min_samples = 5,
  min_district_trips = 10
) {
  frame <- dplyr::filter(frame, .data$gaul_2_code %in% trips$gaul_2_code)
  kind <- unique(frame$category_kind)
  if (length(kind) != 1) {
    stop("The census must count one kind of unit per country, found: ", toString(kind))
  }
  groups <- gear_groups |>
    dplyr::filter(!is.na(.data$fao_category), .data$fao_category != "") |>
    dplyr::distinct(.data$standard_name, .data$fao_category)
  twice <- unique(groups$standard_name[duplicated(groups$standard_name)])
  if (length(twice)) {
    stop("Gears with more than one FAO_category in Airtable: ", toString(twice))
  }
  groups <- rlang::set_names(groups$fao_category, groups$standard_name)
  to_unit <- if (kind == "gear") {
    function(gear) dplyr::coalesce(unname(groups[gear]), gear)
  } else {
    identity
  }

  census <- frame |>
    dplyr::mutate(fishing_unit = to_unit(.data$standard_name)) |>
    dplyr::summarise(
      boats = sum(.data$n_boats, na.rm = TRUE),
      .by = c("gaul_2_code", "fishing_unit")
    ) |>
    dplyr::filter(.data$boats > 0)

  # Every surveyed trip counts, as `n_submissions` does for generate_fleet_analysis().
  district_trips <- dplyr::summarise(
    trips,
    district_trips = dplyr::n(),
    .by = c("gaul_2_code", "date_month")
  )

  trips <- trips |>
    dplyr::mutate(
      fishing_unit = to_unit(.data[[c(gear = "gear", vessel = "vessel_type")[[kind]]]]),
      days = .data$days_week / 7 * lubridate::days_in_month(.data$date_month)
    ) |>
    dplyr::inner_join(census, by = c("gaul_2_code", "fishing_unit"))

  # Sample size, mean and sd of catch, revenue and days, e.g. `catch_kg_mean`.
  stats_by <- function(by) {
    dplyr::summarise(
      trips,
      dplyr::across(
        c("catch_kg", "revenue", "days"),
        list(
          n = ~ sum(!is.na(.x)),
          mean = ~ stat_or_na(.x, mean),
          sd = ~ stat_or_na(.x, stats::sd)
        )
      ),
      .by = dplyr::all_of(by)
    )
  }
  district_month <- stats_by(c("gaul_2_code", "date_month")) |>
    dplyr::rename_with(~ paste0(.x, "_district"), -c("gaul_2_code", "date_month"))
  unit_all_months <- stats_by(c("gaul_2_code", "fishing_unit")) |>
    dplyr::rename_with(~ paste0(.x, "_all"), -c("gaul_2_code", "fishing_unit"))

  stats_by(c("gaul_2_code", "gaul_2_name", "fishing_unit", "date_month")) |>
    dplyr::left_join(census, by = c("gaul_2_code", "fishing_unit")) |>
    dplyr::left_join(district_month, by = c("gaul_2_code", "date_month")) |>
    dplyr::left_join(unit_all_months, by = c("gaul_2_code", "fishing_unit")) |>
    dplyr::left_join(district_trips, by = c("gaul_2_code", "date_month")) |>
    dplyr::mutate(
      own_catch = .data$catch_kg_n >= min_samples,
      own_revenue = .data$revenue_n >= min_samples,
      own_days = .data$days_n >= min_samples,
      catch_source = dplyr::if_else(.data$own_catch, "unit", "district"),
      revenue_source = dplyr::if_else(.data$own_revenue, "unit", "district"),
      effort_source = dplyr::case_when(
        .data$own_days ~ "month",
        .data$days_n_all > 0 ~ "all_months"
      ),
      catch_n = dplyr::if_else(.data$own_catch, .data$catch_kg_n, .data$catch_kg_n_district),
      catch_mean = dplyr::if_else(.data$own_catch, .data$catch_kg_mean, .data$catch_kg_mean_district),
      catch_sd = dplyr::if_else(.data$own_catch, .data$catch_kg_sd, .data$catch_kg_sd_district),
      revenue_n_used = dplyr::if_else(.data$own_revenue, .data$revenue_n, .data$revenue_n_district),
      revenue_mean = dplyr::if_else(.data$own_revenue, .data$revenue_mean, .data$revenue_mean_district),
      revenue_sd_used = dplyr::if_else(.data$own_revenue, .data$revenue_sd, .data$revenue_sd_district),
      n_days = dplyr::if_else(.data$own_days, .data$days_n, .data$days_n_all),
      days_fished = dplyr::if_else(.data$own_days, .data$days_mean, .data$days_mean_all),
      days_sd = dplyr::if_else(.data$own_days, .data$days_sd, .data$days_sd_all),
      raised = .data$district_trips >= min_district_trips,
      total_catch_kg = dplyr::if_else(
        .data$raised,
        .data$boats * .data$days_fished * .data$catch_mean,
        NA_real_
      ),
      total_revenue = dplyr::if_else(
        .data$raised,
        .data$boats * .data$days_fished * .data$revenue_mean,
        NA_real_
      ),
      re_catch = compound_re(
        .data$catch_mean, .data$catch_sd, .data$catch_n,
        .data$days_fished, .data$days_sd, .data$n_days
      ),
      re_revenue = compound_re(
        .data$revenue_mean, .data$revenue_sd_used, .data$revenue_n_used,
        .data$days_fished, .data$days_sd, .data$n_days
      ),
      quality_catch = dplyr::if_else(.data$raised, fao_quality(.data$re_catch), "too_few_trips"),
      quality_revenue = dplyr::if_else(.data$raised, fao_quality(.data$re_revenue), "too_few_trips")
    ) |>
    dplyr::select(
      "gaul_2_code", "gaul_2_name", "fishing_unit", "date_month", "boats",
      n_trips = "catch_kg_n", "catch_source", catch_kg_per_trip = "catch_mean",
      "n_days", "days_fished", "effort_source",
      "total_catch_kg", "re_catch", "quality_catch",
      n_revenue = "revenue_n", "revenue_source", revenue_per_trip = "revenue_mean",
      "total_revenue", "re_revenue", "quality_revenue"
    )
}

#' Compound relative error of a raised total (OPEN ARTFISH, p. 9)
#'
#' Raises the upper 90% confidence limits of catch per day and of days fished
#' together and returns how far that lifts the total, as a share of it. The
#' boat count cancels out.
#'
#' @param mean_x,sd_x,n_x Mean, standard deviation and sample size of catch (or
#'   value) per fishing day.
#' @param mean_d,sd_d,n_d The same for days fished per boat in the month.
#' @param alpha Two-sided significance level; 0.10 is the Toolkit's 90%.
#'
#' @return Relative error, `NA` when either sample has fewer than two values or
#'   a zero mean.
#' @keywords internal
compound_re <- function(mean_x, sd_x, n_x, mean_d, sd_d, n_d, alpha = 0.10) {
  ok <- n_x >= 2 & n_d >= 2 & mean_x > 0 & mean_d > 0
  ok[is.na(ok)] <- FALSE
  limit <- function(s, n) stats::qt(1 - alpha / 2, df = pmax(n - 1, 1)) * s / sqrt(n)
  re <- (mean_x + limit(sd_x, n_x)) * (mean_d + limit(sd_d, n_d)) /
    (mean_x * mean_d) - 1
  dplyr::if_else(ok, re, NA_real_)
}

fao_quality <- function(re) {
  dplyr::case_when(
    is.na(re) ~ "unknown",
    re <= 0.15 ~ "pass",
    re <= 0.20 ~ "warn",
    .default = "fail"
  )
}
