# =============================================================================
#  FAO-aligned statistical aggregation for small-scale fisheries
# =============================================================================
#
#  Implements the routine catch & effort estimation procedure described in:
#    de Graaf G., Stamatopoulos C., Jarrett T. (2017).
#    OPEN ARTFISH and the FAO ODK mobile phone application — a toolkit
#    for small-scale fisheries routine data collection. FAO, Rome.
#
#  Methodological summary (toolkit pp. 4–10):
#
#  1.  Fishing unit (fu) = vessel type × major gear (e.g. "motorised_canoe |
#      gillnet"). FAO discourages gear-only schemes.
#
#  2.  Stratification:
#       - MAJOR stratum = administrative unit (here: gaul_2_name / district).
#         Used for reporting only.
#       - MINOR stratum = homogeneous landing-site group (here: landing_site).
#         All population estimates are computed AT THE MINOR STRATUM LEVEL,
#         then aggregated up to the major stratum.
#
#  3.  Catch formula, evaluated per (minor stratum × fishing unit × month):
#         Total monthly catch_fu = F_fu × avg_fishing_days_fu × avg_CPUE_fu
#       where
#         F_fu                = number of fu in the frame (active + inactive)
#         avg_fishing_days_fu = AC × D
#                                AC = BAC (vertical) or PAB (horizontal)
#                                D  = max fishing days in month (BAC)
#                                     or calendar days in month (PAB)
#         avg_CPUE_fu         = mean catch per fishing day, fu-specific
#
#  4.  Reliability is reported as Relative Error (RE) at 90 % probability,
#         RE = t_(n-1, 1-α/2) × s / (√n × x̄)
#       FAO target: RE < 0.15. Compound RE for total catch follows the
#       7-step procedure on toolkit p. 9 (CL_days + CL_cpue → CL_catch).
#
#  This file is the FAO-statistical companion to model-cpue.R / aggregate-
#  effort.R — those handle spatial (H3) effort & CPUE; this handles the
#  reporting estimates that feed the country dashboards.
#
#  All functions are pure: no I/O, no GCS calls. Wire them into the targets
#  pipeline by feeding the validated catch table (and, when available, the
#  frame survey + activity observations).
#
# =============================================================================


# ── 1. FISHING UNITS ─────────────────────────────────────────────────────────

#' Build FAO fishing-unit labels from vessel & gear columns
#'
#' A fishing unit (`fu`) is defined by the combination of vessel
#' characteristics and major gear (FAO toolkit p. 6). This function
#' concatenates one or more columns into a stable `fishing_unit` label.
#'
#' Rows with `NA` in any of the input columns get `NA` for `fishing_unit`
#' and are excluded from FAO estimation downstream.
#'
#' @param data    Data frame with vessel / gear columns.
#' @param fu_cols Character vector of column names defining the fishing unit.
#'                Default: `c("vessel_type", "gear")`.
#' @param sep     Separator between components. Default: " | ".
#'
#' @return `data` with an added (or overwritten) `fishing_unit` column.
#' @export
build_fishing_units <- function(data,
                                fu_cols = c("vessel_type", "gear"),
                                sep     = " | ") {

  fu_cols_present <- intersect(fu_cols, colnames(data))
  if (length(fu_cols_present) == 0L) {
    stop(
      "None of the fishing-unit columns ", paste(fu_cols, collapse = ", "),
      " are present in the data."
    )
  }

  if (length(fu_cols_present) < length(fu_cols)) {
    logger::log_warn(
      "Missing fishing-unit columns: ",
      paste(setdiff(fu_cols, fu_cols_present), collapse = ", "),
      ". Using ", paste(fu_cols_present, collapse = " + "), " only."
    )
  }

  fu_mat <- as.matrix(data[, fu_cols_present, drop = FALSE])
  fu     <- apply(fu_mat, 1L, function(row) {
    if (any(is.na(row) | row == "")) NA_character_
    else paste(row, collapse = sep)
  })

  data |>
    dplyr::mutate(fishing_unit = fu) |>
    dplyr::relocate(fishing_unit, .after = dplyr::all_of(fu_cols_present[length(fu_cols_present)]))
}


# ── 2. RELATIVE ERROR HELPERS ────────────────────────────────────────────────

#' Relative error of a sample mean (FAO toolkit p. 7)
#'
#' Computes RE = t_(n-1, 1-alpha/2) * s / (sqrt(n) * mean(x)), expressed
#' as a proportion (multiply by 100 for percent). Returns `NA` if `n < 2`,
#' `mean(x) = 0`, or all values are `NA`.
#'
#' @param x     Numeric vector of sample observations.
#' @param alpha Significance level. Default 0.10 (90 percent CI).
#'
#' @return Single numeric value, the relative error as a proportion.
#' @export
relative_error_t <- function(x, alpha = 0.10) {
  x <- x[!is.na(x) & is.finite(x)]
  n <- length(x)
  if (n < 2L) return(NA_real_)

  mean_x <- mean(x)
  if (mean_x == 0) return(NA_real_)

  sd_x   <- stats::sd(x)
  t_crit <- stats::qt(1 - alpha / 2, df = n - 1L)
  abs((t_crit * sd_x / sqrt(n)) / mean_x)
}


#' Compound relative error of total monthly catch (FAO toolkit p. 9)
#'
#' Implements the 7-step procedure that propagates uncertainty from both
#' fishing days and CPUE into the total-catch estimate:
#'
#' \enumerate{
#'   \item CL_days  = t * s_days  / sqrt(n_days)
#'   \item CL_cpue  = t * s_cpue  / sqrt(n_cpue)
#'   \item max_days = mean_days + CL_days
#'   \item max_cpue = mean_cpue + CL_cpue
#'   \item max_catch = F * max_days * max_cpue
#'   \item CL_catch  = max_catch - (F * mean_days * mean_cpue)
#'   \item RE_catch  = CL_catch / mean_catch
#' }
#'
#' @param cpue_vec Numeric vector of trip-level CPUE observations.
#' @param days_vec Numeric vector of per-unit fishing-days observations
#'                 (one per active fishing unit in the period).
#' @param F_total  Total number of fishing units in the frame.
#' @param alpha    Significance level. Default 0.10.
#'
#' @return Single numeric value (proportion).
#' @export
compound_re_catch <- function(cpue_vec, days_vec, F_total, alpha = 0.10) {

  cpue_vec <- cpue_vec[!is.na(cpue_vec) & is.finite(cpue_vec)]
  days_vec <- days_vec[!is.na(days_vec) & is.finite(days_vec)]

  n_c <- length(cpue_vec); n_d <- length(days_vec)
  if (n_c < 2L || n_d < 2L) return(NA_real_)
  if (is.na(F_total) || F_total <= 0) return(NA_real_)

  m_c <- mean(cpue_vec); s_c <- stats::sd(cpue_vec)
  m_d <- mean(days_vec); s_d <- stats::sd(days_vec)
  if (m_c == 0 || m_d == 0) return(NA_real_)

  cl_c <- stats::qt(1 - alpha / 2, df = n_c - 1L) * s_c / sqrt(n_c)
  cl_d <- stats::qt(1 - alpha / 2, df = n_d - 1L) * s_d / sqrt(n_d)

  mean_catch <- F_total * m_d * m_c
  max_catch  <- F_total * (m_d + cl_d) * (m_c + cl_c)

  abs((max_catch - mean_catch) / mean_catch)
}


# ── 3. ACTIVITY COEFFICIENTS (BAC / PAB) ─────────────────────────────────────

#' Boat Activity Coefficient — vertical sampling (FAO p. 9)
#'
#' BAC = (active fishing units summed over the period) / (examined units
#' summed over the same period). Computed per group.
#'
#' @param obs       Data frame with daily counts of active and examined units.
#' @param group_by  Character vector of grouping columns
#'                  (e.g. `c("landing_site", "fishing_unit", "year_month")`).
#' @param active    Column with daily active-unit count. Default "n_active".
#' @param examined  Column with daily examined-unit count. Default "n_examined".
#'
#' @return Tibble with one row per group and a `bac` column.
#' @export
compute_bac <- function(obs,
                        group_by,
                        active   = "n_active",
                        examined = "n_examined") {

  obs |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      n_active   = sum(.data[[active]],   na.rm = TRUE),
      n_examined = sum(.data[[examined]], na.rm = TRUE),
      bac        = dplyr::if_else(n_examined > 0,
                                  n_active / n_examined,
                                  NA_real_),
      .groups    = "drop"
    )
}


#' Probability Active Boat — horizontal sampling (FAO p. 9)
#'
#' PAB = (today + yesterday + day-before-yesterday + n_days_last_week) / 10,
#' from the four CAS questions asked of the fisher.
#'
#' @param today              0/1 — fished today.
#' @param yesterday          0/1 — fished yesterday.
#' @param before_yesterday   0/1 — fished day before yesterday.
#' @param last_week          0–7 — number of days fished in the previous week.
#'
#' @return Numeric vector of PAB values (0–1).
#' @export
compute_pab <- function(today, yesterday, before_yesterday, last_week) {
  (
    pmin(pmax(today,            0L), 1L) +
      pmin(pmax(yesterday,        0L), 1L) +
      pmin(pmax(before_yesterday, 0L), 1L) +
      pmin(pmax(last_week,        0L), 7L)
  ) / 10
}


#' Fall-back activity coefficient derived from observed catch records
#'
#' When no separate effort survey exists, approximate AC as the fraction of
#' calendar days in the period on which a unique boat was observed landing.
#' This is biased upward (only observed units enter) and downward (only
#' sampled days). Use as a placeholder until BAC/PAB data are available.
#'
#' @param landings    Trip-level data with one row per landing.
#' @param group_by    Grouping columns (typically minor stratum × fu × period).
#' @param boat_col    Column with unique boat identifier. Default "boat_name".
#' @param date_col    Column with landing date. Default "landing_date".
#' @param days_in_period Either an integer scalar (e.g. 30) or a column name
#'                       giving the calendar denominator per group.
#'
#' @return Tibble with `ac_observed` and `n_unique_boats` per group.
#' @keywords internal
derive_activity_observed <- function(landings,
                                     group_by,
                                     boat_col       = "boat_name",
                                     date_col       = "landing_date",
                                     days_in_period = 30L) {

  logger::log_warn(
    "Deriving activity coefficient from observed landings — ",
    "this is a placeholder. Replace with BAC or PAB data when available."
  )

  landings |>
    dplyr::filter(!is.na(.data[[boat_col]]), !is.na(.data[[date_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      n_unique_boats = dplyr::n_distinct(.data[[boat_col]]),
      n_active_days  = dplyr::n_distinct(as.Date(.data[[date_col]])),
      ac_observed    = pmin(n_active_days / days_in_period, 1),
      .groups        = "drop"
    )
}


# ── 4. TRIP-LEVEL CPUE ───────────────────────────────────────────────────────

#' Collapse a landings table to one row per trip
#'
#' Combines per-species rows into a single trip-level record with total
#' catch, then computes daily CPUE = total_catch / trip_duration_days.
#' Trips with non-positive or missing duration are dropped (FAO requires
#' a defined daily catch).
#'
#' @param landings        Validated landings with one row per species per trip.
#' @param trip_id_col     Column with the trip identifier. Default
#'                        "submission_id".
#' @param duration_col    Column with trip duration. Default "trip_duration".
#' @param duration_units  Units of `duration_col`: "hours" (default) or "days".
#' @param catch_col       Per-row catch in kg. Default "catch_kg".
#' @param keep_cols       Columns to carry through unchanged (one value per
#'                        trip — typically stratum + fu identifiers + date).
#'
#' @return Tibble: one row per trip with `total_catch_kg`, `fishing_days`,
#'         `cpue_kg_day` and the `keep_cols` carried through.
#' @export
compute_trip_cpue <- function(landings,
                              trip_id_col    = "submission_id",
                              duration_col   = "trip_duration",
                              duration_units = c("hours", "days"),
                              catch_col      = "catch_kg",
                              keep_cols      = c("gaul_2_name",
                                                 "landing_site",
                                                 "fishing_unit",
                                                 "landing_date")) {

  duration_units <- match.arg(duration_units)
  keep_cols      <- intersect(keep_cols, colnames(landings))

  trips <- landings |>
    dplyr::filter(!is.na(.data[[trip_id_col]]),
                  !is.na(.data[[duration_col]]),
                  .data[[duration_col]] > 0,
                  !is.na(.data[[catch_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(trip_id_col, keep_cols)))) |>
    dplyr::summarise(
      total_catch_kg = sum(.data[[catch_col]], na.rm = TRUE),
      trip_duration  = dplyr::first(.data[[duration_col]]),
      .groups        = "drop"
    ) |>
    dplyr::mutate(
      fishing_days = if (duration_units == "hours") trip_duration / 24
      else                           trip_duration,
      fishing_days = pmax(fishing_days, 1 / 24),     # floor at 1 hour
      cpue_kg_day  = total_catch_kg / fishing_days
    ) |>
    dplyr::filter(is.finite(cpue_kg_day), cpue_kg_day >= 0)

  logger::log_info(
    "Trip-level CPUE: {nrow(trips)} trips | ",
    "median CPUE = {round(stats::median(trips$cpue_kg_day, na.rm = TRUE), 1)} kg/day"
  )
  trips
}


# ── 5. STRATUM-LEVEL CPUE & EFFORT SUMMARIES ─────────────────────────────────

#' Mean CPUE and relative error per minor stratum × fu × period
#'
#' @param trips      Output of `compute_trip_cpue()`.
#' @param group_by   Character vector of grouping columns.
#' @param cpue_col   CPUE column. Default "cpue_kg_day".
#' @param alpha      For RE. Default 0.10.
#'
#' @return Tibble with `mean_cpue`, `sd_cpue`, `n_cpue`, `re_cpue`.
#' @export
summarize_cpue <- function(trips,
                           group_by  = c("gaul_2_name", "landing_site",
                                         "fishing_unit", "year_month"),
                           cpue_col  = "cpue_kg_day",
                           alpha     = 0.10) {

  group_by <- intersect(group_by, colnames(trips))

  trips |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      mean_cpue = mean(.data[[cpue_col]],   na.rm = TRUE),
      sd_cpue   = stats::sd(.data[[cpue_col]], na.rm = TRUE),
      n_cpue    = sum(!is.na(.data[[cpue_col]])),
      re_cpue   = relative_error_t(.data[[cpue_col]], alpha = alpha),
      .groups   = "drop"
    )
}


#' Mean fishing days per unit + relative error per stratum × fu × period
#'
#' Counts unique fishing days per boat in the period, then summarises the
#' resulting per-boat days vector. This serves as the "average fishing days
#' per fu" term in the FAO formula when activity coefficients aren't
#' available — and as the input to the compound RE for total catch.
#'
#' @param trips      Output of `compute_trip_cpue()` (must include `boat_name`
#'                   and a date column).
#' @param group_by   Character vector of grouping columns.
#' @param boat_col   Boat identifier column. Default "boat_name".
#' @param date_col   Date column. Default "landing_date".
#' @param alpha      For RE. Default 0.10.
#'
#' @return Tibble with `mean_days`, `sd_days`, `n_boats_sampled`, `re_days`.
#' @export
summarize_fishing_days <- function(trips,
                                   group_by = c("gaul_2_name",
                                                "landing_site",
                                                "fishing_unit",
                                                "year_month"),
                                   boat_col = "boat_name",
                                   date_col = "landing_date",
                                   alpha    = 0.10) {

  group_by <- intersect(group_by, colnames(trips))

  per_boat <- trips |>
    dplyr::filter(!is.na(.data[[boat_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(group_by, boat_col)))) |>
    dplyr::summarise(
      days_sampled = dplyr::n_distinct(as.Date(.data[[date_col]])),
      .groups      = "drop"
    )

  per_boat |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      mean_days        = mean(days_sampled,       na.rm = TRUE),
      sd_days          = stats::sd(days_sampled,  na.rm = TRUE),
      n_boats_sampled  = dplyr::n(),
      re_days          = relative_error_t(days_sampled, alpha = alpha),
      .groups          = "drop"
    )
}


# ── 6. FRAME (TOTAL FISHING UNITS) ───────────────────────────────────────────

#' Fall-back frame — count unique boats observed per minor stratum × fu
#'
#' When no frame survey exists, approximate F as the count of unique
#' boats observed in the validated landings. Biased downward (any
#' un-sampled vessel is invisible). Use as a placeholder.
#'
#' @param landings  Validated landings with `boat_name`, `fishing_unit`,
#'                  and stratum columns.
#' @param group_by  Grouping columns. Default minor stratum × fu.
#' @param boat_col  Boat identifier. Default "boat_name".
#'
#' @return Tibble with `F_total` per group.
#' @keywords internal
derive_frame_observed <- function(landings,
                                  group_by = c("gaul_2_name",
                                               "landing_site",
                                               "fishing_unit"),
                                  boat_col = "boat_name") {

  logger::log_warn(
    "Deriving frame F from observed boats — placeholder. ",
    "Replace with frame-survey or vessel-register counts when available."
  )

  group_by <- intersect(group_by, colnames(landings))

  landings |>
    dplyr::filter(!is.na(.data[[boat_col]]),
                  !is.na(fishing_unit)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      F_total = dplyr::n_distinct(.data[[boat_col]]),
      .groups = "drop"
    )
}


# ── 7. FAO TOTAL CATCH AT MINOR STRATUM ──────────────────────────────────────

#' FAO total monthly catch per (minor stratum × fishing unit × period)
#'
#' Implements the master FAO formula
#'   Total_catch = F × avg_fishing_days × avg_CPUE
#' with compound relative error per the 7-step procedure on toolkit p. 9.
#'
#' @param cpue_summary   Output of `summarize_cpue()`.
#' @param days_summary   Output of `summarize_fishing_days()`.
#' @param frame          Tibble with `F_total` per group.
#' @param trips          Trip-level table (needed for the compound RE that
#'                       uses the raw vectors, not just summary stats).
#' @param join_keys      Columns to join the summaries on. Default minor
#'                       stratum × fu × period.
#' @param boat_col       Vessel-identifier column in `trips`. If `NULL` or
#'                       absent, the days-vector for compound RE collapses
#'                       to one observation per trip and the compound RE
#'                       degrades to the CPUE RE (with a warning).
#' @param date_col       Date column in `trips`. Default "landing_date".
#' @param alpha          For compound RE. Default 0.10.
#'
#' @return Tibble: one row per (minor stratum × fu × period) with
#'         total_catch, RE, and component diagnostics.
#' @export
estimate_minor_total <- function(cpue_summary,
                                 days_summary,
                                 frame,
                                 trips,
                                 join_keys = c("gaul_2_name",
                                               "landing_site",
                                               "fishing_unit",
                                               "year_month"),
                                 boat_col  = "boat_name",
                                 date_col  = "landing_date",
                                 alpha     = 0.10) {

  join_keys  <- intersect(join_keys, colnames(cpue_summary))
  frame_keys <- intersect(join_keys, colnames(frame))

  est <- cpue_summary |>
    dplyr::inner_join(days_summary, by = join_keys) |>
    dplyr::left_join(frame,         by = frame_keys) |>
    dplyr::mutate(
      total_catch_kg = F_total * mean_days * mean_cpue
    )

  # Compound RE — done per group with raw vectors. Days-per-unit needs a
  # vessel identifier; if missing, we fall back to a 1-day-per-trip vector
  # and the compound RE collapses to the CPUE RE.
  has_boats <- !is.null(boat_col) && boat_col %in% colnames(trips)
  if (!has_boats) {
    logger::log_warn(
      "estimate_minor_total: no vessel identifier (`", boat_col, "`) in trips. ",
      "Compound RE for total catch will reduce to the CPUE RE — bring in ",
      "BAC/PAB activity data to recover the days-variability term."
    )
  }

  re_compound <- trips |>
    dplyr::group_by(dplyr::across(dplyr::all_of(join_keys))) |>
    dplyr::summarise(
      cpue_vec = list(cpue_kg_day),
      days_vec = list({
        if (has_boats) {
          per_boat <- tapply(
            as.Date(.data[[date_col]]),
            .data[[boat_col]],
            FUN = function(d) length(unique(d))
          )
          as.numeric(per_boat)
        } else {
          # one fishing day per trip — variance becomes 0 by construction
          rep(1, dplyr::n())
        }
      }),
      .groups  = "drop"
    ) |>
    dplyr::left_join(dplyr::select(est,
                                   dplyr::all_of(join_keys), F_total),
                     by = join_keys) |>
    dplyr::mutate(
      re_total_catch = purrr::pmap_dbl(
        list(cpue_vec, days_vec, F_total),
        function(c, d, f) compound_re_catch(c, d, f, alpha = alpha)
      )
    ) |>
    dplyr::select(dplyr::all_of(join_keys), re_total_catch)

  est |>
    dplyr::left_join(re_compound, by = join_keys)
}


# ── 8. SPECIES BREAKDOWN (FAO step 2) ────────────────────────────────────────

#' Distribute total catch across species using observed proportions
#'
#' FAO toolkit p. 7:
#'   Total_catch_fu_species = Total_catch_fu × Proportion_fu_species
#'
#' Proportions are computed from observed species-level catch in the same
#' (minor stratum × fishing unit × period) group, normalised to sum to 1.
#'
#' @param landings        Validated landings (one row per species per trip).
#' @param minor_estimates Output of `estimate_minor_total()`.
#' @param join_keys       Same keys used at the minor-stratum level.
#' @param species_col     Species/taxon column. Default "catch_taxon".
#' @param catch_col       Per-row catch in kg. Default "catch_kg".
#'
#' @return Tibble: one row per (group × species) with `total_catch_kg_species`.
#' @export
estimate_species_total <- function(landings,
                                   minor_estimates,
                                   join_keys   = c("gaul_2_name",
                                                   "landing_site",
                                                   "fishing_unit",
                                                   "year_month"),
                                   species_col = "catch_taxon",
                                   catch_col   = "catch_kg") {

  join_keys <- intersect(join_keys, colnames(minor_estimates))

  proportions <- landings |>
    dplyr::filter(!is.na(.data[[species_col]]),
                  !is.na(.data[[catch_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(join_keys, species_col)))) |>
    dplyr::summarise(species_kg = sum(.data[[catch_col]], na.rm = TRUE),
                     .groups    = "drop") |>
    dplyr::group_by(dplyr::across(dplyr::all_of(join_keys))) |>
    dplyr::mutate(proportion = species_kg / sum(species_kg)) |>
    dplyr::ungroup()

  minor_estimates |>
    dplyr::select(dplyr::all_of(join_keys), total_catch_kg) |>
    dplyr::inner_join(proportions, by = join_keys) |>
    dplyr::mutate(total_catch_kg_species = total_catch_kg * proportion)
}


# ── 9. AGGREGATION TO MAJOR STRATUM ──────────────────────────────────────────

#' Aggregate minor-stratum estimates up to the major stratum
#'
#' Per FAO (p. 5): "Totals at the major stratum level are simply
#' aggregations of estimates and counts from the minor strata involved."
#' Catch totals add. RE for the aggregate is approximated by combining
#' minor-stratum CLs in quadrature (independence assumption — flag this
#' if your minor strata are correlated).
#'
#' @param minor_estimates Output of `estimate_minor_total()`.
#' @param major_keys      Grouping columns at the major level. Default
#'                        major stratum × fu × period.
#'
#' @return Tibble: one row per (major stratum × fu × period).
#' @export
aggregate_to_major <- function(minor_estimates,
                               major_keys = c("gaul_2_name",
                                              "fishing_unit",
                                              "year_month")) {

  major_keys <- intersect(major_keys, colnames(minor_estimates))

  minor_estimates |>
    dplyr::filter(!is.na(total_catch_kg)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(major_keys))) |>
    dplyr::summarise(
      total_catch_kg     = sum(total_catch_kg, na.rm = TRUE),
      F_total            = sum(F_total,        na.rm = TRUE),
      n_minor_strata     = dplyr::n(),
      # Quadrature combine of CL_i = RE_i x catch_i, then RE_total = CL / total.
      # If ALL minor strata have NA re, the aggregate must also be NA (not 0).
      re_total_catch     = dplyr::if_else(
        all(is.na(re_total_catch)),
        NA_real_,
        sqrt(sum((re_total_catch * total_catch_kg) ^ 2,
                 na.rm = TRUE)) /
          sum(total_catch_kg, na.rm = TRUE)
      ),
      mean_cpue_weighted = stats::weighted.mean(mean_cpue,
                                                w = F_total * mean_days,
                                                na.rm = TRUE),
      .groups            = "drop"
    )
}


# ── 10. DATA-QUALITY FLAGS ───────────────────────────────────────────────────

#' Flag estimates that fail the FAO 15 percent relative-error target
#'
#' Adds three columns:
#'   `quality_cpue`    - pass / warn / fail based on `re_cpue`.
#'   `quality_catch`   - same, based on `re_total_catch`.
#'   `quality_overall` - worst of the two.
#'
#' @param estimates  Tibble with `re_cpue` and/or `re_total_catch`.
#' @param threshold  RE above which we fail. Default 0.15 (FAO target).
#' @param warn_at    RE above which we warn. Default 0.20.
#'
#' @return The input tibble with quality columns appended.
#' @export
flag_quality <- function(estimates,
                         threshold = 0.15,
                         warn_at   = 0.20) {

  classify <- function(x) {
    dplyr::case_when(
      is.na(x)        ~ "unknown",
      x == 0          ~ "unknown",    # exact zero = degenerate (n<2 or NA propagation)
      x <= threshold  ~ "pass",
      x <= warn_at    ~ "warn",
      TRUE            ~ "fail"
    )
  }

  out <- estimates
  if ("re_cpue"        %in% colnames(out)) out$quality_cpue  <- classify(out$re_cpue)
  if ("re_total_catch" %in% colnames(out)) out$quality_catch <- classify(out$re_total_catch)

  if (all(c("quality_cpue", "quality_catch") %in% colnames(out))) {
    levels_q <- c("unknown", "pass", "warn", "fail")
    r_cpue   <- match(out$quality_cpue,  levels_q, nomatch = 1L)
    r_catch  <- match(out$quality_catch, levels_q, nomatch = 1L)
    out$quality_overall <- levels_q[pmax(r_cpue, r_catch)]
  }
  out
}


# ── 11. COUNTRY-SPECIFIC ADAPTERS ────────────────────────────────────────────

#' Reshape KEFS (Kenya) validated landings into FAO long format
#'
#' KEFS records each trip with a "priority" species (target catch, weighed
#' in full) and a "sample" species (a sub-sample weighed for composition).
#' The FAO estimator wants one row per species per trip with `catch_kg` and
#' `catch_taxon`. This helper unions the priority and sample blocks into
#' that shape, dropping rows with zero or missing weight / taxon.
#'
#' Called automatically by `estimate_catch_fao()` when the KEFS schema is
#' detected. Exposed as an internal helper for testing only.
#'
#' Note: this is a basic union; full FAO compliance for KEFS would also
#' raise the sample weight to total catch via
#' `sample_weight_raised = sample_weight * total_catch_weight / sum(sample_weight)`
#' per trip. Add this when the team confirms the sub-sampling protocol.
#'
#' @param landings_kefs Raw KEFS validated landings.
#'
#' @return Long-format tibble with `catch_kg` and `catch_taxon` ready
#'         for `estimate_catch_fao()`.
#' @keywords internal
prepare_kenya_landings <- function(landings_kefs) {

  carry <- intersect(
    c("submission_id", "gaul_1_name", "gaul_2_name", "landing_site",
      "vessel_type", "gear", "boat_name", "trip_duration",
      "landing_date", "total_catch_weight"),
    colnames(landings_kefs)
  )

  # Priority block: revenue estimated from unit price
  #   priority_revenue = priority_weight x total_price_kg
  priority <- landings_kefs |>
    dplyr::mutate(
      .priority_revenue = .data$priority_weight * .data$total_price_kg
    ) |>
    dplyr::select(dplyr::all_of(carry),
                  catch_taxon = "priority_scientific_name",
                  catch_kg    = "priority_weight",
                  catch_price = ".priority_revenue")

  # Sample block: revenue is given directly per row
  sample <- landings_kefs |>
    dplyr::select(dplyr::all_of(carry),
                  catch_taxon = "sample_scientific_name",
                  catch_kg    = "sample_weight",
                  catch_price = "sample_price")

  out <- dplyr::bind_rows(priority, sample) |>
    dplyr::filter(!is.na(.data$catch_taxon),
                  !is.na(.data$catch_kg),
                  .data$catch_kg > 0)

  logger::log_info(
    "prepare_kenya_landings: {nrow(landings_kefs)} input rows -> ",
    "{nrow(out)} long-format rows ",
    "({dplyr::n_distinct(out$submission_id)} unique trips)."
  )
  out
}


# ── 11b. GEAR MACRO-CATEGORIES & PDS ACTIVITY ────────────────────────────────

#' Default gear lookup: 23 survey gears -> 5 PDS macro-categories
#'
#' Maps the gear labels used in country landing surveys (ADNAP, KEFS, Zanzibar)
#' onto the 5 PDS macro-categories used by the Airtable `pds_devices` table:
#' `gillnet_trammel`, `hook_and_line`, `seine_encircling`, `line_and_gillnet`,
#' `other_unknown`. The macro grouping reduces the explosion of (vessel x gear)
#' combinations (e.g. ~85 in Kenya) to a handful of homogeneous fishing units,
#' which dramatically improves cell-level sample sizes and FAO pass rates.
#'
#' Override this default by passing your own tibble as `gear_lookup` to
#' [estimate_catch_fao()].
#'
#' @export
default_gear_lookup <- tibble::tribble(
  ~gear,            ~gear_class,
  "Gill Net",       "gillnet_trammel",
  "Trammel Net",    "gillnet_trammel",
  "Tangle Net",     "gillnet_trammel",
  "Hand Line",      "hook_and_line",
  "Long Line",      "hook_and_line",
  "Pole and Line",  "hook_and_line",
  "Stick Rod",      "hook_and_line",
  "Trolling Line",  "hook_and_line",
  "Dropline",       "hook_and_line",
  "Beach Seine",    "seine_encircling",
  "Purse Seine",    "seine_encircling",
  "Ring Net",       "seine_encircling",
  "Reef Seine",     "seine_encircling",
  "Seine",          "seine_encircling",
  "Trawl Net",      "seine_encircling",
  "Scoop Net",      "seine_encircling",
  "Mixed gears",    "line_and_gillnet",
  "Trap",           "other_unknown",
  "Cage Trap",      "other_unknown",
  "Cast Net",       "other_unknown",
  "Spear Gun",      "other_unknown",
  "Harpoon",        "other_unknown",
  "Gleaning",       "other_unknown",
  "Other",          "other_unknown"
)


#' Apply gear macro-categories to a landings or frame tibble
#'
#' Overwrites the gear-name column with its corresponding macro-category
#' from `gear_lookup`. Operates on a `gear` column by default, but the
#' column name is configurable (e.g. `standard_name` for the Airtable frame).
#'
#' Rows where the gear value has no match in `gear_lookup` keep the original
#' label (a warning is emitted listing the unmapped values).
#'
#' @param data        A tibble containing the gear-name column.
#' @param gear_lookup A tibble with columns `gear` and `gear_class`. Default
#'                    `default_gear_lookup`.
#' @param gear_col    Name of the column to remap. Default `"gear"`.
#'
#' @return The input tibble with `gear_col` overwritten by the macro-category.
#' @export
apply_gear_macros <- function(data,
                              gear_lookup = default_gear_lookup,
                              gear_col    = "gear") {

  if (!gear_col %in% colnames(data)) {
    logger::log_warn(
      "apply_gear_macros: column `{gear_col}` not in data — skipping."
    )
    return(data)
  }

  lookup <- gear_lookup |>
    dplyr::rename(.gear_in = "gear", .gear_out = "gear_class")

  data_out <- data |>
    dplyr::left_join(lookup, by = setNames(".gear_in", gear_col)) |>
    dplyr::mutate(
      "{gear_col}" := dplyr::coalesce(.data$.gear_out, .data[[gear_col]])
    ) |>
    dplyr::select(-".gear_out")

  unmapped <- setdiff(unique(stats::na.omit(data[[gear_col]])),
                      lookup$.gear_in)
  if (length(unmapped) > 0L) {
    logger::log_warn(
      "apply_gear_macros: {length(unmapped)} unmapped gear value(s): ",
      paste(unmapped, collapse = ", "),
      ". Original labels kept."
    )
  }

  data_out
}


#' Build an activity table from PDS trips and devices
#'
#' Computes a Boat Activity Coefficient (BAC) per gear macro-category by
#' counting how many trips a GPS-tracked vessel makes per month. By default
#' the BAC is calculated **per (gear x year_month)** — i.e. each month gets
#' its own activity estimate — which is the FAO-orthodox approach
#' (de Graaf et al. 2017, Section 3 of the toolkit: "BAC ... the probability
#' that any boat will be active on any day during the month"). Set
#' `by_period = FALSE` to fall back to a single annual mean per gear class.
#'
#' The function joins the PDS trips (which carry `IMEI` and trip
#' timestamps) with the Airtable `pds_devices` table (which carries
#' `gear class` per IMEI), aggregates trips per boat per month, then
#' computes the mean monthly trip count per (gear class [x year_month]).
#' Each unique `(vessel_type x gear_class)` combination in the landings is
#' then assigned the corresponding BAC.
#'
#' When `by_period = TRUE`, months with no PDS observations for a given
#' gear class are simply absent from the output -- the orchestrator will
#' fall back to observed days for those cells. To get a graceful fallback
#' chain (monthly -> annual -> observed), call this function twice (once
#' with `by_period = TRUE`, once with `by_period = FALSE`) and bind the
#' rows, keeping the monthly value where present.
#'
#' @param pds_trips    Tibble of GPS trips with `IMEI` and `Started` columns
#'                     (as returned by reading the `pds-trips` parquet).
#' @param pds_devices  Tibble from the Airtable `pds_devices` table with
#'                     `imei` and `gear class` columns.
#' @param landings     The landings tibble (used to derive which
#'                     `(vessel_type, gear_class)` combinations exist).
#' @param days_in_period Days in the analysis period. Default 30.
#' @param by_period    Logical. If `TRUE` (default, FAO-orthodox), the BAC
#'                     is calculated per (gear x year_month). If `FALSE`,
#'                     a single annual mean per gear class is returned.
#'
#' @return A tibble with columns `fishing_unit`, `bac`, `pab`, `ac`,
#'         `days_in_period`, and (when `by_period = TRUE`) `year_month`.
#'         Consumable by the `activity` parameter of [estimate_catch_fao()].
#' @export
build_pds_activity <- function(pds_trips,
                               pds_devices,
                               landings,
                               days_in_period = 30,
                               by_period      = TRUE) {

  # Trip counts per (boat x gear_class x year_month) from the raw PDS data.
  # Each row is one boat's monthly trip count -- the unit FAO calls "AverF".
  trips_per_boat_month <- pds_trips |>
    dplyr::mutate(
      .imei      = as.character(.data$IMEI),
      year_month = format(.data$Started, "%Y-%m")
    ) |>
    dplyr::left_join(
      pds_devices |>
        dplyr::transmute(
          .imei      = as.character(.data$imei),
          boat_name  = .data$boat_name,
          gear_class = .data$`gear class`
        ),
      by = ".imei"
    ) |>
    dplyr::filter(!is.na(.data$gear_class), !is.na(.data$boat_name)) |>
    dplyr::count(.data$boat_name, .data$gear_class, .data$year_month,
                 name = "n_trips")

  # Aggregate to BAC at the requested temporal grain.
  if (isTRUE(by_period)) {
    trips_summary <- trips_per_boat_month |>
      dplyr::group_by(.data$gear_class, .data$year_month) |>
      dplyr::summarise(
        mean_trips_per_month = mean(.data$n_trips),
        n_boats              = dplyr::n_distinct(.data$boat_name),
        .groups              = "drop"
      )
    logger::log_info(
      "build_pds_activity (by_period = TRUE, FAO-orthodox): ",
      "{nrow(trips_summary)} (gear x year_month) cells from ",
      "{dplyr::n_distinct(trips_per_boat_month$boat_name)} GPS-tracked boats."
    )
  } else {
    trips_summary <- trips_per_boat_month |>
      dplyr::group_by(.data$gear_class) |>
      dplyr::summarise(
        mean_trips_per_month = mean(.data$n_trips),
        n_boats              = dplyr::n_distinct(.data$boat_name),
        .groups              = "drop"
      )
    logger::log_info(
      "build_pds_activity (by_period = FALSE, annual mean): ",
      "{nrow(trips_summary)} gear classes from ",
      "{sum(trips_summary$n_boats)} unique GPS-tracked boats."
    )
  }

  # Unique fishing units in the landings (after gear macros have been
  # applied -- if they have, `gear` already holds the macro label).
  unique_fus <- landings |>
    dplyr::filter(!is.na(.data$vessel_type), !is.na(.data$gear)) |>
    dplyr::distinct(.data$vessel_type, .data$gear) |>
    dplyr::mutate(fishing_unit = paste(.data$vessel_type, .data$gear,
                                       sep = " | "))

  if (isTRUE(by_period)) {
    # Cartesian-expand fishing units by every year_month present in PDS,
    # then drop the (gear x month) combinations not observed by PDS.
    out <- tidyr::crossing(
      unique_fus,
      trips_summary |> dplyr::distinct(.data$year_month)
    ) |>
      dplyr::left_join(
        trips_summary |>
          dplyr::transmute(
            gear           = .data$gear_class,
            year_month     = .data$year_month,
            bac            = .data$mean_trips_per_month /
              .env$days_in_period,
            days_in_period = .env$days_in_period
          ),
        by = c("gear", "year_month")
      ) |>
      dplyr::filter(!is.na(.data$bac)) |>
      dplyr::transmute(
        fishing_unit   = .data$fishing_unit,
        year_month     = .data$year_month,
        bac            = .data$bac,
        pab            = NA_real_,
        ac             = NA_real_,
        days_in_period = .data$days_in_period
      )
  } else {
    out <- unique_fus |>
      dplyr::left_join(
        trips_summary |>
          dplyr::transmute(
            gear           = .data$gear_class,
            bac            = .data$mean_trips_per_month /
              .env$days_in_period,
            days_in_period = .env$days_in_period
          ),
        by = "gear"
      ) |>
      dplyr::filter(!is.na(.data$bac)) |>
      dplyr::transmute(
        fishing_unit   = .data$fishing_unit,
        bac            = .data$bac,
        pab            = NA_real_,
        ac             = NA_real_,
        days_in_period = .data$days_in_period
      )
  }

  out
}


# ── 12. FRAME SURVEY INTEGRATION (Airtable assets) ───────────────────────────

#' Download the Peskas Airtable assets RDS containing the frame survey
#'
#' The Airtable `assets` RDS bundled by Lorenzo's `model-fishery.R` pipeline
#' is the canonical source for the FAO frame `F_total`. This helper fetches
#' it from cloud storage and returns the full named list:
#'   `geo`, `taxa`, `gear`, `vessels`, `sites`, `forms`, `devices`, `frame`.
#'
#' The `frame` element has one row per (district x vessel-or-gear) with
#' `n_boats` (= F), `standard_name` (canonical fishing-unit label) and
#' `category_kind` ("vessel" or "gear").
#'
#' Note: uses `conf$storage$google$options` (not `options_coasts`).
#'
#' @param conf Output of `read_config()`.
#'
#' @return Named list as stored in the Airtable assets RDS.
#' @export
download_fao_frame <- function(conf) {

  logger::log_info("Downloading Airtable assets RDS for FAO frame ...")

  assets <- cloud_object_name(
    prefix    = conf$metadata$airtable$assets,
    provider  = conf$storage$google$key,
    version   = "latest",
    extension = "rds",
    options   = conf$storage$google$options
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options  = conf$storage$google$options
    ) |>
    readr::read_rds()

  if (!"frame" %in% names(assets)) {
    stop(
      "The downloaded assets RDS has no `frame` element. ",
      "Available elements: ", paste(names(assets), collapse = ", ")
    )
  }

  logger::log_info(
    "Frame loaded: {nrow(assets$frame)} rows across ",
    "{dplyr::n_distinct(assets$frame$country)} countries."
  )
  assets
}


#' Build a FAO `frame` tibble from the Airtable assets frame
#'
#' Filters `assets$frame` by `category_kind` and reshapes to the
#' (`gaul_2_name` x `fishing_unit` -> `F_total`) tibble that
#' `estimate_catch_fao()` consumes via its `frame` argument.
#'
#' Defaults to `level = "vessel"` because the Airtable frame stores
#' vessel and gear counts on separate rows -- there is no recorded
#' joint count of (vessel x gear) combinations. FAO toolkit p. 6
#' explicitly allows defining fishing units by vessel alone when gear
#' assignments are diverse or unknown.
#'
#' @param assets_frame The `frame` element of `download_fao_frame()`.
#' @param level        Either "vessel" (default), "gear", or "both".
#' @param country      Optional country filter (e.g. "Mozambique").
#'                     If `NULL`, all countries are kept.
#' @param gaul_2_filter Optional vector of `gaul_2_name` to keep.
#'                     Useful to restrict the frame to the districts
#'                     that appear in the landings being analysed.
#'
#' @return Tibble with columns `gaul_2_name`, `fishing_unit`, `F_total`.
#' @export
build_frame_table <- function(assets_frame,
                              level         = c("vessel", "gear", "both"),
                              country       = NULL,
                              gaul_2_filter = NULL,
                              gear_lookup   = NULL) {

  level <- match.arg(level)

  fr <- assets_frame
  if (!is.null(country)) {
    fr <- dplyr::filter(fr, .data$country %in% .env$country)
  }
  if (!is.null(gaul_2_filter)) {
    fr <- dplyr::filter(fr, .data$gaul_2_name %in% .env$gaul_2_filter)
  }

  fr <- switch(level,
               vessel = dplyr::filter(fr, .data$category_kind == "vessel"),
               gear   = dplyr::filter(fr, .data$category_kind == "gear"),
               both   = fr
  )

  # Apply gear macro-categories to the frame's standard_name when requested.
  # Only meaningful for level = "gear" (vessel-level frames have vessel
  # names, not gear names).
  if (!is.null(gear_lookup) && level == "gear") {
    fr <- apply_gear_macros(fr,
                            gear_lookup = gear_lookup,
                            gear_col    = "standard_name")
  }

  out <- fr |>
    dplyr::group_by(.data$gaul_2_name, .data$standard_name) |>
    dplyr::summarise(
      F_total = sum(.data$n_boats, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$F_total > 0) |>
    dplyr::rename(fishing_unit = "standard_name")

  logger::log_info(
    "build_frame_table (level = '{level}'): {nrow(out)} ",
    "(district x fishing_unit) cells from frame survey."
  )
  out
}


#' Disaggregate a vessel-level frame to (vessel x gear) using observed proportions
#'
#' The Airtable frame stores `F` separately by vessel and by gear -- it does
#' not record joint counts of (vessel x gear) combinations. This helper
#' bridges that gap by distributing each vessel's frame count across the
#' gears observed for that vessel in the landings, proportionally to trip
#' counts:
#'
#'   F(district, vessel, gear) = F_frame(district, vessel)
#'                              x p_observed(gear | district, vessel)
#'
#' The output is suitable for direct use as the `frame` argument of
#' `estimate_catch_fao()` when `fu_cols = c("vessel_type", "gear")`.
#'
#' Methodological note: the proportional split assumes that the share of
#' observed trips for each gear within a (district x vessel) group is
#' representative of the share of frame vessels using that gear. This is
#' an approximation -- some vessels may make disproportionately more trips
#' than others -- but it is dramatically better than treating
#' heterogeneous gears (e.g. trawl vs gillnet on the same motorised boat)
#' as a single fishing unit, which inflates within-cell variance and
#' destroys the relative-error estimates.
#'
#' Combinations with no observed trips are dropped (we cannot guess the
#' split). Callers may wish to apply a uniform split for unobserved gears
#' as a downstream refinement.
#'
#' @param frame      Tibble with `gaul_2_name`, `fishing_unit` (= vessel),
#'                   `F_total`. Output of `build_frame_table(level = 'vessel')`.
#' @param landings   Validated landings (raw, before `build_fishing_units`).
#' @param vessel_col Name of the vessel column in landings. Default
#'                   "vessel_type".
#' @param gear_col   Name of the gear column in landings. Default "gear".
#' @param sep        Separator between vessel and gear in the new
#'                   `fishing_unit` label. Default " | " (matches
#'                   `build_fishing_units()`).
#'
#' @return Tibble with columns `gaul_2_name`, `fishing_unit` (= vessel | gear),
#'         `F_total` (rounded to integer).
#' @export
disaggregate_frame_by_gear <- function(frame,
                                       landings,
                                       vessel_col = "vessel_type",
                                       gear_col   = "gear",
                                       sep        = " | ") {

  # Observed proportions of gear within (district x vessel)
  proportions <- landings |>
    dplyr::filter(!is.na(.data[[vessel_col]]),
                  !is.na(.data[[gear_col]])) |>
    dplyr::distinct(.data$gaul_2_name,
                    vessel_type = .data[[vessel_col]],
                    gear        = .data[[gear_col]],
                    .data$submission_id) |>
    dplyr::count(.data$gaul_2_name, .data$vessel_type, .data$gear,
                 name = "n_trips") |>
    dplyr::group_by(.data$gaul_2_name, .data$vessel_type) |>
    dplyr::mutate(prop = .data$n_trips / sum(.data$n_trips)) |>
    dplyr::ungroup()

  # Join frame (treating its `fishing_unit` as the vessel name) with
  # observed proportions, then multiply.
  out <- frame |>
    dplyr::rename(.vessel = "fishing_unit") |>
    dplyr::inner_join(
      proportions,
      by = c("gaul_2_name", ".vessel" = "vessel_type")
    ) |>
    dplyr::mutate(
      F_total      = as.integer(round(.data$F_total * .data$prop)),
      fishing_unit = paste(.data$.vessel, .data$gear, sep = sep)
    ) |>
    dplyr::filter(.data$F_total > 0) |>
    dplyr::select("gaul_2_name", "fishing_unit", "F_total")

  logger::log_info(
    "disaggregate_frame_by_gear: {nrow(frame)} (district x vessel) -> ",
    "{nrow(out)} (district x vessel x gear) cells."
  )
  out
}


#' Disaggregate a gear-level frame to (vessel x gear) using observed proportions
#'
#' Mirror of `disaggregate_frame_by_gear()` for countries whose Airtable
#' frame records F by gear rather than by vessel (e.g. Kenya).
#'
#'   F(district, vessel, gear) = F_frame(district, gear)
#'                              x p_observed(vessel | district, gear)
#'
#' @param frame      Tibble with `gaul_2_name`, `fishing_unit` (= gear),
#'                   `F_total`. Output of `build_frame_table(level = 'gear')`.
#' @param landings   Validated landings (raw, before `build_fishing_units`).
#' @param vessel_col Name of the vessel column in landings. Default
#'                   "vessel_type".
#' @param gear_col   Name of the gear column in landings. Default "gear".
#' @param sep        Separator between vessel and gear in the new
#'                   `fishing_unit` label. Default " | ".
#'
#' @return Tibble with columns `gaul_2_name`, `fishing_unit` (= vessel | gear),
#'         `F_total` (rounded to integer).
#' @export
disaggregate_frame_by_vessel <- function(frame,
                                         landings,
                                         vessel_col = "vessel_type",
                                         gear_col   = "gear",
                                         sep        = " | ") {

  proportions <- landings |>
    dplyr::filter(!is.na(.data[[vessel_col]]),
                  !is.na(.data[[gear_col]])) |>
    dplyr::distinct(.data$gaul_2_name,
                    vessel_type = .data[[vessel_col]],
                    gear        = .data[[gear_col]],
                    .data$submission_id) |>
    dplyr::count(.data$gaul_2_name, .data$vessel_type, .data$gear,
                 name = "n_trips") |>
    dplyr::group_by(.data$gaul_2_name, .data$gear) |>
    dplyr::mutate(prop = .data$n_trips / sum(.data$n_trips)) |>
    dplyr::ungroup()

  out <- frame |>
    dplyr::rename(.gear = "fishing_unit") |>
    dplyr::inner_join(
      proportions,
      by = c("gaul_2_name", ".gear" = "gear")
    ) |>
    dplyr::mutate(
      F_total      = as.integer(round(.data$F_total * .data$prop)),
      fishing_unit = paste(.data$vessel_type, .data$.gear, sep = sep)
    ) |>
    dplyr::filter(.data$F_total > 0) |>
    dplyr::select("gaul_2_name", "fishing_unit", "F_total")

  logger::log_info(
    "disaggregate_frame_by_vessel: {nrow(frame)} (district x gear) -> ",
    "{nrow(out)} (district x vessel x gear) cells."
  )
  out
}


# ── 13. TOP-LEVEL ORCHESTRATOR ───────────────────────────────────────────────

#' FAO-aligned catch & effort estimation pipeline
#'
#' End-to-end wrapper for routine country reporting. Takes validated
#' landings (and optionally a frame survey + activity table) and returns
#' the full set of FAO estimates with quality flags.
#'
#' @param landings         Validated landings. One row per species per trip.
#' @param frame            Optional tibble with frame counts (`F_total` per
#'                         minor stratum x fu). If `NULL` and no `assets`,
#'                         derived from observed boats (placeholder, with
#'                         warning).
#' @param assets           Optional Airtable assets list from
#'                         `download_fao_frame()`. When provided and
#'                         `frame` is `NULL`, the frame survey is
#'                         derived from `assets$frame` automatically.
#'                         If `fu_cols` includes both vessel and gear
#'                         (the default), the vessel-level frame is
#'                         disaggregated to (vessel x gear) using the
#'                         observed gear proportions per
#'                         (district x vessel) -- see
#'                         `disaggregate_frame_by_gear()`.
#' @param activity         Optional tibble with `bac`, `pab`, or `ac` per
#'                         minor stratum x fu x period. If `NULL`, fishing
#'                         days are taken directly from observed records.
#'                         When `pds_trips` and `pds_devices` are also
#'                         provided, the table is built automatically from
#'                         GPS tracking data — see [build_pds_activity()].
#' @param pds_trips        Optional tibble of GPS trips (from the
#'                         `pds-trips` parquet). When provided along with
#'                         `pds_devices`, an activity table is built
#'                         automatically.
#' @param pds_devices      Optional tibble from the Airtable `pds_devices`
#'                         table (with `imei` and `gear class`).
#' @param gear_lookup      Tibble mapping survey gears to PDS macro
#'                         categories. Default [default_gear_lookup].
#'                         Set to `NULL` (or `apply_macros = FALSE`) to
#'                         disable the macro-grouping.
#' @param apply_macros     Logical. If `TRUE` (default), the gear column
#'                         of landings is overwritten with the macro
#'                         category from `gear_lookup` before fishing
#'                         units are built. Reduces ~85 vessel-gear combos
#'                         to ~30 macro-units and dramatically improves
#'                         per-cell sample sizes.
#' @param hybrid_frame     Logical. If `TRUE` (default), cells not covered
#'                         by the explicit/assets frame are backfilled
#'                         with observed-boat counts, so every cell ends
#'                         up with a valid F_total.
#' @param major_stratum    Column for major stratum. Default "gaul_2_name".
#' @param minor_stratum    Column for minor stratum. Default "landing_site".
#' @param fu_cols          Columns defining the fishing unit. Default
#'                         `c("vessel_type", "gear")`. Pass just
#'                         `"vessel_type"` for a coarser (and more
#'                         FAO-orthodox) grouping that maps directly to
#'                         the Airtable frame without disaggregation.
#' @param boat_col         Vessel-identifier column. Default "boat_name".
#'                         If absent in `landings`, falls back to
#'                         `submission_id` with a warning — meaning each
#'                         submission is treated as one fishing-unit-day
#'                         and the compound RE for total catch collapses
#'                         to the CPUE RE.
#' @param metric           Either "catch" (default, weights in kg) or
#'                         "revenue" (monetary value). When "revenue",
#'                         the pipeline uses the `catch_price` column as
#'                         the per-row quantity. Output column names stay
#'                         the same (`total_catch_kg`, `mean_cpue`, ...)
#'                         but contain monetary values; the `metric` field
#'                         in the returned list states which is which.
#' @param period_col       Column with the period label (e.g. "year_month").
#'                         If absent, it's built from `landing_date`.
#' @param duration_col     Trip-duration column. Default "trip_duration".
#' @param duration_units   "hours" (default) or "days".
#' @param alpha            Significance level for RE. Default 0.10.
#' @param re_threshold     RE pass threshold. Default 0.15.
#'
#' @return A list with named tibbles:
#'   \describe{
#'     \item{metric}{"catch" or "revenue" — what the numeric outputs mean}
#'     \item{trips}{trip-level CPUE table}
#'     \item{cpue_summary}{minor stratum x fu x period CPUE summary}
#'     \item{days_summary}{minor stratum x fu x period fishing-days summary}
#'     \item{frame}{frame-survey counts used}
#'     \item{minor}{FAO catch estimates at the minor stratum level}
#'     \item{major}{FAO catch estimates aggregated to the major stratum}
#'     \item{species}{catch by species per (group x species)}
#'     \item{quality}{summary of pass/warn/fail counts}
#'   }
#' @export
estimate_catch_fao <- function(landings,
                               frame          = NULL,
                               assets         = NULL,
                               activity       = NULL,
                               pds_trips      = NULL,
                               pds_devices    = NULL,
                               gear_lookup    = default_gear_lookup,
                               apply_macros   = TRUE,
                               hybrid_frame   = TRUE,
                               metric         = c("catch", "revenue"),
                               major_stratum  = "gaul_2_name",
                               minor_stratum  = "landing_site",
                               fu_cols        = c("vessel_type", "gear"),
                               boat_col       = "boat_name",
                               period_col     = "year_month",
                               duration_col   = "trip_duration",
                               duration_units = "hours",
                               alpha          = 0.10,
                               re_threshold   = 0.15) {

  metric <- match.arg(metric)
  logger::log_info(
    "FAO aggregation: starting on {nrow(landings)} landing rows ",
    "(metric = '{metric}')"
  )

  # ── 13.0a Apply gear macro-categories to landings BEFORE everything else,
  # so that frame disaggregation, fishing-unit construction, and activity
  # joins all use the same vocabulary. Skip if user opts out or no lookup.
  use_macros <- isTRUE(apply_macros) && !is.null(gear_lookup) &&
    "gear" %in% colnames(landings)
  if (use_macros) {
    landings <- apply_gear_macros(landings,
                                  gear_lookup = gear_lookup,
                                  gear_col    = "gear")
    logger::log_info(
      "Applied gear macro-categories to landings ",
      "({nrow(gear_lookup)} mappings)."
    )
  }

  # ── 13.0b Frame survey from Airtable assets
  # Auto-detect whether the frame uses vessel or gear categories, then
  # disaggregate to (vessel x gear) fishing units using observed proportions
  # from whichever dimension is missing.
  if (!is.null(assets)) {
    if (!is.null(frame)) {
      logger::log_warn(
        "Both `frame` and `assets` were provided -- `assets` will be ignored ",
        "and the explicit `frame` used."
      )
    } else {
      if (!"frame" %in% names(assets)) {
        stop("`assets` does not contain a `frame` element.")
      }
      gaul_2_in_data <- unique(landings$gaul_2_name)
      frame_lookup <- if (use_macros) gear_lookup else NULL
      frame_vessel <- build_frame_table(
        assets$frame, level = "vessel",
        gaul_2_filter = gaul_2_in_data,
        gear_lookup   = frame_lookup
      )
      frame_gear <- build_frame_table(
        assets$frame, level = "gear",
        gaul_2_filter = gaul_2_in_data,
        gear_lookup   = frame_lookup
      )

      wants_vessel_gear <- length(fu_cols) >= 2 &&
        all(c("vessel_type", "gear") %in% fu_cols)

      if (nrow(frame_vessel) > 0 && wants_vessel_gear) {
        logger::log_info(
          "Frame is vessel-based ({nrow(frame_vessel)} cells). ",
          "Disaggregating by observed gear proportions."
        )
        frame <- disaggregate_frame_by_gear(frame_vessel, landings)

      } else if (nrow(frame_gear) > 0 && wants_vessel_gear) {
        logger::log_info(
          "Frame is gear-based ({nrow(frame_gear)} cells). ",
          "Disaggregating by observed vessel proportions."
        )
        frame <- disaggregate_frame_by_vessel(frame_gear, landings)

      } else if (nrow(frame_vessel) > 0) {
        logger::log_info("Using vessel-only frame ({nrow(frame_vessel)} cells).")
        frame <- frame_vessel

      } else if (nrow(frame_gear) > 0) {
        logger::log_info("Using gear-only frame ({nrow(frame_gear)} cells).")
        frame <- frame_gear

      } else {
        logger::log_warn(
          "No frame data found in assets for the districts in landings. ",
          "Falling back to observed-boats frame."
        )
        frame <- NULL
      }

      # Disaggregation can silently produce an empty frame when the
      # assets/landings vocabulary doesn't intersect. Treat 0-row as NULL.
      if (!is.null(frame) && nrow(frame) == 0L) {
        logger::log_warn(
          "Frame from assets is empty after disaggregation. ",
          "Falling back to observed-boats frame."
        )
        frame <- NULL
      }
    }
  }

  # ── 13.0c Auto-detect country schema and reshape to FAO long format
  is_kefs <- all(c("priority_scientific_name", "priority_weight",
                   "sample_scientific_name",   "sample_weight") %in%
                   colnames(landings))

  if (is_kefs) {
    logger::log_info(
      "Detected KEFS (Kenya) schema -- reshaping priority + sample blocks ",
      "into one row per species per trip."
    )
    landings <- prepare_kenya_landings(landings)
  }

  if (!all(c("catch_kg", "catch_taxon") %in% colnames(landings))) {
    stop(
      "Landings must contain `catch_kg` and `catch_taxon` columns. ",
      "Got: ", paste(colnames(landings), collapse = ", "),
      ". If this is a country with a different schema, add a branch ",
      "to the schema-detection block in `estimate_catch_fao()`."
    )
  }

  # ── 13.0d Revenue swap: when metric = 'revenue', overwrite catch_kg with
  # the per-row revenue value so the rest of the pipeline runs unchanged.
  # Output column names stay catch_kg / total_catch_kg / mean_cpue, but the
  # `metric` field on the result tells the caller what they actually contain.
  if (metric == "revenue") {
    if (!"catch_price" %in% colnames(landings)) {
      stop(
        "metric = 'revenue' requested but `catch_price` column not found. ",
        "Add a per-row revenue column named `catch_price` to the landings."
      )
    }
    n_swapped <- sum(!is.na(landings$catch_price) & landings$catch_price > 0)
    landings$catch_kg <- landings$catch_price
    logger::log_info(
      "Revenue mode: using `catch_price` as quantity ",
      "({n_swapped} non-zero rows). Output columns keep their `catch_kg` ",
      "names but contain monetary values."
    )
  }

  # ── 13.0e Boat column auto-fallback
  if (!boat_col %in% colnames(landings)) {
    if ("submission_id" %in% colnames(landings)) {
      logger::log_warn(
        "boat_col `", boat_col, "` not found — falling back to ",
        "`submission_id` as the unit-of-observation. Each submission ",
        "becomes one fishing-unit-day; days-variability term in the ",
        "compound RE collapses to 0. Supply a frame + activity (BAC/PAB) ",
        "table to recover the FAO total-catch RE."
      )
      boat_col <- "submission_id"
    } else {
      stop("Neither `", boat_col, "` nor `submission_id` is in landings — ",
           "cannot identify fishing units.")
    }
  }

  # ── 13.1 Build period column if missing
  if (!period_col %in% colnames(landings)) {
    if (!"landing_date" %in% colnames(landings)) {
      stop("Provide either `", period_col, "` or `landing_date` in landings.")
    }
    landings[[period_col]] <- format(as.Date(landings$landing_date), "%Y-%m")
  }

  # ── 13.2 Build fishing-unit labels
  landings <- build_fishing_units(landings, fu_cols = fu_cols)

  # ── 13.3 Trip-level CPUE
  keep <- c(major_stratum, minor_stratum, "fishing_unit",
            period_col, boat_col, "landing_date")
  trips <- compute_trip_cpue(
    landings,
    duration_col   = duration_col,
    duration_units = duration_units,
    keep_cols      = intersect(keep, colnames(landings))
  )

  # ── 13.4 Minor-stratum summaries
  group_minor <- c(major_stratum, minor_stratum, "fishing_unit", period_col)
  cpue_summary <- summarize_cpue(trips, group_by = group_minor, alpha = alpha)

  # If activity coefficients are provided, override observed days with
  # F × AC × D-derived expectation. Otherwise summarise observed days.
  days_summary <- summarize_fishing_days(
    trips,
    group_by = group_minor,
    boat_col = boat_col,
    alpha    = alpha
  )
  # If pds_trips and pds_devices are provided, derive activity automatically
  # from GPS tracking data. The result populates `activity` if not already set.
  if (!is.null(pds_trips) && !is.null(pds_devices) && is.null(activity)) {
    logger::log_info(
      "Building BAC activity table from PDS trips + Airtable devices ..."
    )
    activity <- build_pds_activity(
      pds_trips   = pds_trips,
      pds_devices = pds_devices,
      landings    = landings
    )
  }

  if (!is.null(activity)) {
    logger::log_info("Applying provided activity coefficients (BAC / PAB).")
    ac_cols <- c("bac", "pab", "ac", "days_in_period")
    join_ac <- intersect(group_minor, colnames(activity))

    # Ensure all coalesce columns exist (fill missing as NA) so the join
    # produces a tibble with all three coefficient columns available.
    activity_for_join <- activity |>
      dplyr::select(dplyr::all_of(join_ac), dplyr::any_of(ac_cols))
    for (col in c("bac", "pab", "ac", "days_in_period")) {
      if (!col %in% colnames(activity_for_join)) {
        activity_for_join[[col]] <- NA_real_
      }
    }

    days_summary <- days_summary |>
      dplyr::left_join(activity_for_join, by = join_ac) |>
      dplyr::mutate(
        ac_combined = dplyr::coalesce(.data$bac, .data$pab, .data$ac),
        D           = dplyr::coalesce(.data$days_in_period, 30),
        mean_days   = dplyr::if_else(
          is.na(.data$ac_combined),
          .data$mean_days,
          .data$ac_combined * .data$D
        )
      )
  }

  # ── 13.5 Frame (with hybrid fallback)
  # If an explicit frame is provided (or derived from assets), the join with
  # the minor-stratum estimates can leave some cells with F_total = NA when
  # the frame doesn't cover every (district x fishing_unit) combination.
  # When `hybrid_frame = TRUE`, we fill those gaps with observed-boat counts
  # aggregated to the SAME granularity as the explicit frame. This is the
  # "best of both worlds": real F from the census where available, observed
  # counts as a placeholder, joined at consistent keys.
  if (is.null(frame)) {
    frame <- derive_frame_observed(
      landings,
      group_by = c(major_stratum, minor_stratum, "fishing_unit"),
      boat_col = boat_col
    )
  } else if (isTRUE(hybrid_frame)) {
    # Match the explicit frame's granularity for the fallback
    frame_keys <- intersect(c(major_stratum, minor_stratum, "fishing_unit"),
                            colnames(frame))
    observed_frame <- derive_frame_observed(
      landings,
      group_by = frame_keys,
      boat_col = boat_col
    )
    n_before <- nrow(frame)
    frame <- dplyr::bind_rows(
      frame,
      dplyr::anti_join(observed_frame, frame, by = frame_keys)
    )
    logger::log_info(
      "Hybrid frame: {n_before} cells from real frame + ",
      "{nrow(frame) - n_before} fallback cells from observed boats ",
      "(aggregated at keys: {paste(frame_keys, collapse = ' + ')})."
    )
  }

  # ── 13.6 Minor-stratum totals + compound RE
  minor <- estimate_minor_total(
    cpue_summary  = cpue_summary,
    days_summary  = days_summary,
    frame         = frame,
    trips         = trips,
    join_keys     = group_minor,
    boat_col      = boat_col,
    alpha         = alpha
  ) |>
    flag_quality(threshold = re_threshold)

  # ── 13.7 Aggregation to major stratum
  major <- aggregate_to_major(
    minor,
    major_keys = c(major_stratum, "fishing_unit", period_col)
  ) |>
    flag_quality(threshold = re_threshold)

  # ── 13.8 Species breakdown
  species <- estimate_species_total(
    landings        = landings,
    minor_estimates = minor,
    join_keys       = group_minor
  )

  # ── 13.9 Quality summary
  quality <- minor |>
    dplyr::count(quality_overall, name = "n_groups") |>
    dplyr::mutate(pct = n_groups / sum(n_groups))

  logger::log_info(
    "FAO aggregation: {nrow(minor)} minor-stratum estimates | ",
    "{sum(minor$quality_overall == 'pass', na.rm = TRUE)} pass, ",
    "{sum(minor$quality_overall == 'warn', na.rm = TRUE)} warn, ",
    "{sum(minor$quality_overall == 'fail', na.rm = TRUE)} fail"
  )

  list(
    metric        = metric,
    trips         = trips,
    cpue_summary  = cpue_summary,
    days_summary  = days_summary,
    frame         = frame,
    minor         = minor,
    major         = major,
    species       = species,
    quality       = quality
  )
}

