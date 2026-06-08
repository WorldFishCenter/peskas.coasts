# Statistical estimation engine for the FAO catch & effort pipeline.
#
# Stage helpers consumed by estimate_catch_fao() (in aggregate-fao.R):
# trip-level CPUE, minor-stratum summaries, frame-from-observed,
# minor-total + compound RE, species breakdown, aggregation to major
# stratum, and final quality flags.

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
build_fishing_units <- function(
  data,
  fu_cols = c("vessel_type", "gear"),
  sep = " | "
) {
  fu_cols_present <- intersect(fu_cols, colnames(data))
  if (length(fu_cols_present) == 0L) {
    stop(
      "None of the fishing-unit columns ",
      paste(fu_cols, collapse = ", "),
      " are present in the data."
    )
  }

  if (length(fu_cols_present) < length(fu_cols)) {
    logger::log_warn(
      "Missing fishing-unit columns: ",
      paste(setdiff(fu_cols, fu_cols_present), collapse = ", "),
      ". Using ",
      paste(fu_cols_present, collapse = " + "),
      " only."
    )
  }

  fu_mat <- as.matrix(data[, fu_cols_present, drop = FALSE])
  fu <- apply(fu_mat, 1L, function(row) {
    if (any(is.na(row) | row == "")) {
      NA_character_
    } else {
      paste(row, collapse = sep)
    }
  })

  data |>
    dplyr::mutate(fishing_unit = fu) |>
    dplyr::relocate(
      fishing_unit,
      .after = dplyr::all_of(fu_cols_present[length(fu_cols_present)])
    )
}


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
compute_trip_cpue <- function(
  landings,
  trip_id_col = "submission_id",
  duration_col = "trip_duration",
  duration_units = c("hours", "days"),
  catch_col = "catch_kg",
  keep_cols = c("gaul_2_name", "landing_site", "fishing_unit", "landing_date")
) {
  duration_units <- match.arg(duration_units)
  keep_cols <- intersect(keep_cols, colnames(landings))

  trips <- landings |>
    dplyr::filter(
      !is.na(.data[[trip_id_col]]),
      !is.na(.data[[duration_col]]),
      .data[[duration_col]] > 0,
      !is.na(.data[[catch_col]])
    ) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(trip_id_col, keep_cols)))) |>
    dplyr::summarise(
      total_catch_kg = sum(.data[[catch_col]], na.rm = TRUE),
      trip_duration = dplyr::first(.data[[duration_col]]),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      fishing_days = if (duration_units == "hours") {
        trip_duration / 24
      } else {
        trip_duration
      },
      fishing_days = pmax(fishing_days, 1 / 24), # floor at 1 hour
      cpue_kg_day = total_catch_kg / fishing_days
    ) |>
    dplyr::filter(is.finite(cpue_kg_day), cpue_kg_day >= 0)

  logger::log_info(
    "Trip-level CPUE: {nrow(trips)} trips | ",
    "median CPUE = {round(stats::median(trips$cpue_kg_day, na.rm = TRUE), 1)} kg/day"
  )
  trips
}


#' Mean CPUE and relative error per minor stratum × fu × period
#'
#' @param trips      Output of `compute_trip_cpue()`.
#' @param group_by   Character vector of grouping columns.
#' @param cpue_col   CPUE column. Default "cpue_kg_day".
#' @param alpha      For RE. Default 0.10.
#'
#' @return Tibble with `mean_cpue`, `sd_cpue`, `n_cpue`, `re_cpue`.
#' @export
summarize_cpue <- function(
  trips,
  group_by = c("gaul_2_name", "landing_site", "fishing_unit", "year_month"),
  cpue_col = "cpue_kg_day",
  alpha = 0.10
) {
  group_by <- intersect(group_by, colnames(trips))

  trips |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      mean_cpue = mean(.data[[cpue_col]], na.rm = TRUE),
      sd_cpue = stats::sd(.data[[cpue_col]], na.rm = TRUE),
      n_cpue = sum(!is.na(.data[[cpue_col]])),
      re_cpue = relative_error_t(.data[[cpue_col]], alpha = alpha),
      .groups = "drop"
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
summarize_fishing_days <- function(
  trips,
  group_by = c("gaul_2_name", "landing_site", "fishing_unit", "year_month"),
  boat_col = "boat_name",
  date_col = "landing_date",
  alpha = 0.10
) {
  group_by <- intersect(group_by, colnames(trips))

  per_boat <- trips |>
    dplyr::filter(!is.na(.data[[boat_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(group_by, boat_col)))) |>
    dplyr::summarise(
      days_sampled = dplyr::n_distinct(as.Date(.data[[date_col]])),
      .groups = "drop"
    )

  per_boat |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      mean_days = mean(days_sampled, na.rm = TRUE),
      sd_days = stats::sd(days_sampled, na.rm = TRUE),
      n_boats_sampled = dplyr::n(),
      re_days = relative_error_t(days_sampled, alpha = alpha),
      .groups = "drop"
    )
}


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
derive_frame_observed <- function(
  landings,
  group_by = c("gaul_2_name", "landing_site", "fishing_unit"),
  boat_col = "boat_name"
) {
  logger::log_warn(
    "Deriving frame F from observed boats — placeholder. ",
    "Replace with frame-survey or vessel-register counts when available."
  )

  group_by <- intersect(group_by, colnames(landings))

  landings |>
    dplyr::filter(!is.na(.data[[boat_col]]), !is.na(fishing_unit)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      F_total = dplyr::n_distinct(.data[[boat_col]]),
      .groups = "drop"
    )
}


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
estimate_minor_total <- function(
  cpue_summary,
  days_summary,
  frame,
  trips,
  join_keys = c("gaul_2_name", "landing_site", "fishing_unit", "year_month"),
  boat_col = "boat_name",
  date_col = "landing_date",
  alpha = 0.10
) {
  join_keys <- intersect(join_keys, colnames(cpue_summary))
  frame_keys <- intersect(join_keys, colnames(frame))

  est <- cpue_summary |>
    dplyr::inner_join(days_summary, by = join_keys) |>
    dplyr::left_join(frame, by = frame_keys) |>
    dplyr::mutate(
      total_catch_kg = F_total * mean_days * mean_cpue
    )

  # Compound RE — done per group with raw vectors. Days-per-unit needs a
  # vessel identifier; if missing, we fall back to a 1-day-per-trip vector
  # and the compound RE collapses to the CPUE RE.
  has_boats <- !is.null(boat_col) && boat_col %in% colnames(trips)
  if (!has_boats) {
    logger::log_warn(
      "estimate_minor_total: no vessel identifier (`",
      boat_col,
      "`) in trips. ",
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
      .groups = "drop"
    ) |>
    dplyr::left_join(
      dplyr::select(est, dplyr::all_of(join_keys), F_total),
      by = join_keys
    ) |>
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
estimate_species_total <- function(
  landings,
  minor_estimates,
  join_keys = c("gaul_2_name", "landing_site", "fishing_unit", "year_month"),
  species_col = "catch_taxon",
  catch_col = "catch_kg"
) {
  join_keys <- intersect(join_keys, colnames(minor_estimates))

  proportions <- landings |>
    dplyr::filter(!is.na(.data[[species_col]]), !is.na(.data[[catch_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(join_keys, species_col)))) |>
    dplyr::summarise(
      species_kg = sum(.data[[catch_col]], na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(join_keys))) |>
    dplyr::mutate(proportion = species_kg / sum(species_kg)) |>
    dplyr::ungroup()

  minor_estimates |>
    dplyr::select(dplyr::all_of(join_keys), total_catch_kg) |>
    dplyr::inner_join(proportions, by = join_keys) |>
    dplyr::mutate(total_catch_kg_species = total_catch_kg * proportion)
}


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
aggregate_to_major <- function(
  minor_estimates,
  major_keys = c("gaul_2_name", "fishing_unit", "year_month")
) {
  major_keys <- intersect(major_keys, colnames(minor_estimates))

  minor_estimates |>
    dplyr::filter(!is.na(total_catch_kg)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(major_keys))) |>
    dplyr::summarise(
      total_catch_kg = sum(total_catch_kg, na.rm = TRUE),
      F_total = sum(F_total, na.rm = TRUE),
      n_minor_strata = dplyr::n(),
      # Quadrature combine of CL_i = RE_i x catch_i, then RE_total = CL / total.
      # If ALL minor strata have NA re, the aggregate must also be NA (not 0).
      re_total_catch = dplyr::if_else(
        all(is.na(re_total_catch)),
        NA_real_,
        sqrt(sum((re_total_catch * total_catch_kg)^2, na.rm = TRUE)) /
          sum(total_catch_kg, na.rm = TRUE)
      ),
      mean_cpue_weighted = stats::weighted.mean(
        mean_cpue,
        w = F_total * mean_days,
        na.rm = TRUE
      ),
      .groups = "drop"
    )
}


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
flag_quality <- function(estimates, threshold = 0.15, warn_at = 0.20) {
  classify <- function(x) {
    dplyr::case_when(
      is.na(x) ~ "unknown",
      x == 0 ~ "unknown", # exact zero = degenerate (n<2 or NA propagation)
      x <= threshold ~ "pass",
      x <= warn_at ~ "warn",
      TRUE ~ "fail"
    )
  }

  out <- estimates
  if ("re_cpue" %in% colnames(out)) {
    out$quality_cpue <- classify(out$re_cpue)
  }
  if ("re_total_catch" %in% colnames(out)) {
    out$quality_catch <- classify(out$re_total_catch)
  }

  if (all(c("quality_cpue", "quality_catch") %in% colnames(out))) {
    levels_q <- c("unknown", "pass", "warn", "fail")
    r_cpue <- match(out$quality_cpue, levels_q, nomatch = 1L)
    r_catch <- match(out$quality_catch, levels_q, nomatch = 1L)
    out$quality_overall <- levels_q[pmax(r_cpue, r_catch)]
  }
  out
}
