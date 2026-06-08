# Helpers for the FAO statistical aggregation pipeline.
#
# Pure numerical / statistical utilities used by estimate_catch_fao()
# and exposed for direct use when building activity tables by hand.

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
  if (n < 2L) {
    return(NA_real_)
  }

  mean_x <- mean(x)
  if (mean_x == 0) {
    return(NA_real_)
  }

  sd_x <- stats::sd(x)
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

  n_c <- length(cpue_vec)
  n_d <- length(days_vec)
  if (n_c < 2L || n_d < 2L) {
    return(NA_real_)
  }
  if (is.na(F_total) || F_total <= 0) {
    return(NA_real_)
  }

  m_c <- mean(cpue_vec)
  s_c <- stats::sd(cpue_vec)
  m_d <- mean(days_vec)
  s_d <- stats::sd(days_vec)
  if (m_c == 0 || m_d == 0) {
    return(NA_real_)
  }

  cl_c <- stats::qt(1 - alpha / 2, df = n_c - 1L) * s_c / sqrt(n_c)
  cl_d <- stats::qt(1 - alpha / 2, df = n_d - 1L) * s_d / sqrt(n_d)

  mean_catch <- F_total * m_d * m_c
  max_catch <- F_total * (m_d + cl_d) * (m_c + cl_c)

  abs((max_catch - mean_catch) / mean_catch)
}


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
compute_bac <- function(
  obs,
  group_by,
  active = "n_active",
  examined = "n_examined"
) {
  obs |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      n_active = sum(.data[[active]], na.rm = TRUE),
      n_examined = sum(.data[[examined]], na.rm = TRUE),
      bac = dplyr::if_else(n_examined > 0, n_active / n_examined, NA_real_),
      .groups = "drop"
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
  (pmin(pmax(today, 0L), 1L) +
    pmin(pmax(yesterday, 0L), 1L) +
    pmin(pmax(before_yesterday, 0L), 1L) +
    pmin(pmax(last_week, 0L), 7L)) /
    10
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
derive_activity_observed <- function(
  landings,
  group_by,
  boat_col = "boat_name",
  date_col = "landing_date",
  days_in_period = 30L
) {
  logger::log_warn(
    "Deriving activity coefficient from observed landings — ",
    "this is a placeholder. Replace with BAC or PAB data when available."
  )

  landings |>
    dplyr::filter(!is.na(.data[[boat_col]]), !is.na(.data[[date_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_by))) |>
    dplyr::summarise(
      n_unique_boats = dplyr::n_distinct(.data[[boat_col]]),
      n_active_days = dplyr::n_distinct(as.Date(.data[[date_col]])),
      ac_observed = pmin(n_active_days / days_in_period, 1),
      .groups = "drop"
    )
}
