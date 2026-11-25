# Plain-language reasons and data-collection advice for each stock read.
#
# Turns the quality flags into actionable suggestions for the dashboard. The
# thresholds follow a subsampling test on Siganus sutor: LBB precision plateaus around
# 100-150 samples, and a status is only reliable when its uncertainty
# band does not cross the status line. A sample is a trip with measured fish:
# fish from one catch are not independent.

#' Read a Column if Present
#'
#' `tibble`'s `$` warns on absent columns; coverage tables from older callers
#' may lack the newer fields (`l_99`, `n_length_classes`).
#'
#' @param x A data frame (one row).
#' @param name Column name.
#' @param default Value when the column is absent.
#'
#' @keywords internal
.col_or <- function(x, name, default = NA) {
  if (name %in% names(x)) x[[name]] else default
}

#' Reasons Behind a Data-Quality Flag
#'
#' Lists, in plain words, which sample properties weaken an assessment. The
#' same signals feed [.flag_assessment_quality()]. It also lists stand-in or
#' contradicting FishBase values from `priors`, which don't change the flag.
#'
#' @param coverage A one-row coverage tibble from [.length_coverage()].
#' @param linf Asymptotic length (cm) used for the read.
#' @param ci_width Optional relative width of the LBB credible interval.
#' @param priors Optional one-row priors from [get_life_history_priors()].
#'
#' @return A single string of `"; "`-separated reasons (`""` when none).
#'
#' @keywords internal
.quality_reasons <- function(coverage, linf, ci_width = NA_real_, priors = NULL) {
  n <- .col_or(coverage, "n_trips_measured")
  frac <- .col_or(coverage, "frac_measured")
  l_top <- .col_or(coverage, "l_99", .col_or(coverage, "l_max"))
  reach <- if (isTRUE(linf > 0)) l_top / linf else NA_real_
  n_cls <- .col_or(coverage, "n_length_classes")
  reasons <- c(
    if (isTRUE(n_cls < 6)) sprintf("only %d length classes (lengths recorded too coarsely)", n_cls),
    if (isTRUE(n < 100)) "fish measured on few trips (< 100)",
    if (isTRUE(frac < 0.1)) "lengths on < 10% of records",
    if (isTRUE(reach < 0.8)) "no fish near maximum size (99th percentile < 80% of Linf)",
    if (isTRUE(ci_width > 0.5)) "wide credible interval",
    if (identical(.col_or(priors, "linf_source"), "max_length")) {
      "no FishBase growth study (Linf is the largest fish recorded)"
    },
    if (identical(.col_or(priors, "lm_source"), "from_linf")) {
      "no FishBase maturity study (size at maturity estimated from Linf)"
    },
    if (!is.na(.col_or(priors, "prior_conflict"))) .col_or(priors, "prior_conflict")
  )
  paste(reasons, collapse = "; ")
}

#' One Data-Collection Suggestion per Stock Read
#'
#' Picks the single most useful next step for the people collecting data,
#' in priority order: not assessable yet, possible misidentification, lengths
#' recorded too coarsely, too few trips, large fish missing, borderline status
#' (sensitive prior, band straddling the line, or methods disagreeing), mixed
#' coarse taxon, reliable.
#'
#' @param n_trips Trips with measured fish in the sample.
#' @param reasons Output of [.quality_reasons()].
#' @param n_classes Number of distinct recorded lengths.
#' @param consensus Consensus label from [.ensemble_consensus()].
#' @param misid Misidentification flag from [flag_length_mixture()].
#' @param robust_flag `"robust"` / `"sensitive"` from [lbb_robustness()].
#' @param production_band `"clear"` / `"straddles"` from [assess_production()].
#' @param gear_profile Optional one-row output of [.gear_profile()]; when the
#'   sample is dominated by dome-selective gears the "large fish missing"
#'   advice names the gear.
#' @param coarse_low_confidence Logical; a low-confidence genus/family read.
#' @param min_n Minimum trips for any read. Default 20 (species track).
#' @param target_n Recommended trips per assessment period. Default 150.
#'
#' @return A single sentence.
#'
#' @keywords internal
.stock_advice <- function(n_trips, reasons = "", n_classes = NA_integer_,
                          consensus = NA_character_, misid = NA,
                          robust_flag = NA_character_,
                          production_band = NA_character_,
                          gear_profile = NULL,
                          coarse_low_confidence = FALSE,
                          min_n = 20L, target_n = 150L) {
  n <- n_trips %||% 0
  if (is.na(n) || n < min_n) {
    return(sprintf(
      "Not assessable yet: fish measured on %d trips, at least %d needed (aim for %d).",
      as.integer(dplyr::coalesce(n, 0)), as.integer(min_n), as.integer(target_n)
    ))
  }
  if (isTRUE(misid)) {
    return("Check species identification: the length sample looks like two species.")
  }
  if (isTRUE(n_classes < 6)) {
    return(sprintf(
      "Record lengths to the nearest cm: only %d length classes, too coarse for this species.",
      as.integer(n_classes)
    ))
  }
  if (n < 100) {
    return(sprintf(
      "Measure fish on more trips: aim for %d+ per assessment period (currently %d).",
      as.integer(target_n), as.integer(n)
    ))
  }
  if (grepl("maximum size", reasons %||% "", fixed = TRUE)) {
    if (isTRUE(gear_profile$selectivity_flag)) {
      return(sprintf(
        paste(
          "Catch comes mainly from %s (%d%% of measured fish), which under-catch",
          "large fish: measure catches from other gears (e.g. hand lines) to confirm the status."
        ),
        gear_profile$main_gear, round(100 * gear_profile$main_gear_share)
      ))
    }
    return(paste(
      "Large fish are missing from the sample: measure fish of all sizes and",
      "check whether the gear (e.g. traps) excludes them."
    ))
  }
  if (identical(robust_flag, "sensitive") || identical(production_band, "straddles") ||
      identical(consensus, "mixed (methods disagree)")) {
    return("Borderline status: more lengths and local growth data would settle it.")
  }
  if (isTRUE(coarse_low_confidence)) {
    return("Mixed group with different growth: record the catch at species level where possible.")
  }
  "Reliable read: keep current sampling."
}
