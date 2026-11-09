# Generated with Claude (Cowork) — review before committing.
# Plain-language reasons and data-collection advice for each stock read.
#
# The team asked (meeting 2026-10-21) for the quality flags to become
# actionable suggestions on the dashboard. The thresholds follow the
# subsampling test on Siganus sutor (2026-10-21): LBB precision plateaus around
# 100-150 measured fish, and a status is only reliable when its uncertainty
# band does not cross the status line.

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
#' same signals feed [.flag_assessment_quality()].
#'
#' @param coverage A one-row coverage tibble from [.length_coverage()].
#' @param linf Asymptotic length (cm) used for the read.
#' @param ci_width Optional relative width of the LBB credible interval.
#'
#' @return A single string of `"; "`-separated reasons (`""` when none).
#'
#' @keywords internal
.quality_reasons <- function(coverage, linf, ci_width = NA_real_) {
  n <- .col_or(coverage, "n_measured")
  frac <- .col_or(coverage, "frac_measured")
  l_top <- .col_or(coverage, "l_99", .col_or(coverage, "l_max"))
  reach <- if (isTRUE(linf > 0)) l_top / linf else NA_real_
  n_cls <- .col_or(coverage, "n_length_classes")
  reasons <- c(
    if (isTRUE(n_cls < 6)) sprintf("only %d length classes (lengths recorded too coarsely)", n_cls),
    if (isTRUE(n < 100)) "few fish measured (n < 100)",
    if (isTRUE(frac < 0.1)) "lengths on < 10% of records",
    if (isTRUE(reach < 0.8)) "no fish near maximum size (99th percentile < 80% of Linf)",
    if (isTRUE(ci_width > 0.5)) "wide credible interval"
  )
  paste(reasons, collapse = "; ")
}

#' One Data-Collection Suggestion per Stock Read
#'
#' Picks the single most useful next step for the people collecting data,
#' in priority order: not assessable yet, possible misidentification, lengths
#' recorded too coarsely, too few fish, large fish missing, borderline status
#' (sensitive prior, band straddling the line, or methods disagreeing), mixed
#' coarse taxon, reliable.
#'
#' @param n_measured Measured fish in the sample.
#' @param reasons Output of [.quality_reasons()].
#' @param n_classes Number of distinct recorded lengths.
#' @param consensus Consensus label from [.ensemble_consensus()].
#' @param misid Misidentification flag from [flag_length_mixture()].
#' @param robust_flag `"robust"` / `"sensitive"` from [lbb_robustness()].
#' @param production_band `"clear"` / `"straddles"` from [assess_production()].
#' @param coarse_low_confidence Logical; a low-confidence genus/family read.
#' @param min_n Minimum fish for any read. Default 20 (species track).
#' @param target_n Recommended fish per assessment period. Default 150.
#'
#' @return A single sentence.
#'
#' @keywords internal
.stock_advice <- function(n_measured, reasons = "", n_classes = NA_integer_,
                          consensus = NA_character_, misid = NA,
                          robust_flag = NA_character_,
                          production_band = NA_character_,
                          coarse_low_confidence = FALSE,
                          min_n = 20L, target_n = 150L) {
  n <- n_measured %||% 0
  if (is.na(n) || n < min_n) {
    return(sprintf(
      "Not assessable yet: measure at least %d fish (currently %d).",
      as.integer(target_n), as.integer(dplyr::coalesce(n, 0))
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
      "Measure more fish: aim for %d+ per assessment period (currently %d).",
      as.integer(target_n), as.integer(n)
    ))
  }
  if (grepl("maximum size", reasons %||% "", fixed = TRUE)) {
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
