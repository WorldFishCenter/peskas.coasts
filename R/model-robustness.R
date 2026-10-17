# Generated with Claude (Cowork) — review before committing.
# Prior-robustness band for the LBB stock estimate.
#
# The 2026-09-15 sensitivity work showed LBB's B/B0 and F/M move with the L-inf
# and M/K priors (over-estimated L-inf overstates depletion/F; under-estimated
# L-inf overstates health). This makes that routine: re-fit LBB over a small
# grid of prior multipliers and report the resulting band plus a stability flag
# — does the *status category* hold across plausible priors, or flip? A single
# number the status is robust to is worth far more than one that isn't.

#' Three-Class Stock Status from Relative Biomass
#' @keywords internal
.stock_class <- function(bb0) {
  dplyr::case_when(
    is.na(bb0) ~ NA_character_,
    bb0 < 0.3 ~ "depleted",
    bb0 < 0.5 ~ "moderate",
    TRUE ~ "healthy"
  )
}

#' Prior-Robustness Band for an LBB Fit
#'
#' Re-fits the LBB engine across a grid of L-inf and M/K prior multipliers and
#' summarises how much the estimate moves. Reports the central estimate, the
#' B/B0 and F/M bands (min–max across the grid), and a `robust_flag`: `"robust"`
#' when the biomass status class and the over/under-fishing verdict are
#' unchanged across every grid point, `"sensitive"` when either flips.
#'
#' @param lengths Numeric vector of measured lengths (cm), already cleaned.
#' @param priors A one-row life-history tibble (`linf`, `mk`, `lm`).
#' @param lbb_engine An LBB fitting routine (see [assess_stock_lbb()]). Required.
#' @param linf_mult Multipliers applied to the L-inf prior centre. Default
#'   `c(0.85, 1, 1.15)` (±15%).
#' @param mk_mult Multipliers applied to the M/K prior centre. Default
#'   `c(0.75, 1, 1.25)`.
#'
#' @return A one-row tibble: `bb0_center`, `bb0_lo`, `bb0_hi`, `fm_center`,
#'   `fm_lo`, `fm_hi`, `n_fits`, `status_set` (distinct biomass classes seen)
#'   and `robust_flag`.
#'
#' @keywords workflow modeling
#' @export
lbb_robustness <- function(
  lengths,
  priors,
  lbb_engine,
  linf_mult = c(0.85, 1, 1.15),
  mk_mult = c(0.75, 1, 1.25)
) {
  na_row <- tibble::tibble(
    bb0_center = NA_real_, bb0_lo = NA_real_, bb0_hi = NA_real_,
    fm_center = NA_real_, fm_lo = NA_real_, fm_hi = NA_real_,
    n_fits = 0L, status_set = NA_character_, robust_flag = NA_character_
  )
  if (is.null(lbb_engine) || nrow(priors) == 0 ||
      is.na(priors$linf) || length(lengths) < 20) {
    return(na_row)
  }

  grid <- expand.grid(lm = linf_mult, mm = mk_mult)
  mk0 <- if (!is.null(priors$mk) && !is.na(priors$mk) && priors$mk > 0) priors$mk else 1.5

  fits <- purrr::pmap_dfr(grid, function(lm, mm) {
    pr <- priors
    pr$linf <- priors$linf * lm
    pr$mk <- mk0 * mm
    f <- tryCatch(lbb_engine(lengths, pr), error = function(e) NULL)
    if (is.null(f)) {
      return(tibble::tibble())
    }
    tibble::tibble(
      linf_mult = lm, mk_mult = mm,
      bb0 = f$bb0 %||% NA_real_, fm = f$fm %||% NA_real_
    )
  })
  if (nrow(fits) == 0) {
    return(na_row)
  }

  center <- fits |> dplyr::filter(.data$linf_mult == 1, .data$mk_mult == 1)
  bb0_c <- if (nrow(center) > 0) center$bb0[1] else stats::median(fits$bb0, na.rm = TRUE)
  fm_c <- if (nrow(center) > 0) center$fm[1] else stats::median(fits$fm, na.rm = TRUE)

  classes <- unique(stats::na.omit(.stock_class(fits$bb0)))
  over <- unique(stats::na.omit(fits$fm > 1))
  robust <- (length(classes) <= 1) && (length(over) <= 1)

  tibble::tibble(
    bb0_center = bb0_c,
    bb0_lo = min(fits$bb0, na.rm = TRUE),
    bb0_hi = max(fits$bb0, na.rm = TRUE),
    fm_center = fm_c,
    fm_lo = min(fits$fm, na.rm = TRUE),
    fm_hi = max(fits$fm, na.rm = TRUE),
    n_fits = nrow(fits),
    status_set = paste(classes, collapse = "/"),
    robust_flag = if (robust) "robust" else "sensitive"
  )
}

#' Ensemble Consensus Across LBB and the Froese Length Indicators
#'
#' Collapses the two length-based methods to a single concern/healthy verdict
#' and reports whether they agree. LBB speaks to biomass and fishing pressure
#' (B/B0, F/M); the Froese indicators speak to size structure (share mature,
#' mean length vs optimal). Agreement raises confidence; disagreement is itself
#' a useful flag.
#'
#' @param bb0,fm LBB relative biomass and F/M.
#' @param p_mature,lmean_lopt Froese indicators.
#'
#' @return A list: `consensus` (character) and `agree` (logical or `NA`).
#'
#' @keywords internal
.ensemble_consensus <- function(bb0, fm, p_mature, lmean_lopt) {
  lbb_known <- !is.na(bb0) || !is.na(fm)
  lbi_known <- !is.na(p_mature) || !is.na(lmean_lopt)
  lbb_concern <- isTRUE(bb0 < 0.5) || isTRUE(fm > 1)
  lbi_concern <- isTRUE(p_mature < 0.5) || isTRUE(lmean_lopt < 0.9)

  if (!lbb_known && !lbi_known) {
    return(list(consensus = "unknown", agree = NA))
  }
  if (!lbb_known || !lbi_known) {
    v <- if (lbb_known) lbb_concern else lbi_concern
    return(list(
      consensus = if (v) "concern (1 method)" else "healthy (1 method)",
      agree = NA
    ))
  }
  if (lbb_concern == lbi_concern) {
    list(
      consensus = if (lbb_concern) "concern (agree)" else "healthy (agree)",
      agree = TRUE
    )
  } else {
    list(consensus = "mixed (methods disagree)", agree = FALSE)
  }
}
