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
#' Re-fits the LBB engine across a grid of L-inf and M/K priors and summarises
#' how much the estimate moves. Reports the central estimate, the B/B0 and F/M
#' bands (min–max across the grid), and a `robust_flag`: `"robust"` when the
#' biomass status class and the over/under-fishing verdict are unchanged across
#' every grid point, `"sensitive"` when either flips.
#'
#' The L-inf axis spans the FishBase centre +/- 15\% and, when it falls
#' outside that range, the catch-implied value `L99 / 0.95`. FishBase L-inf
#' values for these stocks often sit far above the largest fish ever landed
#' (Kenya Siganus sutor 54.9 cm vs L99 32.7 cm), so a +/- 15\% band alone
#' understates the real prior uncertainty; the catch-implied value is the
#' opposite (optimistic, if the gear truncates large fish) bound.
#'
#' @param lengths Numeric vector of measured lengths (cm), already cleaned.
#' @param priors A one-row life-history tibble (`linf`, `mk`, `lm`).
#' @param lbb_engine An LBB fitting routine (see [assess_stock_lbb()]). Required.
#' @param linf_mult Multipliers applied to the L-inf prior centre. Default
#'   `c(0.85, 1, 1.15)`.
#' @param mk_mult Multipliers applied to the M/K prior centre. Default
#'   `c(0.75, 1, 1.25)`.
#' @param catch_linf Logical. Add the catch-implied L-inf (`L99 / 0.95`) to
#'   the grid when it lies outside the multiplier range. Default `TRUE`.
#'
#' @return A one-row tibble: `bb0_center`, `bb0_lo`, `bb0_hi`, `fm_center`,
#'   `fm_lo`, `fm_hi`, `n_fits`, `status_set` (distinct biomass classes seen),
#'   `robust_flag`, `linf_catch` (the catch-implied L-inf) and `linf_ratio`
#'   (FishBase L-inf / catch-implied L-inf; well above 1 means the prior sits far
#'   above anything landed).
#'
#' @keywords workflow modeling
#' @export
lbb_robustness <- function(
  lengths,
  priors,
  lbb_engine,
  linf_mult = c(0.85, 1, 1.15),
  mk_mult = c(0.75, 1, 1.25),
  catch_linf = TRUE
) {
  na_row <- tibble::tibble(
    bb0_center = NA_real_, bb0_lo = NA_real_, bb0_hi = NA_real_,
    fm_center = NA_real_, fm_lo = NA_real_, fm_hi = NA_real_,
    n_fits = 0L, status_set = NA_character_, robust_flag = NA_character_,
    linf_catch = NA_real_, linf_ratio = NA_real_
  )
  if (is.null(lbb_engine) || nrow(priors) == 0 ||
      is.na(priors$linf) || length(lengths) < 20) {
    return(na_row)
  }

  linf0 <- priors$linf
  linf_c <- as.numeric(stats::quantile(lengths, 0.99, type = 7)) / 0.95
  linf_values <- linf0 * linf_mult
  if (isTRUE(catch_linf) &&
      (linf_c < min(linf_values) || linf_c > max(linf_values))) {
    linf_values <- c(linf_values, linf_c)
  }
  mk0 <- if (!is.null(priors$mk) && !is.na(priors$mk) && priors$mk > 0) priors$mk else 1.5

  grid <- expand.grid(linf = linf_values, mk = mk0 * mk_mult)
  fits <- purrr::pmap_dfr(grid, function(linf, mk) {
    pr <- priors
    pr$linf <- linf
    pr$mk <- mk
    f <- tryCatch(lbb_engine(lengths, pr), error = function(e) NULL)
    if (is.null(f)) {
      return(tibble::tibble())
    }
    tibble::tibble(
      linf = linf, mk = mk,
      bb0 = f$bb0 %||% NA_real_, fm = f$fm %||% NA_real_
    )
  })
  if (nrow(fits) == 0) {
    return(na_row)
  }

  center <- fits |>
    dplyr::filter(abs(.data$linf - linf0) < 1e-8, abs(.data$mk - mk0) < 1e-8)
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
    robust_flag = if (robust) "robust" else "sensitive",
    linf_catch = linf_c,
    linf_ratio = linf0 / linf_c
  )
}

#' Ensemble Consensus Across the Length-Based Methods
#'
#' Collapses up to three independent reads to a concern/healthy verdict and
#' reports whether they agree: LBB (biomass and pressure: B/B0 < 0.5 or
#' F/M > 1), the Froese indicators (size structure: share mature < 0.5 or
#' Lmean/Lopt < 0.9) and the production layer (exploitation: E/x > 1, i.e.
#' fishing takes more than its sustainable share of production). Agreement
#' raises confidence; disagreement is itself a useful flag.
#'
#' @param bb0,fm LBB relative biomass and F/M.
#' @param p_mature,lmean_lopt Froese indicators.
#' @param e_x Production-layer E/x. Default `NA` (leg not used).
#'
#' @return A list: `consensus` (character), `agree` (logical or `NA`),
#'   `n_concern` and `n_methods` (integers).
#'
#' @keywords internal
.ensemble_consensus <- function(bb0, fm, p_mature, lmean_lopt, e_x = NA_real_) {
  known <- c(
    lbb = !is.na(bb0) || !is.na(fm),
    lbi = !is.na(p_mature) || !is.na(lmean_lopt),
    production = !is.na(e_x)
  )
  concern <- c(
    lbb = isTRUE(bb0 < 0.5) || isTRUE(fm > 1),
    lbi = isTRUE(p_mature < 0.5) || isTRUE(lmean_lopt < 0.9),
    production = isTRUE(e_x > 1)
  )[known]
  n <- length(concern)

  verdict <- if (n == 0) {
    list(consensus = "unknown", agree = NA)
  } else if (n == 1) {
    list(
      consensus = if (concern) "concern (1 method)" else "healthy (1 method)",
      agree = NA
    )
  } else if (all(concern)) {
    list(consensus = "concern (agree)", agree = TRUE)
  } else if (!any(concern)) {
    list(consensus = "healthy (agree)", agree = TRUE)
  } else {
    list(consensus = "mixed (methods disagree)", agree = FALSE)
  }
  c(verdict, list(n_concern = as.integer(sum(concern)), n_methods = as.integer(n)))
}
