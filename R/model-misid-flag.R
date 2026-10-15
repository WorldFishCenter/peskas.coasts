# Generated with Claude (Cowork) — review before committing.
# Misidentification / species-mixture flag for the stock-assessment framework.
#
# A single species' length sample should be one coherent distribution. When it
# is clearly bimodal AND the upper mode sits beyond the species' FishBase
# asymptotic length, the large mode is almost certainly a different (larger)
# species recorded under the same name — a data-quality problem the length data
# itself reveals (the Scarus ghobban case: a cluster of fish far above L-inf
# drags the fitted L-inf up and trips the "L-inf high vs FishBase" flag).
#
# Design notes from the Timor prototype:
#  - Dip test and BIC are over-powered at these sample sizes (thousands of
#    fish): both reject unimodality for any trivial deviation. The usable
#    signal is the STANDARDISED SEPARATION of the two fitted modes and their
#    weight balance, not a p-value.
#  - Family/genus aggregates are bimodal by construction (they are mixes of
#    species), so this flag is meaningful only for species-level (binomial)
#    taxa. Bimodality WITHIN the plausible size range is read as cohorts, not
#    misidentification.
#  - A dependency-free two-component Gaussian EM is used so the flag adds no new
#    package requirement.

#' Two-Component Gaussian Mixture by EM (base R)
#'
#' Fits a two-component univariate Gaussian mixture by expectation-maximisation.
#' Deterministic (quartile initialisation), dependency-free, intended for the
#' misidentification flag rather than general mixture modelling.
#'
#' @param x Numeric vector.
#' @param iter Maximum EM iterations. Default 200.
#' @param tol Log-likelihood convergence tolerance. Default 1e-6.
#'
#' @return A list with `mu`, `sd`, `w` (each length-2, ordered by `mu`) and the
#'   final log-likelihood `ll`.
#'
#' @keywords internal
.fit_2gauss <- function(x, iter = 200L, tol = 1e-6) {
  x <- as.numeric(x[is.finite(x)])
  mu <- as.numeric(stats::quantile(x, c(0.25, 0.75), names = FALSE))
  if (mu[1] == mu[2]) mu <- mu + c(-1, 1) * stats::sd(x) / 2
  sdv <- rep(max(stats::sd(x) / 2, 1e-3), 2)
  w <- c(0.5, 0.5)
  ll_old <- -Inf
  for (i in seq_len(iter)) {
    d1 <- w[1] * stats::dnorm(x, mu[1], sdv[1])
    d2 <- w[2] * stats::dnorm(x, mu[2], sdv[2])
    tot <- d1 + d2
    tot[tot == 0] <- .Machine$double.eps
    r1 <- d1 / tot
    r2 <- 1 - r1
    s1 <- sum(r1)
    s2 <- sum(r2)
    if (s1 < 1 || s2 < 1) break
    w <- c(s1, s2) / length(x)
    mu <- c(sum(r1 * x) / s1, sum(r2 * x) / s2)
    sdv <- c(
      sqrt(sum(r1 * (x - mu[1])^2) / s1),
      sqrt(sum(r2 * (x - mu[2])^2) / s2)
    )
    sdv[!is.finite(sdv) | sdv < 1e-3] <- 1e-3
    ll <- sum(log(tot))
    if (is.finite(ll) && abs(ll - ll_old) < tol) break
    ll_old <- ll
  }
  o <- order(mu)
  list(mu = mu[o], sd = sdv[o], w = w[o], ll = ll_old)
}

#' Flag a Possible Species Mixture / Misidentification
#'
#' Fits a two-component Gaussian mixture to a species' length sample and decides
#' whether it is meaningfully bimodal (standardised mode separation and a
#' substantial minor weight). Bimodality alone is read as cohort structure; a
#' *misidentification* is flagged only when the upper mode also exceeds the
#' species' FishBase asymptotic length, i.e. the large mode is biologically
#' implausible for the named species.
#'
#' @param lengths Numeric vector of measured lengths (cm).
#' @param priors A one-row life-history tibble (`linf`) for the named species.
#' @param min_n Minimum sample size to attempt the test. Default 200.
#' @param sep_min Minimum standardised separation `|mu2 - mu1| / (sd1 + sd2)` to
#'   call the sample bimodal. Default 1.5 (a clear valley between modes).
#' @param min_weight Minimum weight on the smaller component. Default 0.15.
#' @param oversize_frac Fraction of fish above `1.1 x linf` that, on its own,
#'   flags a misidentification (a small cluster of a larger species). Default
#'   0.01.
#'
#' @return A one-row tibble: `n_modes`, `mode_lo`, `mode_hi`, `weight_hi`,
#'   `separation`, `upper_linf` (upper mode / FishBase L-inf), `frac_oversize`
#'   (share above 1.1 x L-inf), `misid_flag`, `mixture_note`.
#'
#' @keywords workflow modeling
#' @export
flag_length_mixture <- function(
  lengths,
  priors,
  min_n = 200L,
  sep_min = 1.5,
  min_weight = 0.15,
  oversize_frac = 0.01
) {
  lengths <- lengths[is.finite(lengths) & lengths > 0]
  n <- length(lengths)
  empty <- tibble::tibble(
    n_modes = NA_integer_, mode_lo = NA_real_, mode_hi = NA_real_,
    weight_hi = NA_real_, separation = NA_real_, upper_linf = NA_real_,
    frac_oversize = NA_real_, misid_flag = FALSE, mixture_note = "insufficient n"
  )
  if (n < min_n) {
    return(empty)
  }

  fit <- tryCatch(.fit_2gauss(lengths), error = function(e) NULL)
  if (is.null(fit)) {
    return(empty)
  }

  sep <- abs(fit$mu[2] - fit$mu[1]) / (fit$sd[1] + fit$sd[2])
  minw <- min(fit$w)
  bimodal <- is.finite(sep) && sep >= sep_min && minw >= min_weight

  linf <- if (!is.null(priors$linf)) priors$linf else NA_real_
  upper_linf <- if (!is.na(linf) && linf > 0) fit$mu[2] / linf else NA_real_

  # Two misidentification patterns, tested separately:
  #  (1) a balanced second mode above Linf (two comparable species lumped);
  #  (2) a small cluster of fish well above the species' own Linf (a few
  #      individuals of a larger species / an ID error) — the pattern that
  #      inflates the LBB Linf and trips "Linf high vs FishBase". A balanced
  #      bimodal test misses (2) because that cluster carries little weight, so
  #      it is tested as a fraction above 1.1 x Linf.
  frac_oversize <- if (!is.na(linf) && linf > 0) mean(lengths > 1.1 * linf) else NA_real_
  mixture_misid <- isTRUE(bimodal && !is.na(linf) && fit$mu[2] > linf)
  oversize_misid <- isTRUE(!is.na(frac_oversize) && frac_oversize > oversize_frac)
  misid <- mixture_misid || oversize_misid

  note <- dplyr::case_when(
    mixture_misid & oversize_misid ~
      "bimodal upper mode AND oversize cluster > Linf (likely second/larger species)",
    oversize_misid ~ sprintf(
      "%.1f%% of fish > 1.1x FishBase Linf (possible larger species / ID error)",
      100 * frac_oversize
    ),
    mixture_misid ~ "bimodal; upper mode > FishBase Linf (possible second/larger species)",
    bimodal ~ "bimodal within size range (likely cohorts)",
    TRUE ~ "unimodal"
  )

  tibble::tibble(
    n_modes = if (bimodal) 2L else 1L,
    mode_lo = round(fit$mu[1], 1),
    mode_hi = round(fit$mu[2], 1),
    weight_hi = round(fit$w[2], 2),
    separation = round(sep, 2),
    upper_linf = round(upper_linf, 2),
    frac_oversize = if (is.na(frac_oversize)) NA_real_ else round(frac_oversize, 3),
    misid_flag = misid,
    mixture_note = note
  )
}
