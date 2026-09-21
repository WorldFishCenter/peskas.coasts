# Generated with Claude (Cowork) — review before committing.
# Length-Based Bayesian (LBB, Froese et al. 2018) fitting engine for the
# stock-assessment framework. Plugs into assess_stock_lbb() via the
# `lbb_engine` hook: a function of (lengths, priors) returning the list that
# assess_stock_lbb() expects (bb0, bbmsy, fm, lc_lopt, ci_width).
#
# The L-infinity prior is centred on the FishBase value carried in `priors`
# (from get_life_history_priors()), so the Bayesian fit is anchored to known
# biology rather than letting L-inf run free on a single, often truncated,
# length sample. Requires JAGS (system library) + rjags.

# ---- LBB forward model (base R) --------------------------------------------

#' Relative numbers-at-length under the LBB equilibrium model
#' @keywords internal
.lbb_numbers <- function(lmid, linf, lc, alpha, mk, fk) {
  s <- 1 / (1 + exp(-alpha * (lmid - lc)))
  n <- numeric(length(lmid))
  n[1] <- 1
  for (i in 2:length(lmid)) {
    ratio <- (linf - lmid[i]) / (linf - lmid[i - 1])
    if (ratio <= 0) ratio <- 1e-6
    n[i] <- n[i - 1] * ratio^(mk + fk * s[i])
  }
  list(n = n, s = s)
}

#' Relative biomass B/B0 = fished / unfished per-recruit biomass (weight prop L^b)
#' @keywords internal
.lbb_bb0 <- function(lmid, linf, lc, alpha, mk, fk, b = 3) {
  nf <- .lbb_numbers(lmid, linf, lc, alpha, mk, fk)$n
  nu <- .lbb_numbers(lmid, linf, lc, alpha, mk, 0)$n
  w <- lmid^b
  sum(nf * w) / sum(nu * w)
}

# JAGS model. L-inf and M/K priors are informative and passed in as data
# (Linf_mu/tau centred on FishBase; MK_mu/tau centred on the species' M/K).
.LBB_JAGS <- "
model {
  Linf  ~ dnorm(Linf_mu, Linf_tau) T(LmidMax, )
  MK    ~ dnorm(MK_mu, MK_tau) T(0.4, 3)
  FK    ~ dunif(0, 8)
  Lc    ~ dunif(Lc_lo, Lc_hi)
  alpha ~ dunif(0.05, 3)

  N[1] <- 1
  S[1] <- 1 / (1 + exp(-alpha * (Lmid[1] - Lc)))
  for (i in 2:nL) {
    S[i]     <- 1 / (1 + exp(-alpha * (Lmid[i] - Lc)))
    ratio[i] <- (Linf - Lmid[i]) / (Linf - Lmid[i - 1])
    N[i]     <- N[i - 1] * pow(ratio[i], MK + FK * S[i])
  }
  for (i in 1:nL) { C[i] <- N[i] * S[i] }
  Csum <- sum(C[])
  for (i in 1:nL) { p[i] <- C[i] / Csum }
  LF[1:nL] ~ dmulti(p[1:nL], Ntot)
}
"

#' LBB Fitting Engine (JAGS)
#'
#' Fits the Length-Based Bayesian biomass model (Froese et al. 2018) to one
#' species' length sample via JAGS/MCMC, anchoring the asymptotic-length prior
#' on the FishBase value in `priors`. Returns the summary list that
#' [assess_stock_lbb()] consumes, or `NULL` when JAGS is unavailable, the
#' sample is too small, or the fit fails — so the orchestrator degrades to the
#' indicator and SPR methods rather than erroring.
#'
#' @param lengths Numeric vector of measured lengths (cm), already cleaned by
#'   [.clean_lengths()].
#' @param priors A one-row life-history tibble from [get_life_history_priors()]
#'   (`linf`, `mk`, `lm`).
#' @param bin_width Length-bin width (cm). Default 1.
#' @param n_iter,n_burn,n_chains MCMC controls.
#' @param linf_sd_frac Prior SD on L-inf as a fraction of its centre. Default
#'   0.10; the data can still pull L-inf off the FishBase value.
#' @param bmsy_b0 Assumed Bmsy/B0 used to convert B/B0 to B/Bmsy. Default 0.5,
#'   the LBB convention.
#'
#' @return A list with `bb0`, `bbmsy`, `fm`, `lc_lopt`, `ci_width` (width of the
#'   95% B/B0 credible interval) and the interval bounds `bb0_lo`/`bb0_hi`, or
#'   `NULL`.
#'
#' @keywords workflow modeling
#' @export
lbb_engine_jags <- function(
  lengths,
  priors,
  bin_width = 1,
  n_iter = 6000,
  n_burn = 3000,
  n_chains = 3,
  linf_sd_frac = 0.10,
  bmsy_b0 = 0.5
) {
  if (!requireNamespace("rjags", quietly = TRUE)) {
    logger::log_warn("rjags/JAGS not available; skipping LBB fit.")
    return(NULL)
  }
  lengths <- lengths[is.finite(lengths) & lengths > 0]
  if (length(lengths) < 20) {
    return(NULL)
  }

  lmax <- as.numeric(stats::quantile(lengths, 0.99, type = 7))
  lo <- max(2, floor(min(lengths)))
  breaks <- seq(lo, ceiling(lmax * 1.05) + bin_width, by = bin_width)
  idx <- floor((lengths - lo) / bin_width) + 1L
  idx <- idx[idx >= 1 & idx <= (length(breaks) - 1)]
  counts <- tabulate(idx, nbins = length(breaks) - 1)
  lmid <- breaks[-length(breaks)] + bin_width / 2
  keep <- counts > 0
  counts <- counts[keep]
  lmid <- lmid[keep]
  if (length(lmid) < 6) {
    return(NULL)
  }

  # L-inf prior centre: FishBase value, but always above the largest occupied
  # length class (else (Linf - Lmid) goes negative and JAGS rejects pow()).
  occ_max <- max(lmid)
  linf_fb <- if (!is.null(priors$linf) && !is.na(priors$linf) && priors$linf > 0) {
    priors$linf
  } else {
    lmax / 0.95
  }
  linf_mu <- max(linf_fb, occ_max + 1)
  mk_mu <- if (!is.null(priors$mk) && !is.na(priors$mk) && priors$mk > 0) {
    priors$mk
  } else {
    1.5
  }
  mode_lc <- lmid[which.max(counts)]

  dat <- list(
    LF = counts, Lmid = lmid, nL = length(lmid), Ntot = sum(counts),
    Linf_mu = linf_mu, Linf_tau = 1 / (linf_sd_frac * linf_mu)^2,
    MK_mu = mk_mu, MK_tau = 1 / 0.3^2,
    LmidMax = occ_max + 0.5,
    Lc_lo = min(lmid), Lc_hi = mode_lc + (linf_mu - mode_lc) * 0.5
  )
  inits <- function() {
    list(Linf = linf_mu, MK = mk_mu, FK = 1.0, Lc = mode_lc, alpha = 0.4)
  }

  fit <- tryCatch(
    {
      m <- rjags::jags.model(
        textConnection(.LBB_JAGS), data = dat, inits = inits,
        n.chains = n_chains, quiet = TRUE
      )
      stats::update(m, n_burn)
      rjags::coda.samples(m, c("Linf", "Lc", "alpha", "MK", "FK"), n.iter = n_iter)
    },
    error = function(e) {
      logger::log_warn("LBB JAGS fit failed: {conditionMessage(e)}")
      NULL
    }
  )
  if (is.null(fit)) {
    return(NULL)
  }

  dr <- as.matrix(fit)
  bb0 <- apply(dr, 1, function(p) {
    .lbb_bb0(lmid, p[["Linf"]], p[["Lc"]], p[["alpha"]], p[["MK"]], p[["FK"]])
  })
  fm <- dr[, "FK"] / dr[, "MK"]
  lopt <- dr[, "Linf"] * 3 / (3 + dr[, "MK"])
  lc_lopt <- dr[, "Lc"] / lopt
  bb <- stats::quantile(bb0, c(0.5, 0.025, 0.975), names = FALSE)

  list(
    bb0 = bb[1],
    bbmsy = bb[1] / bmsy_b0,
    fm = stats::median(fm),
    lc_lopt = stats::median(lc_lopt),
    ci_width = bb[3] - bb[2],
    bb0_lo = bb[2],
    bb0_hi = bb[3]
  )
}
