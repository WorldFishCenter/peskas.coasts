# =====================================================================
# LBB — Length-Based Bayesian biomass estimator (Froese et al. 2018)
# Real Bayesian fit via JAGS, on the WCS length-weight monitoring.
#
# Estimates, per reef species, the posterior of Linf, Lc, alpha (selectivity
# slope), M/K and F/K from the catch length-frequency, and derives:
#   - B/B0    relative stock biomass (fished / unfished per-recruit biomass)
#   - F/M     relative fishing mortality
#   - Lc/Lopt selection relative to optimum
# with 95% credible intervals from the posterior.
#
# The model equations were validated in Python against the mean-length proxy
# on this dataset (corr B/B0~SPR = 0.80, F/M~F/M = 0.73).
#
# DEPENDENCIES (install in RStudio; needs internet + the JAGS system library):
#   - JAGS  : https://mcmc-jags.sourceforge.io  (Windows installer / `brew install jags`)
#   - R pkgs: install.packages(c("rjags","coda","readxl"))
#
# USAGE:
#   source("data-raw/stock_assessment_workflow.R")   # reuses cleaning helpers
#   source("data-raw/lbb_assessment.R")
#   res <- run_lbb_assessment("data-raw/Length_weight 2024_2026.xlsx")
#   # writes lbb_results.csv
# =====================================================================

# Reuse cleaning / loading helpers from the standalone workflow script.
# Source it first (same folder), e.g. source("data-raw/stock_assessment_workflow.R").
if (!exists("run_assessment")) {
  wf <- if (file.exists("data-raw/stock_assessment_workflow.R"))
    "data-raw/stock_assessment_workflow.R" else "stock_assessment_workflow.R"
  if (file.exists(wf)) source(wf) else
    stop("Please source stock_assessment_workflow.R first (it provides the cleaning helpers).")
}

# ---- LBB forward model (base R; identical to the validated Python version) ----

#' Relative numbers-at-length under the LBB equilibrium model.
#' @keywords internal
lbb_numbers <- function(Lmid, Linf, Lc, alpha, MK, FK) {
  S <- 1 / (1 + exp(-alpha * (Lmid - Lc)))
  N <- numeric(length(Lmid)); N[1] <- 1
  for (i in 2:length(Lmid)) {
    ratio <- (Linf - Lmid[i]) / (Linf - Lmid[i - 1])
    if (ratio <= 0) ratio <- 1e-6
    N[i] <- N[i - 1] * ratio^(MK + FK * S[i])
  }
  list(N = N, S = S)
}

#' Relative biomass B/B0 = fished / unfished per-recruit biomass (weight ∝ L^b).
#' @keywords internal
lbb_bb0 <- function(Lmid, Linf, Lc, alpha, MK, FK, b = 3) {
  Nf <- lbb_numbers(Lmid, Linf, Lc, alpha, MK, FK)$N
  Nu <- lbb_numbers(Lmid, Linf, Lc, alpha, MK, 0)$N
  w <- Lmid^b
  sum(Nf * w) / sum(Nu * w)
}

# ---- JAGS model ------------------------------------------------------
LBB_JAGS <- "
model {
  # priors (Froese-style: Linf informed by Lmax, M/K ~ 1.5)
  Linf  ~ dnorm(Linf_mu, Linf_tau) T(LmidMax, )
  MK    ~ dnorm(1.5, 11.111) T(0.4, 3)      # sd = 0.3
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

#' Fit LBB to one species' length vector via JAGS.
#'
#' @param L Numeric vector of measured lengths (cm), already cleaned.
#' @param bin_width Length-bin width (cm). Default 1.
#' @param n_iter,n_burn,n_chains MCMC controls.
#' @return A one-row data.frame of posterior medians and 95% CIs, or NULL.
#' @export
fit_lbb <- function(L, bin_width = 1, n_iter = 6000, n_burn = 3000, n_chains = 3) {
  if (!requireNamespace("rjags", quietly = TRUE))
    stop("Install JAGS + install.packages('rjags').")
  L <- L[is.finite(L) & L > 0]                   # guard against NA/NaN
  if (length(L) < 6) return(NULL)
  Lmax <- as.numeric(quantile(L, 0.99, type = 7))
  lo <- max(2, floor(min(L)))
  breaks <- seq(lo, ceiling(Lmax * 1.05) + bin_width, by = bin_width)
  idx <- floor((L - lo) / bin_width) + 1L
  idx <- idx[idx >= 1 & idx <= (length(breaks) - 1)]
  counts <- tabulate(idx, nbins = length(breaks) - 1)
  Lmid <- breaks[-length(breaks)] + bin_width / 2
  keep <- counts > 0
  counts <- counts[keep]; Lmid <- Lmid[keep]
  if (length(Lmid) < 6) return(NULL)

  # Linf must always exceed the largest occupied length class, otherwise
  # (Linf - Lmid[i]) goes negative and JAGS rejects pow(negative, real).
  occ_max <- max(Lmid)
  Linf_mu <- max(Lmax / 0.95, occ_max + 1)
  dat <- list(
    LF = counts, Lmid = Lmid, nL = length(Lmid), Ntot = sum(counts),
    Linf_mu = Linf_mu, Linf_tau = 1 / (0.10 * Linf_mu)^2,
    LmidMax = occ_max + 0.5,
    Lc_lo = min(Lmid), Lc_hi = Lmid[which.max(counts)] + (Linf_mu - Lmid[which.max(counts)]) * 0.5
  )
  inits <- function() list(Linf = Linf_mu, MK = 1.5, FK = 1.0,
                           Lc = Lmid[which.max(counts)], alpha = 0.4)
  fit <- tryCatch({
    m <- rjags::jags.model(textConnection(LBB_JAGS), data = dat, inits = inits,
                           n.chains = n_chains, quiet = TRUE)
    stats::update(m, n_burn)
    rjags::coda.samples(m, c("Linf", "Lc", "alpha", "MK", "FK"), n.iter = n_iter)
  }, error = function(e) { message("  LBB fit failed: ", conditionMessage(e)); NULL })
  if (is.null(fit)) return(NULL)

  dr <- as.matrix(fit)
  # derived quantities per posterior draw
  bb0 <- apply(dr, 1, function(p)
    lbb_bb0(Lmid, p["Linf"], p["Lc"], p["alpha"], p["MK"], p["FK"]))
  fm <- dr[, "FK"] / dr[, "MK"]
  lopt <- dr[, "Linf"] * 3 / (3 + dr[, "MK"])
  q <- function(x) stats::quantile(x, c(0.5, 0.025, 0.975), names = FALSE)
  Q <- function(v) { z <- q(v); z }
  bb <- Q(bb0); f <- Q(fm); li <- Q(dr[, "Linf"]); lc <- Q(dr[, "Lc"]); mk <- Q(dr[, "MK"])
  lcopt <- Q(dr[, "Lc"] / lopt)
  data.frame(
    n = length(L),
    linf = round(li[1], 1), linf_lo = round(li[2], 1), linf_hi = round(li[3], 1),
    lc = round(lc[1], 1), mk = round(mk[1], 2),
    bb0 = round(bb[1], 3), bb0_lo = round(bb[2], 3), bb0_hi = round(bb[3], 3),
    fm = round(f[1], 2), fm_lo = round(f[2], 2), fm_hi = round(f[3], 2),
    lc_lopt = round(lcopt[1], 2),
    stringsAsFactors = FALSE
  )
}

#' Run LBB across all assessable reef species and write lbb_results.csv.
#'
#' @param input Path to the WCS length-weight xlsx.
#' @param min_n Minimum measured individuals per species (default 150).
#' @return A data.frame of per-species LBB results (also written to CSV).
#' @export
run_lbb_assessment <- function(input = "data-raw/Length_weight 2024_2026.xlsx",
                               output = "lbb_results.csv", min_n = 150) {
  d <- load_wcs(input)
  d$species <- norm_name(d$species); d$family <- norm_name(d$family)
  d$site <- norm_name(d$site); d$type <- title1(norm_name(d$type))
  d <- d[!is.na(d$length_cm) & !is.na(d$weight_kg) & !is.na(d$species) & d$species != "", ]
  d <- d[d$length_cm > GLOBAL_L[1] & d$length_cm < GLOBAL_L[2] &
         d$weight_kg > GLOBAL_W[1] & d$weight_kg < GLOBAL_W[2], ]
  reef <- d[which(d$type == "Reef"), ]   # which() drops any NA logical index

  rows <- list()
  for (sp in sort(unique(reef$species))) {
    g <- reef[which(reef$species == sp), ]
    if (nrow(g) < min_n || !is_species(sp)) next
    rb <- robust_lw(g$length_cm, g$weight_kg)
    L <- g$length_cm[rb$keep]
    if (length(L) < min_n) next
    message("LBB: ", sp, " (n=", length(L), ") ...")
    r <- fit_lbb(L)
    if (is.null(r)) next
    r$species <- sp; r$family <- mode1(g$family)
    r$status <- if (r$fm > 1) "overfishing" else if (r$fm > 0.7) "borderline" else "healthy"
    rows[[length(rows) + 1]] <- r
  }
  res <- do.call(rbind, rows)
  res <- res[order(res$bb0), c("species", "family", "n", "linf", "linf_lo", "linf_hi",
    "lc", "mk", "lc_lopt", "fm", "fm_lo", "fm_hi", "bb0", "bb0_lo", "bb0_hi", "status")]
  utils::write.csv(res, output, row.names = FALSE)
  message("Wrote ", output, " with ", nrow(res), " species.")
  res
}

if (sys.nframe() == 0) {
  res <- run_lbb_assessment()
  print(res)
}
