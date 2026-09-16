# =====================================================================
# FishBase-informed L-infinity priors for LBB  (add-on to lbb_assessment.R)
#
# WHY: LBB estimates L-inf from the catch alone. When the catch is truncated,
# or when a handful of over-sized / mis-identified fish sit in the tail, the
# free L-inf runs away (e.g. Lutjanus bengalensis -> 91 cm for a 30 cm snapper),
# which then inflates depletion (B/B0) and fishing pressure (F/M). Anchoring
# L-inf to FishBase fixes this — and, symmetrically, corrects species whose
# catch is truncated well below their true size (which had looked "healthy").
#
# USAGE (in RStudio, with internet for rfishbase):
#   source("data-raw/stock_assessment_workflow.R")
#   source("data-raw/lbb_assessment.R")            # provides fit_lbb(), etc.
#   source("data-raw/lbb_fishbase_priors.R")       # this file (overrides below)
#   res <- run_lbb_assessment_fb("data-raw/Length_weight 2024_2026.xlsx")
#   # writes lbb_results_fishbase.csv, with FishBase priors + a QA flag column.
#
# DEPENDENCIES: install.packages("rfishbase")  (optional — a curated fallback
# table is built in, so the code still runs offline / if rfishbase is down).
# =====================================================================

# ---- curated fallback: FishBase max length (cm) + type, fetched Sep 2026 ----
# Used only when rfishbase is unavailable or returns nothing for a species.
FB_LMAX_FALLBACK <- rbind(
  data.frame(species = "Lutjanus bengalensis",       lmax = 30.0, type = "TL"),
  data.frame(species = "Acanthurus triostegus",      lmax = 27.0, type = "TL"),
  data.frame(species = "Scarus ghobban",             lmax = 75.0, type = "TL"),
  data.frame(species = "Cheilinus trilobatus",       lmax = 45.0, type = "TL"),
  data.frame(species = "Scolopsis ghanam",           lmax = 30.0, type = "TL"),
  data.frame(species = "Halichoeres hortulanus",     lmax = 27.0, type = "TL"),
  data.frame(species = "Scolopsis bimaculata",       lmax = 31.0, type = "TL"),
  data.frame(species = "Lutjanus gibbus",            lmax = 56.8, type = "FL"),
  data.frame(species = "Lethrinus rubrioperculatus", lmax = 57.0, type = "FL"),
  data.frame(species = "Siganus sutor",              lmax = 45.0, type = "SL"),
  data.frame(species = "Lethrinus mahsena",          lmax = 70.0, type = "TL"),
  data.frame(species = "Lethrinus lentjan",          lmax = 52.0, type = "FL"),
  data.frame(species = "Lethrinus harak",            lmax = 50.0, type = "TL"),
  data.frame(species = "Lethrinus nebulosus",        lmax = 87.0, type = "TL"),
  data.frame(species = "Lethrinus microdon",         lmax = 70.0, type = "TL"),
  data.frame(species = "Lethrinus borbonicus",       lmax = 45.0, type = "TL"),
  data.frame(species = "Lethrinus variegatus",       lmax = 20.0, type = "TL"),
  data.frame(species = "Leptoscarus vaigiensis",     lmax = 35.0, type = "TL"),
  data.frame(species = "Calotomus carolinus",        lmax = 33.0, type = "TL"),
  data.frame(species = "Lutjanus fulviflamma",       lmax = 25.0, type = "TL"),
  data.frame(species = "Parupeneus barberinus",      lmax = 60.0, type = "TL"),
  data.frame(species = "Parupeneus macronemus",      lmax = 40.0, type = "TL"),
  data.frame(species = "Gerres oyena",               lmax = 30.0, type = "TL"),
  data.frame(species = "Epinephelus fasciatus",      lmax = 40.0, type = "TL"),
  data.frame(species = "Cheilio inermis",            lmax = 50.0, type = "TL"),
  data.frame(species = "Acanthurus nigrofuscus",     lmax = 21.0, type = "TL"),
  data.frame(species = "Plectorhinchus flavomaculatus", lmax = 60.0, type = "TL"),
  data.frame(species = "Plectorhinchus gaterinus",   lmax = 45.0, type = "TL"),
  stringsAsFactors = FALSE
)

# Froese's empirical Lmax -> Linf : log10(Linf) = 0.044 + 0.9841 * log10(Lmax)
froese_linf_from_lmax <- function(lmax) 10^(0.044 + 0.9841 * log10(lmax))

# rough length-type harmonisation to TL (our catch lengths are TL)
to_TL <- function(lmax, type) {
  f <- switch(type, "TL" = 1.00, "FL" = 1.02, "SL" = 1.20, 1.00)
  lmax * f
}

#' FishBase asymptotic length (cm, ~TL) for one species.
#' Tries rfishbase popgrowth (median Loo), then species() max length, then the
#' curated fallback. Returns list(linf, lmax, source).
fishbase_linf <- function(sp) {
  linf <- NA_real_; lmax <- NA_real_; src <- "fallback"
  if (requireNamespace("rfishbase", quietly = TRUE)) {
    pg <- tryCatch(rfishbase::popgrowth(sp), error = function(e) NULL)
    if (!is.null(pg) && "Loo" %in% names(pg)) {
      loo <- suppressWarnings(as.numeric(pg$Loo)); loo <- loo[is.finite(loo) & loo > 0]
      if (length(loo)) { linf <- stats::median(loo); src <- "rfishbase:popgrowth" }
    }
    if (is.na(linf)) {
      spt <- tryCatch(rfishbase::species(sp), error = function(e) NULL)
      if (!is.null(spt) && "Length" %in% names(spt)) {
        lm <- suppressWarnings(as.numeric(spt$Length[1]))
        if (is.finite(lm) && lm > 0) { lmax <- lm; linf <- froese_linf_from_lmax(lm); src <- "rfishbase:species" }
      }
    }
  }
  if (is.na(linf)) {                              # curated fallback
    hit <- FB_LMAX_FALLBACK[FB_LMAX_FALLBACK$species == sp, ]
    if (nrow(hit)) { lmax <- to_TL(hit$lmax[1], hit$type[1]); linf <- froese_linf_from_lmax(lmax) }
  }
  list(linf = linf, lmax = lmax, source = src)
}

# ---- fit_lbb with an external L-inf prior (+ optional data cap) --------------
# Same JAGS model as fit_lbb(); only the L-inf prior centre/sd and an optional
# cap that drops fish physically impossible for the species differ.
fit_lbb_fb <- function(L, linf_prior = NULL, linf_sd_frac = 0.08, lmax_cap = NULL,
                       bin_width = 1, n_iter = 6000, n_burn = 3000, n_chains = 3) {
  if (!requireNamespace("rjags", quietly = TRUE))
    stop("Install JAGS + install.packages('rjags').")
  L <- L[is.finite(L) & L > 0]
  n_removed <- 0L
  if (!is.null(lmax_cap)) { keep0 <- L <= lmax_cap * 1.05; n_removed <- sum(!keep0); L <- L[keep0] }
  if (length(L) < 6) return(NULL)
  Lmax <- as.numeric(quantile(L, 0.99, type = 7))
  lo <- max(2, floor(min(L)))
  breaks <- seq(lo, ceiling(Lmax * 1.05) + bin_width, by = bin_width)
  idx <- floor((L - lo) / bin_width) + 1L
  idx <- idx[idx >= 1 & idx <= (length(breaks) - 1)]
  counts <- tabulate(idx, nbins = length(breaks) - 1)
  Lmid <- breaks[-length(breaks)] + bin_width / 2
  keep <- counts > 0; counts <- counts[keep]; Lmid <- Lmid[keep]
  if (length(Lmid) < 6) return(NULL)

  occ_max <- max(Lmid)
  # prior centre: FishBase value if given, else sample-based; always feasible
  base_mu <- if (!is.null(linf_prior) && is.finite(linf_prior)) linf_prior else Lmax / 0.95
  Linf_mu <- max(base_mu, occ_max + 1)
  dat <- list(
    LF = counts, Lmid = Lmid, nL = length(Lmid), Ntot = sum(counts),
    Linf_mu = Linf_mu, Linf_tau = 1 / (linf_sd_frac * Linf_mu)^2,
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
  }, error = function(e) { message("  LBB(fb) fit failed: ", conditionMessage(e)); NULL })
  if (is.null(fit)) return(NULL)

  dr <- as.matrix(fit)
  bb0 <- apply(dr, 1, function(p) lbb_bb0(Lmid, p["Linf"], p["Lc"], p["alpha"], p["MK"], p["FK"]))
  fm <- dr[, "FK"] / dr[, "MK"]; lopt <- dr[, "Linf"] * 3 / (3 + dr[, "MK"])
  Q <- function(v) stats::quantile(v, c(0.5, 0.025, 0.975), names = FALSE)
  bb <- Q(bb0); f <- Q(fm); li <- Q(dr[, "Linf"]); lc <- Q(dr[, "Lc"]); mk <- Q(dr[, "MK"])
  data.frame(
    n = length(L), n_removed = as.integer(n_removed),
    linf = round(li[1], 1), linf_lo = round(li[2], 1), linf_hi = round(li[3], 1),
    lc = round(lc[1], 1), mk = round(mk[1], 2),
    bb0 = round(bb[1], 3), bb0_lo = round(bb[2], 3), bb0_hi = round(bb[3], 3),
    fm = round(f[1], 2), fm_lo = round(f[2], 2), fm_hi = round(f[3], 2),
    lc_lopt = round(Q(dr[, "Lc"] / lopt)[1], 2),
    stringsAsFactors = FALSE
  )
}

#' Run LBB with FishBase L-inf priors across all assessable reef species.
#' Writes lbb_results_fishbase.csv with a data-quality flag per species.
run_lbb_assessment_fb <- function(input = "data-raw/Length_weight 2024_2026.xlsx",
                                  output = "lbb_results_fishbase.csv", min_n = 150,
                                  clean_impossible = TRUE) {
  d <- load_wcs(input)
  d$species <- norm_name(d$species); d$family <- norm_name(d$family)
  d$site <- norm_name(d$site); d$type <- title1(norm_name(d$type))
  d <- d[!is.na(d$length_cm) & !is.na(d$weight_kg) & !is.na(d$species) & d$species != "", ]
  d <- d[d$length_cm > GLOBAL_L[1] & d$length_cm < GLOBAL_L[2] &
         d$weight_kg > GLOBAL_W[1] & d$weight_kg < GLOBAL_W[2], ]
  reef <- d[which(d$type == "Reef"), ]

  rows <- list()
  for (sp in sort(unique(reef$species))) {
    g <- reef[which(reef$species == sp), ]
    if (nrow(g) < min_n || !is_species(sp)) next
    rb <- robust_lw(g$length_cm, g$weight_kg)
    L <- g$length_cm[rb$keep]
    if (length(L) < min_n) next
    fb <- fishbase_linf(sp)
    if (is.na(fb$linf)) { message("no FishBase L-inf for ", sp, " — skipping FB prior"); next }
    lmax_cap <- if (clean_impossible && is.finite(fb$lmax)) fb$lmax else NULL
    message("LBB(fb): ", sp, " (n=", length(L), ", FishBase Linf~", round(fb$linf, 1), ") ...")
    r <- fit_lbb_fb(L, linf_prior = fb$linf, lmax_cap = lmax_cap)
    if (is.null(r)) next
    r$species <- sp; r$family <- mode1(g$family)
    r$linf_fb <- round(fb$linf, 1); r$linf_src <- fb$source
    r$lmax_obs <- round(max(L), 1)
    # QA flag: how far the *unconstrained* catch would push L-inf vs FishBase
    r$linf_over_fb <- round(max(L) / fb$linf, 2)          # >1.1 => oversized fish present
    r$flag <- if (r$n_removed > 0 || r$linf_over_fb > 1.1) "check-data"
              else if (r$fm_hi - r$fm_lo > 3) "wide-CI" else "ok"
    r$status <- if (r$fm > 1) "overfishing" else if (r$fm > 0.7) "borderline" else "healthy"
    rows[[length(rows) + 1]] <- r
  }
  res <- do.call(rbind, rows)
  cols <- c("species","family","n","n_removed","linf","linf_lo","linf_hi","linf_fb","linf_src",
            "lmax_obs","linf_over_fb","lc","mk","lc_lopt","fm","fm_lo","fm_hi",
            "bb0","bb0_lo","bb0_hi","status","flag")
  res <- res[order(res$bb0), cols]
  utils::write.csv(res, output, row.names = FALSE)
  message("Wrote ", output, " with ", nrow(res), " species.")
  res
}
