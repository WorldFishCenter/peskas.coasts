# =====================================================================
# Length-based stock assessment workflow (WCS length-weight monitoring)
# Reproduces `stock_assessment_results.csv` exactly.
#
# INPUT : "Length_weight 2024_2026.xlsx"  (WCS/AABS individual-fish data)
# OUTPUT: "stock_assessment_results.csv"
#
# Method (data-poor proxies, per reef species with >=150 measurements):
#   1. clean + robust per-species length-weight outlier removal (3 x MAD)
#   2. provisional life-history: Linf = q99(L)/0.95 ; Lm = Binohlan-Froese ;
#      Lopt = Linf*3/(3+M/K), M/K = 1.5
#   3. Froese length indicators: Pmat, Popt, Pmega, Lmean/Lopt
#   4. fishing pressure F/M from mean length (Beverton-Holt, ratio form)
#   5. per-recruit spawning potential proxy (SPR) from F/M + empirical b
#   6. data-quality flag (pass/warn)
#
# Dependencies: base R + readxl. No internet needed.
# NOTE: life-history is provisional (sample-based). In the coasts pipeline,
#       swap Linf/Lm/M-K for FishBase priors via rfishbase.
# =====================================================================

INPUT  <- "Length_weight 2024_2026.xlsx"
OUTPUT <- "stock_assessment_results.csv"
MK        <- 1.5   # natural-mortality / growth ratio (teleost default)
MIN_N     <- 150   # minimum measured individuals per species
GLOBAL_L  <- c(2, 250)   # sane length bounds (cm)
GLOBAL_W  <- c(0, 100)   # sane weight bounds (kg)

# ---- helpers --------------------------------------------------------
# Locale-proof name normalization: keep only ASCII letters/digits, any other
# character (incl. unicode/NBSP whitespace) becomes a single space. Matches the
# Python reference and behaves identically regardless of R's locale/encoding.
norm_name <- function(x) trimws(gsub("[^A-Za-z0-9]+", " ", x))
froese_lm  <- function(linf) 10^(0.898 * log10(linf) - 0.0782)
is_species <- function(s) grepl("^[A-Z][a-z]+ [a-z]+$", s) & !grepl(" spp?$", s)
title1     <- function(x) { x <- tolower(x); paste0(toupper(substr(x, 1, 1)), substr(x, 2, nchar(x))) }
mode1      <- function(x) { ux <- x[!is.na(x)]; if (!length(ux)) return(NA_character_); names(sort(table(ux), decreasing = TRUE))[1] }

# robust log-log length-weight fit: iteratively drop |resid| > 3*MAD
robust_lw <- function(L, W) {
  lL <- log(L); lW <- log(W); keep <- rep(TRUE, length(L)); a <- 0; b <- 3
  for (i in 1:3) {
    if (sum(keep) < 10) break
    xk <- lL[keep]; yk <- lW[keep]; mx <- mean(xk); my <- mean(yk)   # closed-form OLS (deterministic)
    b <- sum((xk - mx) * (yk - my)) / sum((xk - mx)^2); a <- my - b * mx
    resid <- lW - (a + b * lL); med <- median(resid[keep])
    m <- median(abs(resid[keep] - med)) * 1.4826
    if (is.na(m) || m <= 0) break
    keep <- abs(resid - med) < 3 * m
  }
  list(keep = keep, a = a, b = b)
}

# length at first capture: first 1-cm bin on the ascending limb reaching 50% of the mode.
# Manual bincount over unit bins [i, i+1) from 0, dropping out-of-range values
# (matches numpy.histogram semantics; base hist() errors on out-of-range x).
est_lc <- function(L, linf) {
  nb <- ceiling(linf * 1.15) + 1          # number of 1-cm bins (edges 0..nb)
  idx <- floor(L)                          # 0-based bin index
  idx <- idx[idx >= 0 & idx < nb]
  if (!length(idx)) return(NA_real_)
  h <- tabulate(idx + 1L, nbins = nb)      # counts per bin 0..nb-1
  mi <- which.max(h); thr <- 0.5 * h[mi]
  for (i in 1:mi) if (h[i] >= thr) return((i - 1) + 0.5)
  (mi - 1) + 0.5
}

# per-recruit spawning potential ratio from F/M, M/K, Linf, Lm, Lc and LWR exponent b
spr_pr <- function(fm, mk, linf, lm, lc, b) {
  K <- 0.3; M <- mk * K; Fv <- fm * M
  a <- seq(0, 60 - 0.1, by = 0.1)
  L <- pmin(linf * (1 - exp(-K * a)), linf * 0.999)
  L95 <- max(lm * 1.1, lm + 0.01); mat <- 1 / (1 + exp(-log(19) * (L - lm) / (L95 - lm)))
  s95 <- max(lc * 1.1, lc + 0.01); sel <- 1 / (1 + exp(-log(19) * (L - lc) / (s95 - lc)))
  w <- L^b
  ssbr <- function(f) { Z <- M + f * sel; surv <- exp(-c(0, cumsum(Z[-length(Z)] * 0.1))); sum(surv * mat * w) * 0.1 }
  as.numeric(min(max(ssbr(Fv) / ssbr(0), 0), 1))
}

# ---- load -----------------------------------------------------------
load_wcs <- function(path) {
  if (!requireNamespace("readxl", quietly = TRUE))
    stop("Package 'readxl' is required to read the .xlsx. Install it with install.packages('readxl').")
  raw <- readxl::read_excel(path)
  names(raw) <- trimws(gsub("[\r\n]+", " ", names(raw)))
  pick <- function(pat) names(raw)[grep(pat, names(raw), ignore.case = TRUE)][1]
  data.frame(
    species   = as.character(raw[["Species"]]),
    family    = as.character(raw[["Family"]]),
    type      = as.character(raw[["TYPE"]]),
    site      = as.character(raw[[pick("Landing")]]),
    length_cm = as.numeric(raw[[pick("^Length")]]),
    weight_kg = as.numeric(raw[[pick("^Weight")]]),
    stringsAsFactors = FALSE
  )
}

# ---- assessment (takes the raw standardized frame) ------------------
run_assessment <- function(d) {
  d$species <- norm_name(d$species)
  d$family  <- norm_name(d$family)
  d$site    <- norm_name(d$site)
  d$type    <- title1(norm_name(d$type))
  d <- d[!is.na(d$length_cm) & !is.na(d$weight_kg) & !is.na(d$species) & d$species != "", ]
  d <- d[d$length_cm > GLOBAL_L[1] & d$length_cm < GLOBAL_L[2] &
         d$weight_kg > GLOBAL_W[1] & d$weight_kg < GLOBAL_W[2], ]

  reef <- d[d$type == "Reef", ]
  rows <- list()
  for (sp in sort(unique(reef$species))) {
    g <- reef[reef$species == sp, ]
    if (nrow(g) < MIN_N || !is_species(sp)) next
    L <- g$length_cm; W <- g$weight_kg
    rb <- robust_lw(L, W); n_out <- sum(!rb$keep)
    Lk <- L[rb$keep]; Wk <- W[rb$keep]; n <- length(Lk)
    if (n < MIN_N) next
    linf <- as.numeric(quantile(Lk, 0.99, type = 7)) / 0.95
    if (!(linf > 5 && linf < 150)) next
    lm <- froese_lm(linf); lopt <- linf * 3 / (3 + MK)
    loga <- rb$a; b <- rb$b
    pred <- loga + b * log(Lk)
    r2 <- 1 - sum((log(Wk) - pred)^2) / sum((log(Wk) - mean(log(Wk)))^2)
    a_g <- exp(loga) * 1000                      # LWR 'a' for weight in grams
    lwr_ok <- (r2 >= 0.7 && b >= 2.3 && b <= 3.5)
    b_eff  <- if (lwr_ok) b else 3.0
    lc <- est_lc(Lk, linf); sub <- Lk[Lk >= lc]
    lbar <- if (length(sub)) mean(sub) else mean(Lk)
    zk <- if ((lbar - lc) > 0.5) (linf - lbar) / (lbar - lc) else NA_real_
    fm <- if (!is.na(zk)) max(0, zk / MK - 1) else NA_real_
    spr <- if (!is.na(fm)) spr_pr(fm, MK, linf, lm, lc, b_eff) else NA_real_
    pmat  <- mean(Lk >= lm)
    popt  <- mean(Lk >= 0.9 * lopt & Lk <= 1.1 * lopt)
    pmega <- mean(Lk > 1.1 * lopt)
    reach <- as.numeric(quantile(Lk, 0.99, type = 7)) / linf
    q <- if (n >= 300 && reach >= 0.8) "pass" else if (n >= 150) "warn" else "fail"
    rows[[length(rows) + 1]] <- data.frame(
      species = sp, family = mode1(g$family), n = n, n_outliers = n_out,
      n_sites = length(unique(g$site)),
      linf = round(linf, 1), lm = round(lm, 1), lopt = round(lopt, 1), lc = round(lc, 1),
      lmean = round(lbar, 1), lmean_lopt = round(lbar / lopt, 3),
      p_mature = round(pmat, 3), p_opt = round(popt, 3), p_mega = round(pmega, 3),
      fm = if (is.na(fm)) NA else round(fm, 2), spr = if (is.na(spr)) NA else round(spr, 3),
      quality = q, lwr_a = round(a_g, 5), lwr_b = round(b, 3), lwr_r2 = round(r2, 3),
      lwr_ok = lwr_ok, stringsAsFactors = FALSE)
  }
  res <- do.call(rbind, rows)
  res[order(-res$n), ]
}

# ---- main -----------------------------------------------------------
if (sys.nframe() == 0) {
  d <- load_wcs(INPUT)
  res <- run_assessment(d)
  write.csv(res, OUTPUT, row.names = FALSE)
  cat("Wrote", OUTPUT, "with", nrow(res), "species\n")
}
