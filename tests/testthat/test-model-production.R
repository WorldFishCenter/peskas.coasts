# Production-based exploitation layer (R/model-production.R).

# Equilibrium lengths with known Z/K: fish enter full selection at lc, die at a
# constant rate Z and grow by von Bertalanffy, so length is
# L = linf - (linf - lc) * exp(-K * age_since_lc), with age ~ Exp(Z).
sim_lengths <- function(n, linf, lc, zk, k = 0.5) {
  age <- stats::rexp(n, rate = zk * k)
  linf - (linf - lc) * exp(-k * age)
}

test_that(".bh_zk recovers a known Z/K", {
  set.seed(1)
  len <- sim_lengths(20000, linf = 40, lc = 15, zk = 3)
  expect_equal(.bh_zk(len, linf = 40, lc = 15)$zk, 3, tolerance = 0.05)
})

test_that(".bh_zk returns NA when not estimable", {
  expect_true(is.na(.bh_zk(1:10, linf = 40)$zk))
  expect_true(is.na(.bh_zk(rep(30, 50), linf = NA)$zk))
})

test_that(".e_from_zk follows E = 1 - M/Z and floors at 0", {
  expect_equal(.e_from_zk(4, 1), 0.75)
  expect_equal(.e_from_zk(1, 1.5), 0)
  expect_true(is.na(.e_from_zk(NA, 1.5)))
})

test_that(".harvest_fraction uses x by trophic level, conservative when unknown", {
  expect_equal(.harvest_fraction(c(2.1, 3.9, 4.0, 4.5, NA)), c(0.25, 0.25, 0.5, 0.5, 0.25))
})

test_that("estimate_exploitation combines BH and LBB with a band", {
  set.seed(2)
  len <- sim_lengths(5000, linf = 40, lc = 15, zk = 4.5)
  pr <- tibble::tibble(linf = 40, mk = 1.5)
  ex <- estimate_exploitation(len, pr, fm = 2, fm_lo = 1.5, fm_hi = 3)
  expect_equal(ex$e_lbb, 2 / 3)
  expect_true(ex$e_lo <= ex$e && ex$e <= ex$e_hi)
  expect_equal(ex$e_methods, "bh+lbb")
})

test_that("assess_production flags over-exploitation of a low trophic level", {
  set.seed(3)
  len <- sim_lengths(5000, linf = 40, lc = 15, zk = 6)   # E ~ 0.75
  pr <- tibble::tibble(linf = 40, mk = 1.5)
  out <- assess_production(len, pr, trophic_level = 2.2)
  expect_equal(out$method, "production")
  expect_equal(out$x, 0.25)
  expect_gt(out$e_x, 1.5)
  expect_equal(out$production_status, "well above sustainable share")
  expect_equal(out$sy_ratio, out$x / out$e)
})

test_that("assess_production reads lightly fished stocks as within share", {
  set.seed(4)
  len <- sim_lengths(5000, linf = 40, lc = 15, zk = 1.7)  # E ~ 0.12
  out <- assess_production(len, tibble::tibble(linf = 40, mk = 1.5), trophic_level = 4.2)
  expect_equal(out$production_status, "within sustainable share")
  expect_gt(out$sy_ratio, 1)
})

test_that("scale_sustainable_yield converts ratios to kg", {
  res <- tibble::tibble(country = "kenya", catch_share = 0.1, sy_ratio = 0.5)
  out <- scale_sustainable_yield(res, tibble::tibble(country = "kenya", total_catch_kg = 1e6))
  expect_equal(out$current_catch_kg, 1e5)
  expect_equal(out$sustainable_yield_kg, 5e4)
})

test_that(".length_resolution detects length classes", {
  expect_equal(.length_resolution(c(12.5, 17.5, 17.5, 22.5, 27.5, 32.5)), 5)
  expect_lt(.length_resolution(c(20.13, 20.27, 20.41, 20.55, 21.9)), 1)
  expect_true(is.na(.length_resolution(c(10, 10))))
})

test_that(".length_coverage repeats each length n_fish times", {
  land <- tibble::tibble(
    scientific_name = "Siganus sutor", trip_id = c("a", "a", "b", "c"),
    length_cm = c(20, 25, NA, 30), n_fish = c(3L, 1L, 2L, NA),
    n_catch = c(1L, 2L, 1L, 1L)
  )
  cov <- .length_coverage(land, "Siganus sutor")
  expect_equal(sort(cov$lengths), c(20, 20, 20, 25, 30))
  expect_equal(cov$coverage$n_measured, 5)
  expect_equal(cov$coverage$frac_measured, 3 / 4)
  # n_catch is the catch's place on the form: a feed without n_fish counts each row once
  cov <- .length_coverage(dplyr::select(land, -"n_fish"), "Siganus sutor")
  expect_equal(sort(cov$lengths), c(20, 25, 30))
})

test_that(".length_coverage counts catches, not length rows", {
  land <- tibble::tibble(
    scientific_name = "Siganus sutor", trip_id = c("a", "a", "b"), n_catch = 1L,
    length_cm = c(20, 25, NA), n_fish = c(2L, 3L, NA)
  )
  cov <- .length_coverage(land, "Siganus sutor")$coverage
  expect_equal(cov$n_records, 2)
  expect_equal(cov$frac_measured, 1 / 2)
  expect_equal(cov$n_measured, 5)
})

test_that(".length_coverage measures size only on the lengths kept", {
  land <- tibble::tibble(
    scientific_name = "Siganus sutor", trip_id = c("a", "b", "c"),
    length_cm = c(20, 25, 300), n_fish = NA_integer_, n_catch = 1L
  )
  cov <- .length_coverage(land, "Siganus sutor", linf = 40)
  expect_equal(cov$lengths, c(20, 25))
  expect_equal(cov$coverage$l_max, 25)
  expect_equal(cov$coverage$n_length_classes, 2)
  expect_equal(cov$coverage$n_trips_measured, 2)
})

test_that(".bh_zk uses the lower class limit as Lc for classed lengths", {
  len <- rep(c(17.5, 22.5, 27.5, 32.5), times = c(10, 50, 30, 10))
  out <- .bh_zk(len, linf = 40)
  expect_equal(out$lc, 20)
  lmean <- mean(len[len >= 20])
  expect_equal(out$zk, (40 - lmean) / (lmean - 20))
})

test_that(".m_then2015 matches the published growth-based estimator", {
  expect_equal(.m_then2015(0.34, 52), 4.118 * 0.34^0.73 * 52^-0.33)
  expect_gt(.m_then2015(0.5, 30), .m_then2015(0.2, 80))
})

test_that(".ensemble_consensus uses up to three legs", {
  expect_equal(.ensemble_consensus(0.4, 2, 0.2, 0.7, e_x = 1.5)$consensus, "concern (agree)")
  expect_equal(.ensemble_consensus(0.8, 0.5, 0.8, 1.0, e_x = 0.5)$consensus, "healthy (agree)")
  mixed <- .ensemble_consensus(0.8, 0.5, 0.8, 1.0, e_x = 2)
  expect_equal(mixed$consensus, "mixed (methods disagree)")
  expect_equal(c(mixed$n_concern, mixed$n_methods), c(1L, 3L))
  expect_equal(.ensemble_consensus(NA, NA, 0.2, 0.7)$consensus, "concern (1 method)")
  expect_equal(.ensemble_consensus(NA, NA, NA, NA)$consensus, "unknown")
})

test_that(".stock_advice gives the highest-priority suggestion", {
  expect_match(.stock_advice(9), "Not assessable")
  expect_match(.stock_advice(300, misid = TRUE), "species identification")
  expect_match(.stock_advice(60), "more trips")
  expect_match(.stock_advice(300, reasons = "no fish near maximum size (largest < 80% of Linf)"),
    "Large fish")
  expect_match(.stock_advice(300, robust_flag = "sensitive"), "Borderline")
  expect_match(.stock_advice(300, production_band = "straddles"), "Borderline")
  expect_match(.stock_advice(300, coarse_low_confidence = TRUE), "species level")
  expect_match(.stock_advice(300, robust_flag = "robust", production_band = "clear"), "Reliable")
})

test_that(".quality_reasons explains a weak sample", {
  cov <- tibble::tibble(n_trips_measured = 50, frac_measured = 0.05, l_max = 20)
  r <- .quality_reasons(cov, linf = 40, ci_width = 0.7)
  expect_match(r, "few trips")
  expect_match(r, "10% of records")
  expect_match(r, "maximum size")
  expect_match(r, "credible interval")
  expect_equal(.quality_reasons(tibble::tibble(n_trips_measured = 500, frac_measured = 0.5, l_max = 38), 40), "")
})

test_that(".quality_reasons names stand-in and contradicting FishBase values", {
  cov <- tibble::tibble(n_trips_measured = 500, frac_measured = 0.5, l_max = 38)
  stand_in <- tibble::tibble(linf_source = "max_length", lm_source = "from_linf", prior_conflict = NA_character_)
  r <- .quality_reasons(cov, 40, priors = stand_in)
  expect_match(r, "no FishBase growth study")
  expect_match(r, "no FishBase maturity study")
  conflict <- tibble::tibble(
    linf_source = "growth_studies", lm_source = "maturity_studies",
    prior_conflict = "best size below size at maturity"
  )
  expect_equal(.quality_reasons(cov, 40, priors = conflict), "best size below size at maturity")
})

test_that(".stock_advice flags coarse length classes and method disagreement", {
  expect_match(.stock_advice(1000, n_classes = 4), "nearest cm")
  expect_match(.stock_advice(300, n_classes = 30, consensus = "mixed (methods disagree)"), "Borderline")
  cov <- tibble::tibble(n_trips_measured = 500, frac_measured = 0.5, l_max = 85, l_99 = 32, n_length_classes = 4)
  r <- .quality_reasons(cov, linf = 54.9)
  expect_match(r, "4 length classes")
  expect_match(r, "maximum size")
})

test_that("lbb_robustness adds the catch-implied Linf to the grid", {
  fake_engine <- function(lengths, priors) list(bb0 = 20 / priors$linf, fm = 1.5 / priors$mk)
  set.seed(5)
  len <- runif(300, 15, 33)
  rb <- lbb_robustness(len, tibble::tibble(linf = 55, mk = 1.5, lm = 30), fake_engine)
  expect_equal(rb$n_fits, 12L)
  expect_equal(rb$linf_catch, unname(stats::quantile(len, 0.99)) / 0.95)
  expect_gt(rb$linf_ratio, 1.5)
  # bb0 = 20 / linf: 0.31 at 1.15 x 55, ~0.57 at the catch-implied Linf -> class flips
  expect_equal(rb$robust_flag, "sensitive")
  rb9 <- lbb_robustness(len, tibble::tibble(linf = 55, mk = 1.5, lm = 30), fake_engine,
    catch_linf = FALSE)
  expect_equal(rb9$n_fits, 9L)
})

test_that("assess_species_stock gives the LBB engine the trips as sample size", {
  set.seed(6)
  land <- tibble::tibble(
    scientific_name = "Siganus sutor", trip_id = rep(paste0("t", 1:30), each = 2),
    n_catch = 1L, length_cm = round(runif(60, 15, 35)), n_fish = 10L
  )
  got <- NULL
  eng <- function(lengths, priors, n_eff) {
    got <<- n_eff
    list(bb0 = 0.5, fm = 1)
  }
  pr <- tibble::tibble(scientific_name = "Siganus sutor", linf = 40, mk = 1.5, lm = 25)
  out <- assess_species_stock(land, "Siganus sutor", pr, methods = "lbb",
    lbb_engine = eng, production = FALSE)
  expect_equal(got, 30)
  expect_equal(unique(out$n_trips_measured), 30)
  expect_equal(unique(out$n_measured), 600)
})

test_that("lbb_engine_jags widens the interval when the sample size is the trips", {
  skip_if_not_installed("rjags")
  set.seed(7)
  len <- rep(round(sim_lengths(400, linf = 40, lc = 15, zk = 3)), 50)
  pr <- tibble::tibble(linf = 40, mk = 1.5)
  all_fish <- lbb_engine_jags(len, pr, n_iter = 1000, n_burn = 500)
  trips <- lbb_engine_jags(len, pr, n_eff = 400, n_iter = 1000, n_burn = 500)
  expect_gt(trips$ci_width, 1.5 * all_fish$ci_width)
})
