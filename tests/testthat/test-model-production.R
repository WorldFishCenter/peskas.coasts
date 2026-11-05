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

test_that(".length_coverage repeats each length n_catch times", {
  land <- tibble::tibble(
    scientific_name = "Siganus sutor", trip_id = c("a", "a", "b", "c"),
    length_cm = c(20, 25, NA, 30), n_catch = c(3L, 1L, 2L, NA)
  )
  cov <- .length_coverage(land, "Siganus sutor")
  expect_equal(sort(cov$lengths), c(20, 20, 20, 25, 30))
  expect_equal(cov$coverage$n_measured, 5)
  expect_equal(cov$coverage$frac_measured, 3 / 4)
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
