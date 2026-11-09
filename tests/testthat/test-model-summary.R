# One-row-per-stock summary (R/model-summary.R) and the FishBase guard.

fake_results <- function() {
  sp <- tibble::tibble(
    country = "zanzibar", scale = "species", scientific_name = "Lethrinus lentjan",
    catch_taxon = "LNJ",
    gear = rep(c("all", "Hand Line", "Trap"), each = 3),
    method = rep(c("lbi", "lbb", "production"), 3),
    reference_gear = rep(c(NA, TRUE, FALSE), each = 3),
    quality = "pass", consensus = "concern (agree)", n_measured = 800,
    n_length_classes = 11L, selectivity_flag = FALSE, catch_share = 0.01,
    advice = "Borderline status: more lengths and local growth data would settle it.",
    bb0 = c(NA, 0.45, NA, NA, 0.55, NA, NA, NA, NA),
    f_m = c(NA, 0.95, NA, NA, 0.67, NA, NA, NA, NA),
    fm_lo = c(0.47, 0.47, 0.47, rep(NA, 6)), fm_hi = c(2.19, 2.19, 2.19, rep(NA, 6)),
    robust_flag = c(rep("sensitive", 3), rep(NA, 6)),
    e = c(NA, NA, 0.54, NA, NA, 0.49, NA, NA, 0.71),
    e_x = c(NA, NA, 1.08, NA, NA, 0.97, NA, NA, 1.42),
    production_band = c(NA, NA, "straddles", NA, NA, "straddles", NA, NA, "clear")
  )
  gr <- tibble::tibble(
    country = "timor", scale = "group", scientific_name = "Caesionidae",
    catch_taxon = "CAE", gear = "all", method = "group-lbi", reference_gear = NA,
    quality = "warn", consensus = "concern (agree)", n_measured = 2956,
    n_length_classes = 14L, selectivity_flag = TRUE, catch_share = 0.2,
    group_status = "overfishing signal", confidence = "low", e = 0.7, e_x = 2.4,
    production_band = "clear", advice = "x"
  )
  dplyr::bind_rows(sp, gr)
}

test_that(".kobe_status maps B/B0 and F/M to the four words", {
  expect_equal(
    .kobe_status(c(0.6, 0.6, 0.4, 0.4, NA), c(0.8, 1.5, 0.8, 1.5, 1)),
    c("healthy", "overfishing", "overfished", "overfished + overfishing", NA)
  )
})

test_that("summarise_stock_status gives one headline row per stock", {
  s <- summarise_stock_status(fake_results())
  expect_equal(nrow(s), 2)
  ll <- s[s$scientific_name == "Lethrinus lentjan", ]
  expect_equal(ll$headline_gear, "Hand Line")
  expect_equal(ll$bb0, 0.55)
  expect_equal(ll$status, "healthy")
  expect_equal(ll$e_x, 0.97)
  expect_equal(ll$robust_flag, "sensitive")   # from the pooled read
  expect_equal(ll$fm_hi, 2.19)
  expect_equal(ll$reliability, "borderline")
  cae <- s[s$scientific_name == "Caesionidae", ]
  expect_equal(cae$status, "overfishing signal")
  expect_equal(cae$reliability, "indicative")
})

test_that(".fb_retry stops instead of returning an empty FishBase result", {
  local_mocked_bindings(.fishbase_reachable = function(...) FALSE)
  expect_error(
    .fb_retry(function() tibble::tibble(), ok = function(x) nrow(x) > 0,
      what = "test", tries = 2, wait = 0),
    class = "fishbase_unavailable"
  )
  local_mocked_bindings(.fishbase_reachable = function(...) TRUE)
  out <- .fb_retry(function() tibble::tibble(), ok = function(x) nrow(x) > 0,
    what = "test", tries = 2, wait = 0)
  expect_equal(nrow(out), 0)
})

test_that("species without an LBB fit fall back to the Froese status; small samples are indicative", {
  r <- tibble::tibble(
    country = "timor", scale = "species", scientific_name = "Hyporhamphus quoyi",
    gear = "all", method = c("lbi", "lbb", "production"), quality = "warn",
    consensus = "concern (agree)", n_measured = 1168, n_length_classes = 5L,
    selectivity_flag = TRUE, catch_share = 0.08,
    p_mature = c(0.1, NA, NA), lmean_lopt = c(0.6, NA, NA),
    bb0 = NA_real_, f_m = NA_real_, e_x = c(NA, NA, 3.3), production_band = c(NA, NA, "clear")
  )
  s <- summarise_stock_status(r)
  expect_equal(s$status, "overfishing signal")
  expect_equal(s$status_basis, "Froese")
  expect_equal(s$reliability, "indicative")
  small <- dplyr::mutate(r, n_measured = 44, n_length_classes = 20L, selectivity_flag = FALSE,
    consensus = "healthy (agree)", p_mature = c(0.9, NA, NA), lmean_lopt = c(1, NA, NA))
  expect_equal(summarise_stock_status(small)$reliability, "indicative")
})
