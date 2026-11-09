# Gear resolution helpers (R/model-gear.R).

fake_land <- function() {
  tibble::tibble(
    scientific_name = "Siganus sutor",
    trip_id = paste0("t", 1:6),
    gear = c("Trap", "Trap", "Trap", "Hand Line", "Hand Line", NA),
    length_cm = c(20, 22, 25, 30, 35, NA),
    n_catch = c(50L, 50L, 20L, 60L, 50L, 1L)
  )
}

test_that(".gear_profile weights by n_catch and flags dome-selective gears", {
  gp <- .gear_profile(fake_land(), "Siganus sutor")
  expect_equal(gp$main_gear, "Trap")
  expect_equal(gp$main_gear_share, 120 / 230)
  expect_equal(gp$selective_gear_share, 120 / 230)
  expect_false(gp$selectivity_flag)
  trap_only <- dplyr::filter(fake_land(), gear == "Trap")
  expect_true(.gear_profile(trap_only, "Siganus sutor")$selectivity_flag)
})

test_that(".gears_with_fish needs two qualifying gears", {
  expect_setequal(.gears_with_fish(fake_land(), "Siganus sutor", min_fish = 100), c("Trap", "Hand Line"))
  expect_length(.gears_with_fish(fake_land(), "Siganus sutor", min_fish = 115), 0)
})

test_that(".mark_reference_gear picks the gear reaching closest to Linf", {
  res <- tibble::tibble(
    country = "zanzibar", scale = "species", scientific_name = "Siganus sutor",
    gear = c("all", "Trap", "Hand Line"), size_reach = c(0.7, 0.6, 0.85),
    quality = c("pass", "pass", "warn")
  )
  out <- .mark_reference_gear(res)
  expect_equal(out$reference_gear, c(NA, FALSE, TRUE))
})

test_that("advice names the dominant selective gear", {
  gp <- tibble::tibble(main_gear = "Trap", main_gear_share = 0.91,
    selective_gear_share = 0.91, selectivity_flag = TRUE)
  a <- .stock_advice(500, reasons = "no fish near maximum size (99th percentile < 80% of Linf)",
    gear_profile = gp)
  expect_match(a, "Trap \\(91%")
  expect_match(a, "other gears")
})

test_that(".mark_reference_gear skips mixed families", {
  res <- tibble::tibble(
    country = "timor", scale = "group", scientific_name = c("Carangidae", "Carangidae", "Siganus spp", "Siganus spp"),
    gear = c("Hand Line", "Long Line", "Gill Net", "Spear Gun"), size_reach = c(1.1, 2.0, 0.7, 0.9),
    quality = "warn", confidence = c("low", "low", "high", "high")
  )
  out <- .mark_reference_gear(res)
  expect_equal(out$reference_gear, c(FALSE, FALSE, FALSE, TRUE))
})
