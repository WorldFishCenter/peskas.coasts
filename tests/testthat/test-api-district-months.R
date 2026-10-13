# The API's district-months are a contract with peskas-api: its
# `schema/field_metadata.py` describes these 23 columns, in this order.
api_columns <- c(
  "country", "month", "gaul_1_code", "gaul_1_name", "gaul_2_code", "gaul_2_name",
  "n_submissions", "mean_n_fishers", "mean_trip_duration_hrs", "mean_catch_kg",
  "mean_catch_price", "mean_price_kg", "mean_cpue", "mean_rpue",
  "estimated_fishing_trips", "estimated_catch_tn", "estimated_revenue", "sampling_rate",
  "estimated_fishing_trips_fao", "estimated_catch_tn_fao", "estimated_revenue_fao",
  "relative_error_catch_fao", "relative_error_revenue_fao"
)

# Two districts and a landing with none; A has a month with nothing recorded.
fake_districts_summaries <- function() {
  data.frame(
    gaul_1_code = c("1", "1", "1", NA),
    gaul_1_name = c("R", "R", "R", NA),
    gaul_2_code = c("11", "11", "12", NA),
    gaul_2_name = c("A", "A", "B", NA),
    date = lubridate::as_datetime(c("2026-05-01", "2026-06-01", "2026-05-01", "2026-05-01")),
    n_submissions = c(20L, NA, 5L, 3L),
    n_fishers = c(3, NA, 2, 1),
    trip_duration_hrs = c(6, NA, NaN, 4),
    mean_catch_kg = c(12, NA, 8, 1),
    mean_catch_price = c(100, NA, 50, 1),
    mean_cpue = c(1, NA, 0.5, 1),
    mean_rpue = c(10, NA, 5, 1),
    mean_price_kg = c(9, NA, 6, 1)
  )
}

gps_estimates <- data.frame(
  gaul_2_name = "A",
  date = lubridate::as_datetime("2026-05-01"),
  estimated_fishing_trips = 400,
  estimated_catch_tn = 4.8,
  estimated_revenue = 40000,
  sampling_rate = 0.2
)

test_that("district-months have one row per district code and month, under the API's names", {
  out <- api_district_months(fake_districts_summaries(), gps_estimates, "testland")

  expect_named(out, api_columns)
  expect_equal(unique(out$country), "testland")
  # The landing with no district is left out.
  expect_equal(out$gaul_2_code, c("11", "11", "12"))
  expect_equal(out$month, as.Date(c("2026-05-01", "2026-06-01", "2026-05-01")))
  # A month with nothing recorded stays, with no landings and no figures.
  expect_equal(out$n_submissions, c(20L, 0L, 5L))
  expect_true(is.na(out$mean_catch_kg[2]))
  expect_equal(out$mean_n_fishers, c(3, NA, 2))
  expect_equal(out$estimated_catch_tn, c(4.8, NA, NA))
  # A mean over nothing is empty, not NaN.
  expect_false(is.nan(out$mean_trip_duration_hrs[3]))
})

test_that("a country without the FAO ARTFISH method keeps its columns, empty", {
  out <- api_district_months(fake_districts_summaries(), gps_estimates, "testland")
  fao <- grep("_fao$", api_columns, value = TRUE)
  expect_true(all(is.na(unlist(out[fao]))))
})

test_that("a district-month estimated twice stops the export rather than repeat a row", {
  twice <- rbind(gps_estimates, gps_estimates)
  expect_error(api_district_months(fake_districts_summaries(), twice, "testland"))
})
