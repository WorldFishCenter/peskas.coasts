test_that("summarise_collection counts each submission once, at both levels", {
  # One row per catch: trip 1 has two, trip 4 is dated in the future.
  raw <- data.frame(
    trip_id = c("TRIP_1", "TRIP_1", "TRIP_2", "TRIP_3", "TRIP_4", "TRIP_5"),
    survey_organization = c("KEFS", "KEFS", "KEFS", "WCS", "WCS", "WCS"),
    landing_date = as.Date(c(
      "2025-01-10", "2025-01-10", "2025-03-05", "2025-02-01",
      "2099-01-01", "2025-02-02"
    )),
    gaul_2_code = c("A", "A", "A", "B", "B", NA),
    gaul_2_name = c("Alpha", "Alpha", "Alpha", "Beta", "Beta", NA),
    landing_site = c("s1", "s1", "s2", "s3", "s3", "s4")
  )
  # WCS records no enumerator.
  flags <- data.frame(submission_id = c("1", "2"), submitted_by = c("e1", "e2"))
  pds_trips <- data.frame(
    Trip = 1:3,
    Boat = c(10, 10, 11),
    Ended = as.POSIXct(c("2025-01-03", "2025-01-20", "2025-02-11"), tz = "UTC")
  )
  fleet <- data.frame(
    gaul_2_name = c("Alpha", "Alpha", "Gamma"),
    date_month = as.Date(c("2025-01-01", "2025-02-01", "2025-01-01")),
    sample_total_trips = c(2, 0, 1),
    sample_boats_tracked = c(1, 0, 1)
  )
  geo <- data.frame(
    gaul_2_code = c("A", "A", "B", "C"),
    gaul_2_name = c("Alpha", "Alpha", "Beta", "Gamma")
  )

  out <- summarise_collection(
    "kenya", raw, c("TRIP_1", "TRIP_3"), flags, pds_trips, fleet, geo
  )

  country <- out[out$level == "country", ]
  expect_equal(country$key, "kenya")
  expect_equal(country$submissions, 4)
  expect_equal(country$submissions_validated, 2)
  expect_equal(country$enumerators, 2)
  expect_equal(country$programmes, "KEFS, WCS")
  expect_equal(country$last_landing, as.Date("2025-03-05"))
  expect_equal(country$submissions_per_month, round(4 / 3, 1))
  expect_equal(country$gps_trips, 3)
  expect_equal(country$trackers_per_month, 1)

  alpha <- out[out$key == "kenya-A", ]
  expect_equal(alpha$submissions, 2)
  expect_equal(alpha$landing_sites, 2)
  expect_equal(alpha$gps_trips, 2)
  expect_equal(alpha$gps_last_month, as.Date("2025-01-01"))
  # Only WCS here: unknown, not zero.
  expect_true(is.na(out$enumerators[out$key == "kenya-B"]))
  # GPS trips but no surveys still make a district row, named from the frame.
  gamma <- out[out$key == "kenya-C", ]
  expect_equal(gamma$gaul_2_name, "Gamma")
  expect_true(is.na(gamma$submissions))
  expect_equal(nrow(out), 4)

  # Timor-Leste's GPS tracker method places no trip in a district and stores
  # months as date-times.
  timor_fleet <- data.frame(
    gaul_2_name = NA_character_,
    date_month = as.POSIXct("2025-01-01", tz = "UTC"),
    sample_total_trips = 5,
    sample_boats_tracked = 2
  )
  expect_no_warning(
    out <- summarise_collection(
      "timor", raw, character(), flags, pds_trips, timor_fleet, geo
    )
  )
  expect_true(all(is.na(out$gps_trips[out$level == "district"])))
})
