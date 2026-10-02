# Known answers for the FAO raising: total = boats x days fished x catch per day.

trip <- function(id, gear, date, catch, value, days_week, district = "D1") {
  data.frame(
    trip_id = id, gaul_2_code = district, gaul_2_name = district,
    gear = gear, vessel_type = "Canoe", date_month = as.Date(date),
    catch_kg = catch, revenue = value, days_week = days_week
  )
}
census <- data.frame(
  gaul_2_code = "D1", category_kind = "gear",
  standard_name = c("Gill Net", "Gill Net"), n_boats = c(60L, 40L)
)
# Airtable `gears`: standard_name and its FAO_category.
groups <- data.frame(
  standard_name = c("Gill Net", "Hand Line", "Long Line", "Trap", "Other"),
  fao_category = c("Gillnets and entangling nets", "Hooks and lines", "Hooks and lines", "Traps", NA)
)

test_that("compound_re follows the Toolkit's seven steps", {
  t4 <- stats::qt(0.95, df = 4)
  max_catch <- 100 * (15 + t4 * 3 / sqrt(5)) * (20 + t4 * 8 / sqrt(5))
  expected <- (max_catch - 100 * 15 * 20) / (100 * 15 * 20)
  expect_equal(compound_re(20, 8, 5, 15, 3, 5), expected)
  expect_true(is.na(compound_re(20, 8, 1, 15, 3, 5)))
})

test_that("a cell is raised as boats x days fished x catch per trip", {
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Gill Net", "2026-06-01", 20, 200, 7)
  )
  out <- estimate_catch_fao(trips, census, groups, min_samples = 2, min_district_trips = 1)

  expect_equal(out$boats, 100)
  expect_equal(out$days_fished, 30) # every day of June
  expect_equal(out$total_catch_kg, 100 * 30 * 15)
  expect_equal(out$total_revenue, 100 * 30 * 150)
  expect_equal(out$effort_source, "month")
  # The error is built from the trip values, not from the cell means.
  expect_equal(out$re_catch, compound_re(15, stats::sd(c(10, 20)), 2, 30, 0, 2))
  expect_equal(out$quality_catch, "fail")
})

test_that("a month with one weekly answer borrows the all-months average", {
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Gill Net", "2026-06-01", 20, 200, 7),
    trip("c", "Gill Net", "2026-07-01", 12, 120, 3.5)
  )
  july <- estimate_catch_fao(trips, census, groups, min_samples = 2, min_district_trips = 1)
  july <- july[july$date_month == as.Date("2026-07-01"), ]

  expect_equal(july$effort_source, "all_months")
  expect_equal(july$days_fished, mean(c(30, 30, 3.5 / 7 * 31)))
  expect_equal(july$quality_catch, "unknown") # one catch sample has no error
})

test_that("units the census does not count are not raised", {
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Trap", "2026-06-01", 20, 200, 7)
  )
  expect_equal(
    estimate_catch_fao(trips, census, groups)$fishing_unit,
    "Gillnets and entangling nets"
  )
})

test_that("gears are raised by FAO group, with their census boats summed", {
  lines <- data.frame(
    gaul_2_code = "D1", category_kind = "gear",
    standard_name = c("Hand Line", "Long Line"), n_boats = c(30L, 20L)
  )
  trips <- rbind(
    trip("a", "Hand Line", "2026-06-01", 10, 100, 7),
    trip("b", "Long Line", "2026-06-01", 20, 200, 7)
  )
  out <- estimate_catch_fao(trips, lines, groups)

  expect_equal(out$fishing_unit, "Hooks and lines")
  expect_equal(out$boats, 50)
})

test_that("a unit with too few trips borrows its district's catch per trip", {
  two_units <- rbind(census, data.frame(
    gaul_2_code = "D1", category_kind = "gear", standard_name = "Trap", n_boats = 10L
  ))
  trips <- rbind(
    do.call(rbind, lapply(1:5, function(i) {
      trip(paste0("g", i), "Gill Net", "2026-06-01", 10, 100, 7)
    })),
    trip("t1", "Trap", "2026-06-01", 100, 1000, 7)
  )
  out <- estimate_catch_fao(trips, two_units, groups)
  traps <- out[out$fishing_unit == "Traps", ]
  nets <- out[out$fishing_unit == "Gillnets and entangling nets", ]

  expect_equal(traps$catch_source, "district")
  expect_equal(traps$catch_kg_per_trip, (5 * 10 + 100) / 6)
  expect_equal(nets$catch_source, "unit")
  expect_equal(nets$catch_kg_per_trip, 10)
})

test_that("a district-month with fewer than ten trips is not raised", {
  nets <- function(n) do.call(rbind, lapply(seq_len(n), function(i) {
    trip(paste0("g", i), "Gill Net", "2026-06-01", 10, 100, 7)
  }))
  thin <- estimate_catch_fao(nets(9), census, groups)
  enough <- estimate_catch_fao(nets(10), census, groups)

  expect_true(is.na(thin$total_catch_kg))
  expect_equal(thin$quality_catch, "too_few_trips")
  expect_equal(enough$total_catch_kg, 100 * 30 * 10)
})

test_that("a gear with two FAO groups in Airtable stops", {
  twice <- rbind(groups, data.frame(standard_name = "Trap", fao_category = "Miscellaneous gear"))
  expect_error(
    estimate_catch_fao(trip("a", "Trap", "2026-06-01", 10, 100, 7), census, twice),
    "more than one FAO_category"
  )
})

test_that("a census mixing gear and vessel counts stops", {
  mixed <- rbind(census, transform(census, category_kind = "vessel"))
  expect_error(
    estimate_catch_fao(trip("a", "Gill Net", "2026-06-01", 10, 100, 7), mixed, groups),
    "one kind of unit"
  )
})

test_that("fao_trips keeps one row per surveyed trip and cleans its inputs", {
  landings <- data.frame(
    trip_id = c("TRIP_1", "TRIP_1", "TRIP_2", "TRIP_3"),
    catch_outcome = c("1", "1", "0", "1"),
    tot_catch_kg = c(30, 30, NA, 5),
    tot_catch_price = c(300, 300, NA, 50),
    landing_date = as.Date(c("2026-06-03", "2026-06-03", "2026-06-10", "2026-06-11"))
  )
  # TRIP_3 is not in the weekly survey, like a WCS trip in Kenya.
  surveys <- data.frame(submission_id = c("1", "2"), fishing_days_week = c(5, 50))

  out <- fao_trips(landings, surveys)

  expect_equal(out$trip_id, c("TRIP_1", "TRIP_2"))
  expect_equal(out$catch_kg, c(30, 0)) # no catch counts as zero
  expect_equal(out$days_week, c(5, NA)) # answers outside 0-7 are dropped
  expect_equal(out$date_month, as.Date(c("2026-06-01", "2026-06-01")))
})

test_that("fao_portal_metrics adds a district's units into the portal metrics", {
  fao <- data.frame(
    gaul_2_name = c("D1", "D1", "D2"),
    date_month = as.Date("2026-06-01"),
    boats = c(10, 5, 8),
    days_fished = c(20, 10, 15),
    total_catch_kg = c(3000, 1000, NA),
    total_revenue = c(30000, 5000, NA)
  )
  out <- fao_portal_metrics(fao)
  d1 <- out[out$gaul_2_name == "D1", ]

  expect_equal(d1$estimated_fishing_trips_fao, 10 * 20 + 5 * 10)
  expect_equal(d1$estimated_catch_tn_fao, 4)
  expect_equal(d1$estimated_revenue_fao, 35000)
  expect_equal(d1$date, lubridate::as_datetime("2026-06-01"))
  # A district-month too thin to raise stays empty rather than zero.
  expect_true(all(is.na(unlist(out[out$gaul_2_name == "D2", 2:4]))))
})

test_that("a country without fao.surveys has no FAO raising", {
  expect_null(raise_catch_fao(list(fao = NULL), assets = list()))
})
