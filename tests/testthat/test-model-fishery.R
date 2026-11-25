# Airtable `geo` has one row per landing site, so one GAUL2 district can
# carry several boat counts. Unsummed, the join repeated every district-month
# once per row, each scaled by a different fleet.

test_that("estimate_fleet_activity keeps one row per district-month", {
  monthly_stats <- data.frame(
    gaul_2_name = c("Cidade De Maputo", "Lamu West"),
    date_month = as.Date("2026-01-01"),
    sample_boats_tracked = c(10, 2),
    avg_trips_per_boat_per_month = c(4, 3)
  )
  boat_registry <- data.frame(
    gaul_2_name = c(rep("Cidade De Maputo", 4), "Lamu West", "Lamu West"),
    total_boats = c(207L, 120L, 94L, 76L, NA, NA)
  )

  out <- estimate_fleet_activity(monthly_stats, boat_registry)

  expect_equal(nrow(out), 2)
  maputo <- out[out$gaul_2_name == "Cidade De Maputo", ]
  expect_equal(maputo$total_boats, 497)
  expect_equal(maputo$estimated_total_trips, 4 * 497)
  # A district with no boat count stays unestimated rather than zero.
  expect_true(is.na(out$total_boats[out$gaul_2_name == "Lamu West"]))
})

# Mjini, December 2025: one ring-net trip of 720 kg was raised to the whole
# district's fleet, mostly hand-line and trap boats landing a few kilos.
test_that("calculate_district_totals does not raise a month with fewer than ten surveyed trips", {
  fleet_estimates <- data.frame(
    gaul_2_name = c("Mjini", "Mkoani"),
    date_month = as.Date("2025-12-01"),
    sample_total_trips = 20,
    estimated_total_trips = 100,
    sampling_rate = 0.1
  )
  summaries <- data.frame(
    gaul_2_name = c("Mjini", "Mkoani"),
    date = as.Date("2025-12-01"),
    n_submissions = c(1L, 10L),
    mean_catch_kg = c(720, 20),
    mean_catch_price = c(7e6, 2e4)
  )

  out <- calculate_district_totals(fleet_estimates, summaries)

  expect_equal(out$estimated_total_catch_kg, c(NA, 100 * 20))
  expect_equal(out$estimated_total_revenue, c(NA, 100 * 2e4))
  # Effort does not depend on the survey sample and is kept.
  expect_equal(out$estimated_total_trips, c(100, 100))
})

# Two coastal districts side by side, each 0.1 degrees (about 11 km) wide.
square <- function(lng) {
  sf::st_polygon(list(cbind(
    lng + c(0, 0.1, 0.1, 0, 0),
    -5 + c(0, 0, 0.1, 0.1, 0)
  )))
}
districts <- sf::st_sf(
  gaul_2_code = c("1", "2"),
  gaul_2_name = c("West", "East"),
  geometry = sf::st_sfc(square(39.0), square(39.1), crs = 4326)
)

test_that("a landing goes to the district it is in or beside, not one far out", {
  landings <- data.frame(
    Trip = 1:3,
    # inside West; about 2 km off East's coast; about 45 km out at sea
    end_lng = c(39.05, 39.22, 39.6),
    end_lat = -4.95
  )

  out <- locate_landings(landings, districts)

  expect_equal(out$gaul_2_name, c("West", "East", NA))
  expect_equal(out$landing_km[1], 0)
  expect_true(out$landing_km[2] > 1 && out$landing_km[2] < 3)
})

test_that("a tracker counts where most of its trips landed that month", {
  jan <- as.Date("2026-01-01")
  feb <- as.Date("2026-02-01")
  located <- data.frame(
    Trip = 1:7,
    IMEI = c(1, 1, 1, 1, 2, 2, 1),
    date_month = c(jan, jan, jan, feb, jan, jan, jan),
    gaul_2_code = c("1", "1", "2", "2", NA, NA, NA),
    gaul_2_name = c("West", "West", "East", "East", NA, NA, NA)
  )

  out <- assign_home_district(located)

  # Trip 3 landed in East and trip 7 at sea, yet both are January trips of a
  # West tracker. February follows the tracker's move; tracker 2 landed nowhere.
  expect_equal(out$Trip, c(1L, 2L, 3L, 4L, 7L))
  expect_equal(out$gaul_2_name, c("West", "West", "West", "East", "West"))
})
