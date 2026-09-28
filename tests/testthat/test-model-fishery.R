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
