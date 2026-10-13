# Known answers for the FAO raising: total = boats x days fished x catch per day.

trip <- function(id, gear, date, catch, value, days_week, district = "D1") {
  data.frame(
    trip_id = id, gaul_2_code = district, gaul_2_name = district,
    gear = gear, vessel_type = "Canoe", date_month = as.Date(date),
    catch_kg = catch, revenue = value, days_week = days_week
  )
}
# Census rows in the `frame_units` layout; `boat = NA` is a census that counts gear only.
units <- function(gear, n_boats, boat = NA_character_, district = "D1") {
  data.frame(
    unit = paste(district, boat, gear, seq_along(gear)), gaul_2_code = district,
    vessel_standard_name = boat, gear_standard_name = gear, n_boats = n_boats
  )
}
census <- units(c("Gill Net", "Gill Net"), c(60L, 40L))
# Airtable `gears`: standard_name and its FAO_category.
groups <- data.frame(
  standard_name = c("Gill Net", "Hand Line", "Long Line", "Trap", "Other"),
  fao_category = c("Gillnets and entangling nets", "Hooks and lines", "Hooks and lines", "Traps", NA)
)
# Airtable `vessels`, one country's rows: standard_name and its vessel_group.
boats <- data.frame(
  standard_name = c("Dugout Canoe", "Outrigger Canoe", "Dhow"),
  vessel_group = c("Canoes", "Canoes", "Dhows")
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
  out <- estimate_catch_fao(trips, census, groups, boats, min_samples = 2, min_district_trips = 1)

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
  july <- estimate_catch_fao(trips, census, groups, boats, min_samples = 2, min_district_trips = 1)
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
    estimate_catch_fao(trips, census, groups, boats)$fishing_unit,
    "Gillnets and entangling nets"
  )
})

test_that("gears are raised by FAO group, with their census boats summed", {
  lines <- units(c("Hand Line", "Long Line"), c(30L, 20L))
  trips <- rbind(
    trip("a", "Hand Line", "2026-06-01", 10, 100, 7),
    trip("b", "Long Line", "2026-06-01", 20, 200, 7)
  )
  out <- estimate_catch_fao(trips, lines, groups, boats)

  expect_equal(out$fishing_unit, "Hooks and lines")
  expect_equal(out$boats, 50)
})

test_that("a unit with too few trips borrows its district's catch per trip", {
  two_units <- units(c("Gill Net", "Gill Net", "Trap"), c(60L, 40L, 10L))
  trips <- rbind(
    do.call(rbind, lapply(1:5, function(i) {
      trip(paste0("g", i), "Gill Net", "2026-06-01", 10, 100, 7)
    })),
    trip("t1", "Trap", "2026-06-01", 100, 1000, 7)
  )
  out <- estimate_catch_fao(trips, two_units, groups, boats)
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
  thin <- estimate_catch_fao(nets(9), census, groups, boats)
  enough <- estimate_catch_fao(nets(10), census, groups, boats)

  expect_true(is.na(thin$total_catch_kg))
  expect_equal(thin$quality_catch, "too_few_trips")
  expect_equal(enough$total_catch_kg, 100 * 30 * 10)
})

test_that("a gear with two FAO groups in Airtable stops", {
  twice <- rbind(groups, data.frame(standard_name = "Trap", fao_category = "Miscellaneous gear"))
  expect_error(
    estimate_catch_fao(trip("a", "Trap", "2026-06-01", 10, 100, 7), census, twice, boats),
    "more than one group"
  )
})

test_that("with a boat x gear census, units are boat group x gear group", {
  both <- units(
    c("Hand Line", "Long Line", "Hand Line"), c(30, 20, 5),
    boat = c("Dugout Canoe", "Outrigger Canoe", "Dhow")
  )
  trips <- rbind(
    transform(trip("a", "Hand Line", "2026-06-01", 10, 100, 7), vessel_type = "Dugout Canoe"),
    transform(trip("b", "Long Line", "2026-06-01", 20, 200, 7), vessel_type = "Outrigger Canoe"),
    transform(trip("c", "Hand Line", "2026-06-01", 30, 300, 7), vessel_type = "Dhow")
  )
  out <- estimate_catch_fao(trips, both, groups, boats, min_samples = 1, min_district_trips = 1)

  expect_setequal(out$fishing_unit, c("Canoes | Hooks and lines", "Dhows | Hooks and lines"))
  expect_equal(out$boats[out$fishing_unit == "Canoes | Hooks and lines"], 50)
  expect_equal(out$n_trips[out$fishing_unit == "Canoes | Hooks and lines"], 2)
})

test_that("a census of boat types only raises by boat type, ungrouped where it has no group", {
  boat_only <- units(c(NA, NA), c(70, 30), boat = c("Planked Canoe", "Motorized Boat"))
  trips <- rbind(
    transform(trip("a", "Gill Net", "2026-06-01", 10, 100, 7), vessel_type = "Planked Canoe"),
    transform(trip("b", "Hand Line", "2026-06-01", 50, 500, 7), vessel_type = "Motorized Boat")
  )
  out <- estimate_catch_fao(trips, boat_only, groups, boats[0, ], min_samples = 1, min_district_trips = 1)

  expect_setequal(out$fishing_unit, c("Planked Canoe", "Motorized Boat"))
  expect_equal(out$boats[out$fishing_unit == "Motorized Boat"], 30)
})

test_that("a census counting different things in different districts stops", {
  mixed <- rbind(census, units("Gill Net", 10L, boat = "Dhow"))
  expect_error(
    estimate_catch_fao(trip("a", "Gill Net", "2026-06-01", 10, 100, 7), mixed, groups, boats),
    "same things"
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
    total_revenue = c(30000, 5000, NA),
    re_catch = c(0.1, 0.5, NA),
    re_revenue = c(0.2, NA, NA)
  )
  out <- fao_portal_metrics(fao)
  d1 <- out[out$gaul_2_name == "D1", ]

  expect_equal(d1$estimated_fishing_trips_fao, 10 * 20 + 5 * 10)
  expect_equal(d1$estimated_catch_tn_fao, 4)
  expect_equal(d1$estimated_revenue_fao, 35000)
  expect_equal(d1$date, lubridate::as_datetime("2026-06-01"))
  # The units' upper limits added: (3000 * 0.1 + 1000 * 0.5) / 4000.
  expect_equal(d1$relative_error_catch_fao, 0.2)
  # A unit with no known error is left out of it.
  expect_equal(d1$relative_error_revenue_fao, 0.2)
  # A district-month too thin to raise stays empty rather than zero.
  expect_true(all(is.na(unlist(out[out$gaul_2_name == "D2", 2:6]))))
})

test_that("fao_catch_per_trip weights each unit by its census boats, leaving out fishers on foot", {
  fleet <- units(c(NA, NA, NA), c(80, 20, 500), boat = c("Planked Canoe", "Motorized Boat", "Feet"))
  trips <- rbind(
    transform(trip("a", "Gill Net", "2026-06-01", 10, 100, 7), vessel_type = "Planked Canoe"),
    transform(trip("b", "Hand Line", "2026-06-01", 50, 500, 7), vessel_type = "Motorized Boat"),
    transform(trip("c", "Hand Line", "2026-06-01", 50, 500, 7), vessel_type = "Motorized Boat"),
    transform(trip("d", "Hand Line", "2026-06-01", 1, 10, 7), vessel_type = "Feet")
  )
  fao <- estimate_catch_fao(trips, fleet, groups, boats[0, ], min_samples = 1, min_district_trips = 1)
  out <- fao_catch_per_trip(fao)

  expect_equal(fao$on_foot, fao$fishing_unit == "Feet")
  # The boats' trips average 37 kg, but canoes are 80% of the boats: 0.8 x 10 + 0.2 x 50.
  expect_equal(out$unit_catch_kg, 18)
  expect_equal(out$unit_catch_price, 180)
  expect_equal(out$date, lubridate::as_datetime("2026-06-01"))
})

test_that("a country without fao.surveys has no FAO raising", {
  expect_null(raise_catch_fao(list(fao = NULL), assets = list()))
})

# Catch rows of the API landings: one per trip and species.
item <- function(id, taxon, kg) data.frame(trip_id = id, catch_taxon = taxon, catch_kg = kg)
species_of <- function(trips, landings, census, ...) {
  fao <- estimate_catch_fao(trips, census, groups, boats, ...)
  out <- estimate_species_fao(fao, fao_units(trips, census, groups, boats)$trips, landings)
  list(fao = fao, species = out[order(out$fishing_unit, out$catch_taxon), ])
}

test_that("a unit's raised catch is split by the species mix of its trips", {
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Gill Net", "2026-06-01", 20, 200, 7)
  )
  landings <- rbind(item("a", "SKJ", 10), item("b", "SKJ", 5), item("b", "YFT", 15))
  out <- species_of(trips, landings, census, min_samples = 2, min_district_trips = 1)

  expect_equal(out$species$catch_taxon, c("SKJ", "YFT"))
  expect_equal(out$species$total_catch_kg, c(22500, 22500)) # 100 boats x 30 days x 15 kg, half each
  expect_equal(sum(out$species$total_catch_kg), out$fao$total_catch_kg)
})

test_that("species rows that are a sample of the trip set its mix, not its weight", {
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Gill Net", "2026-06-01", 20, 200, 7)
  )
  # Trip b weighed a 4 kg sample of its 20 kg: half SKJ, half YFT.
  landings <- rbind(item("a", "SKJ", 1), item("b", "SKJ", 2), item("b", "YFT", 2))
  out <- species_of(trips, landings, census, min_samples = 2, min_district_trips = 1)

  expect_equal(out$species$total_catch_kg, c(30000, 15000)) # 20 of the 30 sampled kg are SKJ
})

test_that("a unit that borrows its catch per trip borrows the district's species mix", {
  two_gears <- units(c("Gill Net", "Hand Line"), c(60L, 40L))
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Gill Net", "2026-06-01", 20, 200, 7),
    trip("c", "Hand Line", "2026-06-01", 30, 300, 7)
  )
  landings <- rbind(item("a", "SKJ", 10), item("b", "SKJ", 20), item("c", "YFT", 30))
  out <- species_of(trips, landings, two_gears, min_samples = 2, min_district_trips = 1)
  lines <- out$species[out$species$fishing_unit == "Hooks and lines", ]
  nets <- out$species[out$species$fishing_unit == "Gillnets and entangling nets", ]

  expect_equal(lines$catch_source, c("district", "district"))
  expect_equal(lines$catch_taxon, c("SKJ", "YFT"))
  expect_equal(lines$total_catch_kg, rep(40 * 30 * 20 / 2, 2)) # the district's trips are half SKJ
  expect_equal(nets$catch_taxon, "SKJ") # enough trips of its own
  expect_equal(sum(out$species$total_catch_kg), sum(out$fao$total_catch_kg))
})

test_that("catch with no weighed species row stays unidentified, and no catch adds nothing", {
  trips <- rbind(
    trip("a", "Gill Net", "2026-06-01", 10, 100, 7),
    trip("b", "Gill Net", "2026-06-01", 0, 0, 7),
    trip("c", "Gill Net", "2026-06-01", 10, 100, 7)
  )
  landings <- rbind(item("a", NA, NA), item("b", NA, NA), item("c", "SKJ", 10))
  out <- species_of(trips, landings, census, min_samples = 2, min_district_trips = 1)

  expect_equal(out$species$catch_taxon, c("SKJ", NA))
  expect_equal(out$species$total_catch_kg, rep(out$fao$total_catch_kg / 2, 2))
  # A district and month too thin to raise has no species either.
  expect_equal(nrow(species_of(trips, landings, census)$species), 0)
})

test_that("fao_portal_taxa adds up a district's units per species, in tonnes, under the summaries' names", {
  species <- data.frame(
    gaul_2_name = c("D1", "D1", "D1", "D1", "D2"),
    fishing_unit = c("Canoes | Nets", "Dhows | Nets", "Canoes | Nets", "Canoes | Nets", "Canoes | Nets"),
    date_month = as.Date("2026-06-01"),
    catch_taxon = c("SKJ", "SKJ", "XXX", NA, "YFT"),
    total_catch_kg = c(1000, 500, 200, 300, 4000)
  )
  # `taxa_traits`: the summaries' label for each code. YFT is listed twice, as one code is in Timor.
  names <- data.frame(
    alpha3_code = c("SKJ", "YFT", "YFT"),
    catch_taxon = c("Katsuwonus pelamis", "Thunnus albacares", "Thunnus albacares")
  )

  out <- fao_portal_taxa(species, names)
  out <- out[order(out$gaul_2_name, out$catch_taxon), ]

  expect_named(out, c("gaul_2_name", "date", "catch_taxon", "metric", "value"))
  expect_equal(unique(out$metric), "estimated_catch_tn_fao")
  expect_s3_class(out$date, "POSIXct")
  # D1: both units' skipjack together; the unnamed code and the unidentified catch share the empty label.
  expect_equal(out$catch_taxon, c("Katsuwonus pelamis", NA, "Thunnus albacares"))
  expect_equal(out$value, c(1.5, 0.5, 4)) # a code listed twice is not counted twice
  expect_equal(sum(out$value), sum(species$total_catch_kg) / 1000)
})
