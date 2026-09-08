# The measured Timor-Leste fleet, shrunk: three devices still under
# "MAF / WorldFish", one sold on to a Barbadian owner years after it recorded
# Timorese trips, one under "Traders" that tracks bicycles rather than boats.
devices <- tibble::tibble(
  imei = c(
    "861508039409198", # redeployed: Timor trips, now owned in Barbados
    "861508039394440",
    "861508039407671",
    "861508039403019",
    "861508039400000" # bicycles
  ),
  customer_name = c(
    "Individual Boat Owner",
    "MAF / WorldFish",
    "MAF / WorldFish",
    "MAF / WorldFish",
    "Traders"
  ),
  community = c("Oistins", "Com", "Beto Tasi", "Beto Tasi", "Dili")
)

trips <- tibble::tibble(
  Trip = 1:7,
  IMEI = as.numeric(c(
    "861508039409198", # Timor trip on the redeployed device
    "861508039409198", # a genuinely Barbadian trip on the same device
    "861508039394440",
    "861508039407671",
    "861508039403019",
    "861508039403019", # no community recorded
    "861508039400000" # bicycle
  )),
  Community = c("Com", "Oistins", "Com", "Beto Tasi", "Beto Tasi", NA, "Dili")
)

test_that("the allowlist drops a redeployed device's whole history", {
  # The defect, pinned: trip 1 is Timorese and is lost.
  out <- select_country_trips(trips, devices, customers = "MAF / WorldFish")
  expect_equal(out$Trip, c(3L, 4L, 5L, 6L))
})

test_that("the denylist keeps redeployed history but not foreign trips", {
  # Trip 2 is Barbadian and survives: a country-scoped token would never
  # have returned it, which is the denylist's whole assumption.
  out <- select_country_trips(
    trips,
    devices,
    exclude_customers = c("Traders")
  )
  expect_equal(out$Trip, 1:6)
})

test_that("an empty denylist excludes nothing", {
  out <- select_country_trips(trips, devices, exclude_customers = character(0))
  expect_equal(nrow(out), nrow(trips))
})

test_that("the community rule recovers the history and rejects the rest", {
  out <- select_country_trips(
    trips,
    devices,
    customers = "MAF / WorldFish",
    select_by = "community"
  )
  # 1 recovered, on hardware since sold to Barbados. 2 rejected: same
  # device, but it happened at Oistins. 6 kept by the no-community fallback,
  # 7 dropped because Dili is Traders', i.e. a bicycle.
  expect_equal(out$Trip, c(1L, 3L, 4L, 5L, 6L))
})

test_that("a site whose devices have all moved on loses its trips, loudly", {
  # Communities come from current device records, so a landing site with no
  # device left on it takes its trips with it, including trips from devices
  # the country still owns. Unmeasured outside Timor-Leste, so it must warn.
  stripped <- dplyr::mutate(
    devices,
    community = ifelse(
      .data$community == "Beto Tasi",
      "Somewhere Else",
      .data$community
    )
  )
  log <- capture.output(
    out <- select_country_trips(
      trips,
      stripped,
      customers = "MAF / WorldFish",
      select_by = "community"
    ),
    type = "message"
  )
  expect_equal(out$Trip, c(1L, 3L, 6L))
  expect_match(
    paste(log, collapse = " "),
    "2 trip\\(s\\) from devices this country still owns were dropped"
  )
  expect_match(paste(log, collapse = " "), "Beto Tasi")
})

test_that("keeping zero trips is an error, not an empty parquet", {
  # An empty result becomes `version: latest` and empties the portal
  # without anything failing. That silence is what this whole fix is about.
  expect_error(
    select_country_trips(trips, devices, customers = "No Such Customer"),
    "kept 0 of 7 trips"
  )
})

test_that("a trips table without deviceInfo fails loudly", {
  # get_trips() without deviceInfo = TRUE returns neither column, and every
  # comparison then matches nothing and looks like a real finding.
  expect_error(
    select_country_trips(
      dplyr::select(trips, "Trip"),
      devices,
      customers = "MAF / WorldFish"
    ),
    "no IMEI column"
  )
  expect_error(
    select_country_trips(
      dplyr::select(trips, "Trip", "IMEI"),
      devices,
      customers = "MAF / WorldFish",
      select_by = "community"
    ),
    "no Community column"
  )
})

test_that("the config must name exactly one rule", {
  expect_error(
    select_country_trips(trips, devices, "MAF / WorldFish", "Traders"),
    "not both"
  )
  expect_error(select_country_trips(trips, devices), "no rule")
  expect_error(
    select_country_trips(
      trips,
      devices,
      exclude_customers = "Traders",
      select_by = "community"
    ),
    "a denylist cannot say"
  )
})

test_that("IMEIs match whether they arrive as text or as numbers", {
  # Character in the assets snapshot, numeric off the API. 15 digits are
  # exact as doubles, so both must land on the same value.
  out <- select_country_trips(
    dplyr::mutate(trips, IMEI = as.character(.data$IMEI)),
    dplyr::mutate(devices, imei = as.numeric(.data$imei)),
    customers = "MAF / WorldFish"
  )
  expect_equal(out$Trip, c(3L, 4L, 5L, 6L))
})

test_that("a retired device keeps its trips as long as its row survives", {
  # `device_sync()` only updates and creates, and nothing filters on
  # `active`, so a retired device keeps its row, its imei and its community.
  # Retirement on its own is therefore not a loss under either rule.
  retired <- dplyr::mutate(
    devices,
    active = c(TRUE, TRUE, FALSE, TRUE, TRUE) # 861508039407671 retired
  )
  expect_equal(
    select_country_trips(trips, retired, "MAF / WorldFish")$Trip,
    c(3L, 4L, 5L, 6L)
  )
  expect_equal(
    select_country_trips(
      trips,
      retired,
      "MAF / WorldFish",
      select_by = "community"
    )$Trip,
    c(1L, 3L, 4L, 5L, 6L)
  )
})

test_that("a site with no device row left is named, not silently dropped", {
  # Devices retired before the pds_devices table existed were never written
  # to it, so a whole landing site can be missing while its trips remain in
  # PDS. They belong to no customer, so they are dropped — but named.
  gone <- dplyr::filter(devices, .data$community != "Beto Tasi")
  log <- capture.output(
    out <- select_country_trips(
      trips,
      gone,
      "MAF / WorldFish",
      select_by = "community"
    ),
    type = "message"
  )
  expect_false(any(c(4L, 5L) %in% out$Trip))
  expect_match(paste(log, collapse = " "), "belong to no device at all")
  expect_match(paste(log, collapse = " "), "Beto Tasi")
})
