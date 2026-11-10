test_that("trip ids are recovered from the live naming convention", {
  # Real object names, as listed 2026-08-13 from pds-mozambique-dev,
  # pds-kenya-dev, pds-zanzibar-dev and pds-timor-dev. All four buckets use
  # the same layout.
  expect_equal(
    extract_trip_ids_from_filenames(
      c("pds-tracks_13518972.parquet", "pds-tracks_10754239.parquet"),
      prefix = "pds-tracks"
    ),
    c("13518972", "10754239")
  )
})

test_that("a bucket in another naming convention fails loudly", {
  # The regression this guards: returning the name unchanged makes every
  # stored track look missing, and ingest_pds_tracks() re-fetches the whole
  # history from the PDS API.
  expect_error(
    extract_trip_ids_from_filenames(
      c(
        "pds-track-13518972__20240101000000_abc1234__.csv.gz",
        "pds-track-10754239__20240101000000_abc1234__.csv.gz"
      ),
      prefix = "pds-tracks"
    ),
    "None of the 2 objects"
  )
})

test_that("an empty bucket is not an error", {
  expect_equal(
    extract_trip_ids_from_filenames(character(0), prefix = "pds-tracks"),
    character(0)
  )
})

test_that("a track is described by its first and last fix in time", {
  track <- tibble::tibble(
    # out of order, as a re-segmented trip can come back
    Time = as.POSIXct("2026-01-01 06:00:00", tz = "UTC") + c(120, 0, 60),
    Boat = 7,
    Lat = c(-6.2, -6.0, NA),
    Lng = c(39.2, 39.0, 39.1),
    `Speed (M/S)` = c(1, 40, 2)
  )

  out <- track_descriptors(track, Trip = 42)

  expect_equal(
    c(out$start_lat, out$start_lng, out$end_lat, out$end_lng),
    c(-6.0, 39.0, -6.2, 39.2)
  )
  expect_equal(round(out$start_end_distance / 1000), 31)
  expect_equal(out$outliers_proportion, 50)
  expect_equal(nrow(track_descriptors(track[3, ], Trip = 42)), 0)
})
