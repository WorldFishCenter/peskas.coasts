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
