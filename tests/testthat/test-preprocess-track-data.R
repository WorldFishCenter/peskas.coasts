# A trip PDS holds no points for comes back as a header-only CSV, typed
# all-character by readr. One such trip used to abort the whole parallel batch
# in preprocess_pds_tracks() with "non-numeric argument to binary operator".
empty_track <- tibble::tibble(
  Time = character(0),
  Boat = character(0),
  Trip = character(0),
  Lat = character(0),
  Lng = character(0),
  `Speed (M/S)` = character(0),
  `Range (Meters)` = character(0),
  Heading = character(0),
  `Boat Name` = character(0),
  Community = character(0)
)

# Two hours of one-minute fixes drifting east, enough that interior cells
# survive the first-two/last-two trim at the end of the pipeline.
points_track <- tibble::tibble(
  Time = as.POSIXct("2026-01-01 06:00:00", tz = "UTC") + (0:119) * 60,
  Boat = 1,
  Trip = 42,
  Lat = -6.16,
  Lng = 39.19 + (0:119) * 0.0005,
  `Speed (M/S)` = 1.5,
  `Range (Meters)` = 100,
  Heading = 90,
  `Boat Name` = "Test",
  Community = "Nungwi"
)

test_that("a track with no points grids to nothing rather than erroring", {
  expect_silent(result <- preprocess_track_data(empty_track))
  expect_equal(nrow(result), 0)
})

test_that("an empty track's columns match a real track's, so the two bind", {
  empty <- preprocess_track_data(empty_track)
  full <- preprocess_track_data(points_track)

  expect_equal(
    vapply(empty, function(x) class(x)[1], character(1)),
    vapply(full, function(x) class(x)[1], character(1))
  )
  expect_equal(nrow(dplyr::bind_rows(empty, full)), nrow(full))
})

test_that("a track with points still grids", {
  result <- preprocess_track_data(points_track)

  expect_gt(nrow(result), 0)
  expect_true(all(result$Trip == 42))
  expect_type(result$lat_grid, "double")
})
