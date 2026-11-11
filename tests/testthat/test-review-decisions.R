test_that("review_decisions keeps other accounts' decisions, the later one winning", {
  flags <- data.frame(
    submission_id = c(1, 2, 3, 4),
    validation_status = c(
      "validation_status_approved",
      "validation_status_not_approved",
      "validation_status_approved",
      "validation_status_not_approved"
    ),
    validated_by = c("reviewer", "pipeline", NA, "reviewer"),
    validated_at = as.Date(c("2026-09-01", "2026-09-01", NA, "2026-09-01"))
  )
  local_mocked_bindings(
    list_validation_statuses = function(...) {
      tibble::tibble(
        submission_id = c(4L, 5L, 6L),
        validation_status = "validation_status_approved",
        validated_at = lubridate::as_datetime("2026-09-10"),
        validated_by = c("kobo_reviewer", "kobo_reviewer", "pipeline"),
        fetch_error = FALSE
      )
    }
  )

  out <- review_decisions(flags, pipeline_users = "pipeline", asset_id = "a1")

  # 2 and 6 are the pipeline's own, 3 has no author.
  expect_equal(sort(out$submission_id), c(1L, 4L, 5L))
  # 4 was rejected in the platform, then approved later in KoBoToolbox.
  expect_equal(
    out$validation_status[out$submission_id == 4L],
    "validation_status_approved"
  )
})

test_that("review_decisions falls back to the collection when KoBoToolbox fails", {
  local_mocked_bindings(list_validation_statuses = function(...) stop("503"))
  flags <- data.frame(
    submission_id = "7",
    validation_status = "validation_status_approved",
    validated_by = "reviewer"
  )

  expect_equal(review_decisions(flags, "pipeline", asset_id = "a1")$submission_id, 7L)
  expect_equal(nrow(review_decisions(NULL, "pipeline")), 0)
})

test_that("review_decisions orders undated decisions last and lets the collection win ties", {
  flags <- data.frame(
    submission_id = c(1, 2, 3),
    validation_status = "validation_status_approved",
    validated_by = "reviewer",
    validated_at = lubridate::as_datetime(c(NA, "2026-09-10", "2026-09-10"))
  )
  local_mocked_bindings(
    list_validation_statuses = function(...) {
      tibble::tibble(
        submission_id = c(1L, 2L, 3L),
        validation_status = "validation_status_not_approved",
        validated_at = lubridate::as_datetime(c("2026-09-01", "2026-09-10", NA)),
        validated_by = "kobo_reviewer",
        fetch_error = FALSE
      )
    }
  )

  out <- review_decisions(flags, "pipeline", asset_id = "a1")
  status <- setNames(out$validation_status, out$submission_id)

  # 1: the dated KoBoToolbox decision beats the undated one in the collection.
  expect_equal(status[["1"]], "validation_status_not_approved")
  # 2: same timestamp in both, the collection's decision is kept.
  expect_equal(status[["2"]], "validation_status_approved")
  # 3: the undated KoBoToolbox decision loses to the dated one in the collection.
  expect_equal(status[["3"]], "validation_status_approved")
})

test_that("review_decisions drops submission ids that are not numbers", {
  flags <- data.frame(
    submission_id = c("x1", "8"),
    validation_status = "validation_status_approved",
    validated_by = "reviewer"
  )
  expect_equal(review_decisions(flags, "pipeline")$submission_id, 8L)
})

test_that("review_decisions stops without a pipeline account name", {
  flags <- data.frame(
    submission_id = c(1, 2),
    validation_status = "validation_status_approved",
    validated_by = c("pipeline", "reviewer")
  )
  expect_error(review_decisions(flags, pipeline_users = ""), "No pipeline account name")
  expect_error(review_decisions(flags, pipeline_users = NA_character_), "No pipeline account name")
})
