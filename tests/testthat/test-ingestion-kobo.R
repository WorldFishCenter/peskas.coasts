test_that("a page that fails stops the download instead of returning part of it", {
  # 19 Nov 2026: page 20 of the Kenya WCS form failed and 19,000 of 60,637
  # submissions were saved as the full raw file.
  httr2::local_mocked_responses(function(req) {
    if (grepl("start=0", req$url)) {
      httr2::response_json(
        body = list(
          count = 2,
          `next` = "https://kobo.test/api/v2/assets/a/data.json?limit=1&start=1",
          results = list(list(`_id` = 1L))
        )
      )
    } else {
      httr2::response(status_code = 500)
    }
  })

  expect_error(
    suppressWarnings(
      get_kobo_data("a", url = "kobo.test", uname = "u", pwd = "p", limit = 1, progress = FALSE)
    ),
    "Failed to retrieve page 2 after 1 records"
  )
})
