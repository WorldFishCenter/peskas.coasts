test_that("get_length_limits follows the siblings' rule on FishBase species only", {
  taxa <- tibble::tibble(
    alpha3_code = c("IUU", "COM", "MUD", "SPI", "SPI"),
    original_name = c("Siganus sutor", "Scomberomorus commerson", "Scylla serrata", "Siganus spp", "Siganus spp"),
    server = c("fishbase", "fishbase", "sealifebase", "fishbase", "fishbase"),
    length_max_tl = c(54.9, 266, 9.15, 30, 50),
    length_common_tl = c(NA, 120, NA, 20, NA)
  )
  limits <- get_length_limits(taxa)

  expect_equal(nrow(limits), 3)
  expect_false("Scylla serrata" %in% limits$original_name)
  sutor <- limits[limits$original_name == "Siganus sutor", ]
  expect_equal(sutor$max_length, 54.9)
  expect_equal(sutor$min_length, 0.25 * 0.625 * 54.9)
  spp <- limits[limits$original_name == "Siganus spp", ]
  expect_equal(spp$max_length, unname(stats::quantile(c(30, 50), 0.95)))
  expect_equal(spp$min_length, 0.25 * 20)
})
