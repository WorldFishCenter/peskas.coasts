# "latest" is not a fixed dataset: it follows whatever release the installed
# rfishbase points at. 26.06/26.07 kept Caesionidae and Scaridae as family
# names with no species attached, so both expanded to nothing, got no
# coefficients, and weighed NA — which sums to zero without erroring. That is
# what removed CJX and PWT from Timor-Leste's portal. These guard the two
# halves of the fix: the release is pinnable and validated, and a name that
# resolves to nothing is no longer silent.

test_that("db_version resolves from argument, then config, then latest", {
  expect_equal(resolve_db_version(list()), "latest")
  expect_equal(
    resolve_db_version(list(metadata = list(fishbase = list(
      db_version = "25.04"
    )))),
    "25.04"
  )
  # An explicit argument beats configuration.
  expect_equal(
    resolve_db_version(
      list(metadata = list(fishbase = list(db_version = "25.04"))),
      version = "24.07"
    ),
    "24.07"
  )
})

test_that("db_version is rejected unless both servers publish it", {
  # FishBase publishes 21.06, SeaLifeBase does not. One release goes to both
  # servers, so this has to fail here rather than halfway through a run.
  testthat::local_mocked_bindings(
    available_releases = function(server = "fishbase") {
      if (identical(server, "fishbase")) {
        c("21.06", "24.07", "25.04")
      } else {
        c("24.07", "25.04")
      }
    },
    .package = "rfishbase"
  )
  expect_equal(resolve_db_version(list(), version = "25.04"), "25.04")
  expect_error(resolve_db_version(list(), version = "21.06"), "sealifebase")
  expect_error(resolve_db_version(list(), version = "99.99"), "fishbase")
  expect_error(resolve_db_version(list(), version = c("24.07", "25.04")))
  expect_error(resolve_db_version(list(), version = ""))
})

test_that("expand_taxonomic_info logs the names it drops", {
  backbone <- tibble::tibble(
    SpecCode = 1L,
    sci_name = "Lethrinus nebulosus",
    Genus = "Lethrinus",
    Species = "nebulosus",
    Family = "Lethrinidae",
    Order = "Eupercaria",
    Class = "Teleostei",
    server = "fishbase"
  )

  taxa <- data.frame(
    alpha3_code = c("EMP", "CJX"),
    # Caesionidae is the 26.06 failure mode: a family name that is real but
    # carries no species, so it silently expands to nothing.
    scientific_name = c("Lethrinidae", "Caesionidae")
  )

  testthat::local_mocked_bindings(
    get_taxa_backbone = function(...) backbone
  )

  # logger writes to stderr rather than raising a condition.
  logs <- utils::capture.output(
    out <- expand_taxonomic_info(taxa),
    type = "message"
  )
  expect_match(paste(logs, collapse = " "), "Caesionidae")
  expect_equal(out$alpha3_code, "EMP")
})
