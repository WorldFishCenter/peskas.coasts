ll <- function(...) {
  dplyr::tribble(
    ~species_found, ~server, ~Length1, ~Length2, ~aL, ~bL,
    ...
  )
}
lw <- function(...) {
  dplyr::tribble(
    ~alpha3_code, ~species_found, ~server, ~Type, ~a, ~b,
    ...
  )
}

test_that("a standard-length pair is restated as a * ratio^b", {
  out <- convert_lw_to_tl(
    lw("EMP", "Sp one", "fishbase", "SL", 0.01, 3),
    ll("Sp one", "fishbase", "SL", "TL", 0, 0.8)
  )
  # SL = 0.8 * TL, so W = 0.01 * (0.8 TL)^3 = 0.00512 * TL^3
  expect_equal(out$a, 0.01 * 0.8^3)
  expect_equal(out$b, 3) # the exponent never moves
  expect_equal(out$Type, "TL")
})

test_that("POPLL is read as Length1 = aL + bL * Length2, not the reverse", {
  # The same conversion written the other way round must give the same answer.
  # Reading the fit backwards would give 1/0.8 = 1.25 and inflate a by ~1.95x.
  forward <- convert_lw_to_tl(
    lw("EMP", "Sp one", "fishbase", "SL", 0.01, 3),
    ll("Sp one", "fishbase", "SL", "TL", 0, 0.8)
  )
  reverse <- convert_lw_to_tl(
    lw("EMP", "Sp one", "fishbase", "SL", 0.01, 3),
    ll("Sp one", "fishbase", "TL", "SL", 0, 1 / 0.8)
  )
  expect_equal(forward$a, reverse$a)
  expect_lt(reverse$a, 0.01) # SL < TL, so restating must shrink a
})

test_that("rows with no usable conversion pass through untouched", {
  out <- convert_lw_to_tl(
    lw(
      "EMP", "Sp one", "fishbase", "FL", 0.02, 3.1, # no conversion published
      "EMP", "Sp one", "fishbase", "TL", 0.03, 3.0, # already total length
      "OCZ", "Sp two", "sealifebase", "SL", 0.04, 2.9 # different species
    ),
    ll("Sp one", "fishbase", "SL", "TL", 0, 0.8)
  )
  expect_equal(out$a, c(0.02, 0.03, 0.04))
  expect_equal(out$Type, c("FL", "TL", "SL"))
})

test_that("several published conversions collapse to their median", {
  out <- convert_lw_to_tl(
    lw("EMP", "Sp one", "fishbase", "SL", 0.01, 3),
    ll(
      "Sp one", "fishbase", "SL", "TL", 0, 0.7,
      "Sp one", "fishbase", "SL", "TL", 0, 0.8,
      "Sp one", "fishbase", "SL", "TL", 0, 0.9
    )
  )
  expect_equal(out$a, 0.01 * 0.8^3)
})

test_that("a fit with a large intercept is not treated as a pure ratio", {
  args <- list(
    lw("EMP", "Sp one", "fishbase", "SL", 0.01, 3),
    ll("Sp one", "fishbase", "SL", "TL", 4.2, 0.8)
  )
  expect_equal(do.call(convert_lw_to_tl, args)$Type, "SL") # dropped, passes through
  expect_equal(
    do.call(convert_lw_to_tl, c(args, max_intercept = 5))$Type,
    "TL"
  )
})

test_that("the species and server keys must both match", {
  out <- convert_lw_to_tl(
    lw("EMP", "Sp one", "fishbase", "SL", 0.01, 3),
    ll("Sp one", "sealifebase", "SL", "TL", 0, 0.8) # same species, other server
  )
  expect_equal(out$a, 0.01)
  expect_equal(out$Type, "SL")
})
