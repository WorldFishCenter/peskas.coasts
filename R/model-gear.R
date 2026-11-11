# Gear resolution for the length-based assessments.
#
# Small-scale gears select sizes very differently. Traps and gill nets are
# dome-shaped: they under-catch the largest fish, so a length sample from them
# looks like heavy mortality even when the stock is lightly fished (Zanzibar
# Siganus sutor: trap Z/K ~4.9 vs hand line ~1.3; Timor Lethrinidae: gill net
# ~9.7 vs hand line ~2.7). Two tools follow:
#  - a gear profile and selectivity flag on every read (which gear dominates the
#    sample, and how much of it comes from dome-selective gears);
#  - optional per-gear reads, with a reference gear = the gear whose catch
#    reaches closest to Linf (least truncated by selectivity).

#' Gears Treated as Dome-Selective (Regex)
#' @keywords internal
.SELECTIVE_GEAR_PATTERN <- "trap|gill|tangle"

#' Fish Represented by Each Length Record
#'
#' A record stands for `n_fish` fish of the recorded length; a feed without
#' the column, and missing or invalid counts, count as one fish. `n_catch` is
#' the catch's place on the form, not a count.
#'
#' @param measured Landing rows that carry a length.
#'
#' @return An integer vector, one weight per row.
#'
#' @keywords internal
.n_fish <- function(measured) {
  if (!"n_fish" %in% names(measured)) {
    return(rep(1L, nrow(measured)))
  }
  n <- suppressWarnings(as.integer(measured$n_fish))
  dplyr::if_else(is.na(n) | n < 1L, 1L, n)
}

#' Measured Lengths of One Taxon, with Fish Weights
#' @keywords internal
.measured_rows <- function(landings, scientific_name) {
  m <- landings |>
    dplyr::filter(
      .data$scientific_name == .env$scientific_name,
      !is.na(.data$length_cm), .data$length_cm > 0
    )
  m$n_fish <- .n_fish(m)
  m
}

#' Which Gears Produced a Taxon's Length Sample
#'
#' @param landings A landing-feed tibble (one country).
#' @param scientific_name Taxon name.
#'
#' @return A one-row tibble: `main_gear`, `main_gear_share` (share of measured
#'   fish), `selective_gear_share` (share from traps / gill / tangle nets) and
#'   `selectivity_flag` (`TRUE` when that share exceeds 0.7).
#'
#' @keywords internal
.gear_profile <- function(landings, scientific_name) {
  na_row <- tibble::tibble(
    main_gear = NA_character_, main_gear_share = NA_real_,
    selective_gear_share = NA_real_, selectivity_flag = NA
  )
  if (!"gear" %in% names(landings)) {
    return(na_row)
  }
  m <- .measured_rows(landings, scientific_name)
  if (nrow(m) == 0) {
    return(na_row)
  }
  gear <- dplyr::coalesce(m$gear, "unknown")
  by_gear <- tapply(m$n_fish, gear, sum)
  sel <- sum(m$n_fish[grepl(.SELECTIVE_GEAR_PATTERN, gear, ignore.case = TRUE)]) /
    sum(m$n_fish)
  tibble::tibble(
    main_gear = names(by_gear)[which.max(by_gear)],
    main_gear_share = max(by_gear) / sum(by_gear),
    selective_gear_share = sel,
    selectivity_flag = sel > 0.7
  )
}

#' Gears with Enough Trips Measured for Their Own Read
#'
#' @inheritParams .gear_profile
#' @param min_trips Minimum trips with measured fish per gear. Default 100,
#'   where LBB precision starts to plateau.
#'
#' @return Character vector of gears; empty unless at least two gears qualify
#'   (with one, the pooled read already is that gear's read).
#'
#' @keywords internal
.gears_with_trips <- function(landings, scientific_name, min_trips = 100L) {
  if (!"gear" %in% names(landings)) {
    return(character(0))
  }
  m <- .measured_rows(landings, scientific_name)
  m <- m[!is.na(m$gear), ]
  if (nrow(m) == 0) {
    return(character(0))
  }
  by_gear <- tapply(m$trip_id, m$gear, dplyr::n_distinct)
  ok <- names(by_gear)[by_gear >= min_trips]
  if (length(ok) < 2) character(0) else ok
}

#' Mark the Reference Gear Among Per-Gear Reads
#'
#' For each country x scale x taxon with per-gear rows, the reference gear is
#' the assessed gear with the highest `size_reach` (99th-percentile length /
#' Linf): the sample least truncated by gear selectivity, and so the best basis
#' for the headline status. Only species and high-confidence genera get one: in
#' a mixed family, gears catch different member species, so a larger reach
#' reflects species composition rather than selectivity (Carangidae and
#' Clupeidae can reach more than 1.5 times their member-median Linf).
#'
#' @param results Assessment rows with `gear`, `size_reach` and `quality`.
#'
#' @return `results` with a logical `reference_gear` (`NA` on pooled rows).
#'
#' @keywords internal
.mark_reference_gear <- function(results) {
  if (!all(c("gear", "size_reach") %in% names(results))) {
    return(results)
  }
  ref <- results |>
    dplyr::filter(
      .data$gear != "all", !is.na(.data$size_reach), .data$quality != "unknown",
      .data$scale == "species" | .col_or(results, "confidence", NA_character_) %in% "high"
    ) |>
    dplyr::distinct(
      .data$country, .data$scale, .data$scientific_name, .data$gear, .data$size_reach
    ) |>
    dplyr::group_by(.data$country, .data$scale, .data$scientific_name) |>
    dplyr::slice_max(.data$size_reach, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select("country", "scale", "scientific_name", ref_gear = "gear")

  results |>
    dplyr::left_join(ref, by = c("country", "scale", "scientific_name")) |>
    dplyr::mutate(
      reference_gear = dplyr::if_else(
        .data$gear == "all", NA, .data$gear == dplyr::coalesce(.data$ref_gear, "")
      )
    ) |>
    dplyr::select(-"ref_gear")
}
