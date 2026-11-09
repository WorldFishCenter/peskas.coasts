# Generated with Claude (Cowork) — review before committing.
# Production-based exploitation layer (Kolding's modified Cadima estimator).
#
# Kolding (2026, "Population ecology and sustainable fisheries yields") argues
# for judging exploitation against CURRENT biological production rather than a
# theoretical MSY. Under approximately stationary conditions P/B ~ Z, so current
# production is P = B * Z, and the modified Cadima sustainable yield is
#
#   SY = x * B * Z,   x = share of production that fishing may take
#
# with x ~ 0.25 for lower trophic levels and ~ 0.5 for top predators
# (heuristics, not constants). Because current yield is Y = F * B, the biomass
# cancels:
#
#   SY = Y * x / E,   E = F / Z  (exploitation rate, = Y / P)
#
# so a stock's position needs only E (from the length data) and x (from its
# trophic level): E / x > 1 means fishing takes more than its sustainable share
# of production, and x / E is the sustainable catch as a multiple of the
# current catch. Absolute tonnes need the fleet-raised catch, see
# [scale_sustainable_yield()].
#
# E is estimated two ways where possible and reported with a band:
#  - Beverton-Holt mean-length Z/K (works for species AND coarse groups),
#    with M/K perturbed +/-25%;
#  - LBB, E = (F/M) / (1 + F/M), with the prior-robustness F/M band if run.
#
# Caveat carried into every result: the validated feed holds ~one length per
# landing, which thins small fish and steepens the apparent mortality, so E is
# biased HIGH. Treat it as direction and rough magnitude.

#' Beverton-Holt Mean-Length Estimate of Z/K
#'
#' Total mortality relative to growth from the mean length of fully selected
#' fish: `Z/K = (Linf - Lmean) / (Lmean - Lc)` (Beverton & Holt 1956), valid
#' for a population near equilibrium with constant mortality above `Lc`. `Lc`,
#' the length of full selection, defaults to the modal length class.
#'
#' @param lengths Numeric vector of measured lengths (cm).
#' @param linf Asymptotic length (cm).
#' @param lc Optional length of full selection (cm). `NULL` uses the mode.
#'
#' @return A list with `zk` and `lc` (both `NA` when not estimable).
#'
#' @keywords internal
.bh_zk <- function(lengths, linf, lc = NULL) {
  na <- list(zk = NA_real_, lc = NA_real_)
  lengths <- lengths[is.finite(lengths) & lengths > 0]
  if (length(lengths) < 20 || is.na(linf) || linf <= 0) {
    return(na)
  }
  if (is.null(lc)) {
    # Lc = lower edge of the modal length class. For data logged in classes
    # (e.g. 5-cm midpoints 17.5, 22.5, ...) bins are centred on the recorded
    # values, so Lc is the class's lower limit (15, 20, ...), not the midpoint;
    # using the midpoint would shrink Lmean - Lc and inflate Z/K.
    res <- .length_resolution(lengths)
    classed <- isTRUE(res >= 2)
    bw <- if (classed) res else max(0.5, linf / 40)
    lo <- if (classed) min(lengths) - res / 2 else floor(min(lengths))
    cnt <- tabulate(floor((lengths - lo) / bw) + 1L)
    lc <- lo + (which.max(cnt) - 1L) * bw
  }
  sel <- lengths[lengths >= lc]
  if (length(sel) < 10) {
    return(na)
  }
  lmean <- mean(sel)
  if (lmean >= linf || lmean <= lc) {
    return(na)
  }
  list(zk = (linf - lmean) / (lmean - lc), lc = lc)
}

#' Exploitation Rate from Z/K and M/K
#' @keywords internal
.e_from_zk <- function(zk, mk) {
  dplyr::case_when(
    is.na(zk) | is.na(mk) ~ NA_real_,
    zk <= mk ~ 0,
    TRUE ~ 1 - mk / zk
  )
}

#' Harvestable Share of Production by Trophic Level
#'
#' Kolding's heuristic exploitation fraction `x`: about 0.25 of production for
#' lower trophic levels (the rest feeds natural predators) and 0.5 for top
#' predators. When the trophic level is unknown the conservative `x_low` is
#' used.
#'
#' @param trophic_level Numeric vector of FishBase trophic levels.
#' @param x_low,x_top Fractions for lower and top trophic levels.
#' @param tl_top Trophic level from which a taxon counts as a top predator.
#'
#' @return Numeric vector of `x`.
#'
#' @keywords internal
.harvest_fraction <- function(trophic_level, x_low = 0.25, x_top = 0.5,
                              tl_top = 4) {
  dplyr::if_else(!is.na(trophic_level) & trophic_level >= tl_top, x_top, x_low)
}

#' Estimate the Exploitation Rate E = F/Z from Lengths
#'
#' Combines the Beverton-Holt mean-length estimate (with M/K perturbed by
#' `mk_mult`) and, when supplied, the LBB F/M (with its robustness band). The
#' point estimate is the median of the available central estimates; the band
#' spans every estimate computed.
#'
#' @param lengths Numeric vector of measured lengths (cm), already cleaned.
#' @param priors A one-row life-history tibble (`linf`, `mk`).
#' @param fm,fm_lo,fm_hi Optional LBB F/M and its robustness band.
#' @param mk_mult M/K multipliers for the Beverton-Holt band. Default
#'   `c(0.75, 1.25)`.
#' @param catch_linf Logical. Also run Beverton-Holt with the catch-implied
#'   L-inf (`L99 / 0.95`) and include it in the band when it differs from the
#'   prior by more than 5\%. Default `TRUE`.
#'
#' @return A one-row tibble: `zk_bh`, `lc_bh`, `e_bh`, `e_lbb`, `e`, `e_lo`,
#'   `e_hi`, `e_methods`.
#'
#' @keywords workflow modeling
#' @export
estimate_exploitation <- function(lengths, priors, fm = NA_real_,
                                  fm_lo = NA_real_, fm_hi = NA_real_,
                                  mk_mult = c(0.75, 1.25),
                                  catch_linf = TRUE) {
  mk <- if (!is.null(priors$mk) && isTRUE(priors$mk > 0)) priors$mk else 1.5
  bh <- .bh_zk(lengths, priors$linf)

  e_bh <- .e_from_zk(bh$zk, mk)
  e_bh_band <- .e_from_zk(bh$zk, mk * mk_mult)
  # L-inf uncertainty: the catch-implied value bounds the FishBase prior from
  # the other side (see lbb_robustness()).
  if (isTRUE(catch_linf) && length(lengths) >= 20 && isTRUE(priors$linf > 0)) {
    linf_c <- as.numeric(stats::quantile(lengths, 0.99, type = 7)) / 0.95
    if (abs(linf_c / priors$linf - 1) > 0.05) {
      e_bh_band <- c(e_bh_band, .e_from_zk(.bh_zk(lengths, linf_c)$zk, mk))
    }
  }
  fm_to_e <- function(x) dplyr::if_else(is.na(x) | x < 0, NA_real_, x / (1 + x))
  e_lbb <- fm_to_e(fm)
  e_lbb_band <- fm_to_e(c(fm_lo, fm_hi))

  central <- stats::na.omit(c(e_bh, e_lbb))
  all_e <- stats::na.omit(c(central, e_bh_band, e_lbb_band))

  tibble::tibble(
    zk_bh = bh$zk,
    lc_bh = bh$lc,
    e_bh = e_bh,
    e_lbb = e_lbb,
    e = if (length(central) > 0) stats::median(central) else NA_real_,
    e_lo = if (length(all_e) > 0) min(all_e) else NA_real_,
    e_hi = if (length(all_e) > 0) max(all_e) else NA_real_,
    e_methods = paste(c("bh", "lbb")[!is.na(c(e_bh, e_lbb))], collapse = "+")
  )
}

#' Production-Based Exploitation Status (Modified Cadima)
#'
#' Positions a stock against Kolding's modified Cadima rule. `e_x = E / x` is
#' the share of production taken relative to the sustainable share (above 1 =
#' too much); `sy_ratio = x / E` is the sustainable catch as a multiple of the
#' current catch (e.g. 0.4 = cut catch to 40\%; 1.6 = room for 60\% more).
#' `production_band` says whether the whole E band lies on one side of the
#' line (`"clear"`) or straddles it (`"straddles"`, i.e. borderline).
#'
#' @inheritParams estimate_exploitation
#' @param trophic_level FishBase trophic level of the taxon (may be `NA`).
#' @param ... Passed to [.harvest_fraction()] (`x_low`, `x_top`, `tl_top`).
#'
#' @return A one-row tibble with `method = "production"`, the
#'   [estimate_exploitation()] columns, `trophic_level`, `x`, `x_basis`,
#'   `e_x`, `sy_ratio`, `production_status` and `production_band`.
#'
#' @keywords workflow modeling
#' @export
assess_production <- function(lengths, priors, trophic_level = NA_real_,
                              fm = NA_real_, fm_lo = NA_real_,
                              fm_hi = NA_real_, ...) {
  ex <- estimate_exploitation(lengths, priors, fm = fm, fm_lo = fm_lo, fm_hi = fm_hi)
  x <- .harvest_fraction(trophic_level, ...)

  ex |>
    dplyr::mutate(
      method = "production",
      trophic_level = .env$trophic_level,
      x = .env$x,
      x_basis = dplyr::if_else(is.na(.env$trophic_level), "default_low", "trophic_level"),
      e_x = .data$e / .data$x,
      sy_ratio = dplyr::if_else(.data$e > 0, .data$x / .data$e, NA_real_),
      production_status = dplyr::case_when(
        is.na(.data$e_x) ~ NA_character_,
        .data$e_x <= 1 ~ "within sustainable share",
        .data$e_x <= 1.5 ~ "above sustainable share",
        TRUE ~ "well above sustainable share"
      ),
      production_band = dplyr::case_when(
        is.na(.data$e_lo) | is.na(.data$e_hi) ~ NA_character_,
        (.data$e_lo / .data$x > 1) == (.data$e_hi / .data$x > 1) ~ "clear",
        TRUE ~ "straddles"
      ),
      .before = 1
    )
}

#' Trophic Levels for Species and Coarse Taxa from FishBase
#'
#' Resolves each name to its member species (a binomial to itself; a genus or
#' family to its members, optionally restricted to FAO areas for coarse taxa),
#' reads `FoodTroph` from the FishBase / SeaLifeBase `ecology` table and returns
#' the member median. Families that resolve to no species use the same fallback
#' members as the group layer.
#'
#' @param scientific_names Character vector of taxon names.
#' @param fao_areas Optional FAO area codes for coarse taxa.
#' @param version FishBase / SeaLifeBase release. See [resolve_db_version()].
#'
#' @return A tibble: `scientific_name`, `trophic_level`, `n_species_tl`. Empty
#'   when FishBase cannot be read (callers then fall back to `x_low`).
#'
#' @keywords workflow modeling
#' @export
get_trophic_levels <- function(scientific_names, fao_areas = NULL,
                               version = "latest") {
  empty <- tibble::tibble(
    scientific_name = character(), trophic_level = numeric(),
    n_species_tl = integer()
  )
  nms <- unique(stats::na.omit(scientific_names))
  if (length(nms) == 0) {
    return(empty)
  }

  expand <- function(names) {
    expand_taxonomic_info(
      data.frame(alpha3_code = NA_character_, scientific_name = names),
      strip_parentheticals = TRUE, version = version
    )
  }

  out <- tryCatch(
    {
      mem <- expand(nms)
      coarse <- !.is_species_level(mem$original_name)
      if (!is.null(fao_areas) && any(coarse)) {
        mem <- dplyr::bind_rows(
          mem[!coarse, ],
          filter_by_fao_area(mem[coarse, ], fao_areas, version = version)
        )
      }

      no_mem <- setdiff(nms, unique(mem$original_name))
      fb <- purrr::map_dfr(no_mem, function(nm) {
        sp <- .group_fallback_species(nm)
        if (length(sp) == 0) {
          return(tibble::tibble())
        }
        expand(sp) |> dplyr::mutate(original_name = .env$nm)
      })
      mem <- dplyr::bind_rows(mem, fb) |>
        dplyr::distinct(.data$original_name, .data$SpecCode, .data$server)

      troph <- get_combined_tbl("ecology", version = version) |>
        dplyr::filter(.data$SpecCode %in% mem$SpecCode, !is.na(.data$FoodTroph)) |>
        dplyr::group_by(.data$SpecCode, .data$server) |>
        dplyr::summarise(tl = stats::median(.data$FoodTroph), .groups = "drop")

      mem |>
        dplyr::inner_join(troph, by = c("SpecCode", "server")) |>
        dplyr::group_by(scientific_name = .data$original_name) |>
        dplyr::summarise(
          trophic_level = stats::median(.data$tl),
          n_species_tl = dplyr::n(),
          .groups = "drop"
        )
    },
    error = function(e) {
      logger::log_warn("Trophic levels unavailable: {conditionMessage(e)}")
      empty
    }
  )
  logger::log_info("Trophic levels resolved for {nrow(out)} of {length(nms)} taxa.")
  out
}

#' Share of Sampled Catch Weight per Taxon and Country
#' @keywords internal
.catch_share <- function(landings) {
  landings |>
    dplyr::group_by(.data$country, .data$scientific_name) |>
    dplyr::summarise(catch_kg_sampled = sum(.data$catch_kg, na.rm = TRUE), .groups = "drop") |>
    dplyr::group_by(.data$country) |>
    dplyr::mutate(catch_share = .data$catch_kg_sampled / sum(.data$catch_kg_sampled)) |>
    dplyr::ungroup()
}

#' Scale Production Results to Tonnes with Fleet-Raised Catch
#'
#' Turns the relative production results into catch volumes using a country
#' total from the Peskas fleet estimates (e.g. summed
#' `estimated_total_catch_kg` from [calculate_district_totals()]): current
#' catch = total x `catch_share`; sustainable yield = current x `sy_ratio`.
#'
#' @param results Output of [run_stock_assessment()] (needs `catch_share`,
#'   `sy_ratio`).
#' @param total_catch A tibble with `country` and `total_catch_kg` for the
#'   same period as the assessment.
#'
#' @return `results` with `current_catch_kg` and `sustainable_yield_kg`.
#'
#' @keywords workflow modeling
#' @export
scale_sustainable_yield <- function(results, total_catch) {
  results |>
    dplyr::left_join(
      dplyr::select(total_catch, "country", "total_catch_kg"),
      by = "country"
    ) |>
    dplyr::mutate(
      current_catch_kg = .data$total_catch_kg * .data$catch_share,
      sustainable_yield_kg = .data$current_catch_kg * .data$sy_ratio
    ) |>
    dplyr::select(-"total_catch_kg")
}
