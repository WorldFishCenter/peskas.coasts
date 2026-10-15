# Generated with Claude (Cowork) — review before committing.
# Group-level assessment layer for the stock-assessment framework.
#
# The per-species LBB track only reaches taxa resolved to a binomial with a
# FishBase growth estimate. In the Peskas coasts feeds that is a small slice of
# the catch — most landings are recorded at genus or family level (Decapterus
# spp, Carangidae, Clupeidae ...). This module gives those coarse taxa a
# length-based read by (a) building a *group* life-history prior from the
# FishBase species that make up the taxon and occur in the country's FAO
# area(s), and (b) running the existing Froese length indicators on the taxon's
# pooled lengths. Output is descriptive (share mature / near-optimal / mega,
# mean-length-to-optimal) plus a coarse status and a confidence grade — not a
# single-species biomass estimate, which a mixed aggregate cannot support.
#
# Prior rule ("anchor to the catch", chosen 2026-10-15): a group prior is a
# median over the taxon's regional member species, but the member pool can miss
# the species actually fished. We therefore floor Linf at the observed catch
# (L99 / 0.95) so it can never sit below the fish we measured (fixes families
# whose pool is dominated by smaller species, e.g. Clupeidae). We do NOT shrink
# Linf when the pool overshoots the catch, because an all-small catch is
# genuinely ambiguous — a small species, or heavy juvenile fishing of a large
# one — and shrinking would erase a real overfishing signal; that case is
# flagged low-confidence instead.

#' Taxonomic Rank of a Coarse Taxon Label
#'
#' Classifies a non-binomial landing label as a genus aggregate (`"Decapterus
#' spp"`), a family (`"Carangidae"`) or something coarser/outside FishBase's
#' finfish backbone (orders, classes, informal groupings, invertebrate
#' infraorders). Group priors are trustworthy for genera, usable with care for
#' families, and not attempted for the rest.
#'
#' @param x Character vector of scientific-name labels.
#'
#' @return A character vector: `"genus"`, `"family"` or `"other"`.
#'
#' @keywords internal
.group_rank <- function(x) {
  x <- as.character(x)
  genus <- grepl(" spp?$", x)
  core <- stringr::str_squish(stringr::str_remove(x, "\\s*\\([^)]*\\)"))
  family <- grepl("idae$", core)
  dplyr::case_when(genus ~ "genus", family ~ "family", TRUE ~ "other")
}

#' Representative Regional Species for FishBase-Gap Families
#'
#' Some families resolve to no species under current FishBase / SeaLifeBase
#' releases even though they are real and important in the catch — 26.06/26.07
#' keep `Caesionidae` and `Scaridae` as family names but attach no species, so
#' [expand_taxonomic_info()] drops them. This supplies a small, hand-picked set
#' of common Indo-Pacific members so those taxa still get a group prior. Extend
#' as needed; names must be ones `rfishbase::estimate()` resolves.
#'
#' @param name A single coarse taxon label.
#'
#' @return A character vector of binomial species names (possibly empty).
#'
#' @keywords internal
.group_fallback_species <- function(name) {
  core <- stringr::str_squish(stringr::str_remove(name, "\\s*\\([^)]*\\)"))
  switch(
    core,
    Caesionidae = c(
      "Caesio cuning", "Caesio teres", "Caesio lunaris",
      "Pterocaesio chrysozona", "Pterocaesio tile", "Pterocaesio pisang"
    ),
    Scaridae = c(
      "Scarus ghobban", "Scarus rivulatus", "Scarus psittacus",
      "Chlorurus sordidus", "Hipposcarus harid", "Scarus frenatus"
    ),
    character(0)
  )
}

#' Build Group-Level Life-History Priors from FishBase
#'
#' For each coarse taxon (genus or family label), expands it to its member
#' species via [expand_taxonomic_info()], restricts those to the requested FAO
#' area(s) with [filter_by_fao_area()] (the "regional species" rule), pulls the
#' per-species priors with [get_life_history_priors()], and aggregates them to a
#' single representative prior (median `linf`, `mk`, `lm`). Families that resolve
#' to no species fall back to [.group_fallback_species()]. The spread of member
#' `linf` and the taxon's rank set a confidence grade. The returned `linf` is
#' the FishBase member median; it is anchored to the observed catch later, in
#' [assess_group_stock()].
#'
#' @param coarse_taxa A tibble with `scientific_name` and `catch_taxon` (the FAO
#'   3-alpha code) columns — one row per coarse taxon to prior.
#' @param fao_areas Integer vector of FAO major fishing area codes to restrict
#'   members to (e.g. `c(51, 57)` for the WIO, `71` for Timor-Leste). `NULL`
#'   skips the regional filter and aggregates over all member species, flagged
#'   as lower confidence.
#' @param version FishBase / SeaLifeBase release. See [resolve_db_version()].
#' @param default_mk Fallback M/K when FishBase has none. Default 1.5.
#'
#' @return A tibble, one row per input taxon: `scientific_name`, `rank`,
#'   `linf`, `mk`, `lm`, `n_species`, `linf_cv`, `source`, `confidence`.
#'
#' @keywords workflow modeling
#' @export
get_group_priors <- function(
  coarse_taxa,
  fao_areas = NULL,
  version = "latest",
  default_mk = 1.5
) {
  coarse_taxa <- coarse_taxa |>
    dplyr::distinct(.data$scientific_name, .data$catch_taxon) |>
    dplyr::mutate(rank = .group_rank(.data$scientific_name))

  assessable <- coarse_taxa |> dplyr::filter(.data$rank %in% c("genus", "family"))
  if (nrow(assessable) == 0) {
    logger::log_warn("No genus/family-level coarse taxa to prior.")
    return(tibble::tibble())
  }

  expanded <- tryCatch(
    expand_taxonomic_info(
      data = assessable |>
        dplyr::transmute(
          alpha3_code = .data$catch_taxon %||% NA_character_,
          scientific_name = .data$scientific_name
        ),
      strip_parentheticals = TRUE,
      version = version
    ),
    error = function(e) {
      logger::log_warn("expand_taxonomic_info() failed: {conditionMessage(e)}")
      tibble::tibble()
    }
  )

  regional <- !is.null(fao_areas)
  if (regional && nrow(expanded) > 0) {
    expanded <- filter_by_fao_area(expanded, fao_areas, version = version)
  }

  mem <- if (nrow(expanded) > 0) {
    expanded |>
      dplyr::transmute(scientific_name = .data$original_name, species = .data$species_found)
  } else {
    tibble::tibble(scientific_name = character(), species = character())
  }

  # Fallback for families that resolve to no species under the current release.
  no_mem <- setdiff(assessable$scientific_name, unique(mem$scientific_name))
  fb_used <- character(0)
  if (length(no_mem) > 0) {
    fb <- purrr::map_dfr(no_mem, function(nm) {
      sp <- .group_fallback_species(nm)
      if (length(sp) > 0) {
        tibble::tibble(scientific_name = nm, species = sp)
      } else {
        tibble::tibble(scientific_name = character(), species = character())
      }
    })
    if (nrow(fb) > 0) {
      mem <- dplyr::bind_rows(mem, fb)
      fb_used <- unique(fb$scientific_name)
    }
  }
  mem <- dplyr::distinct(mem)
  if (nrow(mem) == 0) {
    logger::log_warn("Group priors: no member species resolved for any taxon.")
    return(tibble::tibble())
  }

  sp_priors <- get_life_history_priors(unique(mem$species), default_mk = default_mk)

  grouped <- mem |>
    dplyr::left_join(
      sp_priors |> dplyr::select("scientific_name", "linf", "mk", "lm"),
      by = c("species" = "scientific_name")
    ) |>
    dplyr::group_by(.data$scientific_name) |>
    dplyr::summarise(
      linf = stats::median(.data$linf, na.rm = TRUE),
      mk = stats::median(.data$mk, na.rm = TRUE),
      lm = stats::median(.data$lm, na.rm = TRUE),
      n_species = sum(!is.na(.data$linf)),
      linf_cv = if (sum(!is.na(.data$linf)) > 1) {
        stats::sd(.data$linf, na.rm = TRUE) / mean(.data$linf, na.rm = TRUE)
      } else {
        NA_real_
      },
      .groups = "drop"
    ) |>
    dplyr::filter(.data$n_species > 0, !is.na(.data$linf)) |>
    dplyr::mutate(
      mk = dplyr::if_else(is.na(.data$mk) | .data$mk <= 0, .env$default_mk, .data$mk)
    )

  out <- assessable |>
    dplyr::select("scientific_name", "rank") |>
    dplyr::inner_join(grouped, by = "scientific_name") |>
    dplyr::mutate(
      source = dplyr::case_when(
        .data$scientific_name %in% .env$fb_used ~ "fishbase_fallback",
        regional ~ "fishbase_regional",
        TRUE ~ "fishbase_global"
      ),
      confidence = dplyr::case_when(
        .data$rank == "genus" & (is.na(.data$linf_cv) | .data$linf_cv < 0.35) ~ "high",
        .data$rank == "genus" ~ "moderate",
        .data$rank == "family" & (!is.na(.data$linf_cv) & .data$linf_cv < 0.35) ~ "moderate",
        TRUE ~ "low"
      )
    )

  logger::log_info(
    "Group priors built for {nrow(out)} taxa ",
    "({sum(out$rank == 'genus')} genus, {sum(out$rank == 'family')} family; ",
    "{length(fb_used)} via fallback; source {if (regional) 'regional' else 'global'})."
  )
  out
}

#' Coarse Status from Froese Length Indicators
#'
#' Collapses the Froese indicators into a single, SSF-interpretable label.
#' Driven by the share of the catch that is mature and the mean-length-to-
#' optimal ratio — the two signals most robust to a mixed-species aggregate.
#'
#' @param p_mature Share of the catch at or above length-at-maturity.
#' @param lmean_lopt Mean length divided by optimal length.
#'
#' @return `"healthy"`, `"borderline"` or `"overfishing signal"`.
#'
#' @keywords internal
.group_status <- function(p_mature, lmean_lopt) {
  dplyr::case_when(
    is.na(p_mature) | is.na(lmean_lopt) ~ NA_character_,
    p_mature >= 0.7 & lmean_lopt >= 0.9 ~ "healthy",
    p_mature < 0.4 | lmean_lopt < 0.7 ~ "overfishing signal",
    TRUE ~ "borderline"
  )
}

#' Froese Length-at-Maturity from Linf
#' @keywords internal
.lm_from_linf <- function(linf) 10^(0.8979 * log10(linf) - 0.0782)

#' Assess One Coarse Taxon at the Group Level
#'
#' Runs the Froese length indicators on a coarse taxon's pooled lengths against
#' a *catch-anchored* group prior. The FishBase member-median `linf` is floored
#' to the observed catch (`L99 / 0.95`) so it never sits below the measured fish
#' (`prior_note = "linf_floored_to_catch"`). Where the member pool instead sits
#' far above the catch (`lmax < 0.6 * linf`), the read is marked
#' `prior_note = "catch_below_prior"` and `confidence = "low"`, because an
#' all-small catch cannot be told apart from heavy juvenile fishing on length
#' alone. The `1.2 x Linf` implausible-length cull is deliberately skipped — it
#' is a single-species guard, not valid for an aggregate.
#'
#' @param landings A landing-feed tibble for a single country.
#' @param scientific_name The coarse taxon label.
#' @param group_prior A one-row tibble from [get_group_priors()].
#' @param min_measured Minimum pooled measured lengths to attempt a read.
#'   Default 100.
#'
#' @return A one-row tibble: `scientific_name`, `scale` (`"group"`), `method`
#'   (`"group-lbi"`), the Froese indicator columns, `group_status`,
#'   `n_measured`, `frac_measured`, `n_species`, `confidence`, `prior_note`,
#'   `quality`.
#'
#' @keywords workflow modeling
#' @export
assess_group_stock <- function(
  landings,
  scientific_name,
  group_prior,
  min_measured = 100L
) {
  cov <- .length_coverage(landings, scientific_name)
  # No 1.2 x Linf cull for aggregates — only non-positive lengths are dropped.
  lengths <- cov$lengths[!is.na(cov$lengths) & cov$lengths > 0]

  base <- tibble::tibble(
    scientific_name = scientific_name,
    scale = "group",
    method = "group-lbi",
    n_measured = length(lengths),
    frac_measured = cov$coverage$frac_measured,
    n_species = if (nrow(group_prior) > 0) group_prior$n_species else NA_integer_,
    confidence = if (nrow(group_prior) > 0) group_prior$confidence else NA_character_
  )

  linf_fb <- if (nrow(group_prior) > 0) group_prior$linf else NA_real_
  if (nrow(group_prior) == 0 || is.na(linf_fb) || length(lengths) < min_measured) {
    return(dplyr::mutate(
      base, quality = "unknown", group_status = NA_character_, prior_note = NA_character_
    ))
  }

  # --- anchor the prior to the catch ---
  l99 <- as.numeric(stats::quantile(lengths, 0.99, type = 7))
  linf_anchored <- max(linf_fb, l99 / 0.95)
  floored <- linf_anchored > linf_fb * 1.001
  lm_adj <- if (floored) .lm_from_linf(linf_anchored) else group_prior$lm

  adj_prior <- tibble::tibble(
    scientific_name = scientific_name,
    linf = linf_anchored, mk = group_prior$mk, lm = lm_adj
  )
  ind <- assess_length_indicators(lengths, adj_prior)

  # Ambiguity: when the prior was NOT floored (the member pool sits at or above
  # the catch) yet almost nothing reaches maturity, a prior that overshoots the
  # species actually fished cannot be told from a genuinely juvenile-dominated
  # catch — flag low-confidence rather than claim a firm status. When the prior
  # WAS floored to the catch, a low mature share is a real signal and keeps its
  # graded confidence.
  ambiguous <- !floored && isTRUE(ind$p_mature < 0.10)
  prior_note <- dplyr::case_when(
    floored ~ "linf_floored_to_catch",
    ambiguous ~ "catch_below_prior",
    TRUE ~ "fishbase_member_median"
  )
  confidence <- if (ambiguous) "low" else group_prior$confidence

  flag <- .flag_assessment_quality(cov$coverage, adj_prior, ci_width = NA_real_)
  # A mixed aggregate should not read as firmly as a single-species fit.
  if (flag == "pass" && !identical(confidence, "high")) {
    flag <- "warn"
  }

  base |>
    dplyr::mutate(
      confidence = .env$confidence,
      lopt = ind$lopt, lm = ind$lm,
      p_mature = ind$p_mature, p_opt = ind$p_opt, p_mega = ind$p_mega,
      lmean_lopt = ind$lmean_lopt,
      group_status = .group_status(ind$p_mature, ind$lmean_lopt),
      prior_note = .env$prior_note,
      quality = .env$flag
    )
}

#' Run the Group-Level Layer Across Countries
#'
#' For each country, ranks the coarse (non-species) taxa by economic
#' importance, builds regional group priors, and assesses the top
#' `n_groups` of them. Intended to be called by [run_stock_assessment()] after
#' the per-species track, but usable standalone.
#'
#' @param landings A landing-feed tibble with a `country` column.
#' @param n_groups Coarse taxa to assess per country. Default 15.
#' @param min_records Minimum landing records to be eligible. Default 30.
#' @param min_measured Minimum pooled measured lengths to attempt a read.
#'   Default 100.
#' @param fao_areas Named list mapping country key to an integer vector of FAO
#'   area codes (e.g. `list(kenya = c(51, 57), timor = 71)`). Countries absent
#'   from the list skip the regional filter.
#' @param version FishBase / SeaLifeBase release. See [resolve_db_version()].
#'
#' @return A tibble of per-taxon group assessments with a `country` column.
#'
#' @keywords workflow modeling
#' @export
assess_group_layer <- function(
  landings,
  n_groups = 15L,
  min_records = 30L,
  min_measured = 100L,
  fao_areas = NULL,
  version = "latest"
) {
  ranked <- .species_economic_rank(landings) |>
    dplyr::filter(
      .data$n_records >= .env$min_records,
      !.is_species_level(.data$scientific_name),
      .group_rank(.data$scientific_name) %in% c("genus", "family")
    ) |>
    dplyr::group_by(.data$country) |>
    dplyr::slice_min(.data$rank, n = n_groups, with_ties = FALSE) |>
    dplyr::ungroup()

  if (nrow(ranked) == 0) {
    logger::log_warn("Group layer: no eligible coarse taxa.")
    return(tibble::tibble())
  }

  purrr::map_dfr(unique(ranked$country), function(country) {
    cc <- ranked |> dplyr::filter(.data$country == .env$country)
    land_c <- landings |> dplyr::filter(.data$country == .env$country)
    areas <- if (is.list(fao_areas)) fao_areas[[country]] else fao_areas

    priors <- get_group_priors(
      cc |> dplyr::select("scientific_name", "catch_taxon"),
      fao_areas = areas, version = version
    )

    purrr::map_dfr(cc$scientific_name, function(sci) {
      gp <- priors |> dplyr::filter(.data$scientific_name == .env$sci)
      assess_group_stock(land_c, sci, gp, min_measured = min_measured) |>
        dplyr::mutate(country = .env$country, .before = 1)
    }) |>
      dplyr::left_join(
        cc |> dplyr::select("scientific_name", "catch_taxon", "rank", "rank_basis"),
        by = "scientific_name"
      )
  })
}
