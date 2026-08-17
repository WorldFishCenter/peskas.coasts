#' Rank Species by Economic Importance within Each Country
#'
#' Computes a per-country, per-species economic ranking from the landing feed.
#' Revenue is taken from `catch_price` where the country populates it (Kenya);
#' where it is empty (Mozambique, Zanzibar) the function falls back, in order,
#' to trip-level revenue apportioned by weight, then to landed weight as a
#' volume proxy. The basis actually used is logged and returned.
#'
#' @param landings A landing-feed tibble (one row per trip x species-record)
#'   with at least `scientific_name`, `catch_taxon`, `catch_kg`, `catch_price`,
#'   `tot_catch_price`, `trip_id` and a `country` column.
#'
#' @return A tibble with one row per (`country`, `scientific_name`): columns
#'   `catch_taxon`, `revenue`, `catch_kg`, `n_records`, `n_trips`, `rank` and
#'   `rank_basis` (`"price"`, `"apportioned"` or `"weight"`).
#'
#' @keywords internal
.species_economic_rank <- function(landings) {
  purrr::map_dfr(split(landings, landings$country), function(df) {
    country <- df$country[[1]]
    price_cover <- mean(!is.na(df$catch_price) & df$catch_price > 0)

    scored <- if (price_cover >= 0.5) {
      basis <- "price"
      df |>
        dplyr::mutate(revenue = .data$catch_price)
    } else if (any(!is.na(df$tot_catch_price) & df$tot_catch_price > 0)) {
      basis <- "apportioned"
      df |>
        dplyr::group_by(.data$trip_id) |>
        dplyr::mutate(
          revenue = .data$tot_catch_price *
            .data$catch_kg / sum(.data$catch_kg, na.rm = TRUE)
        ) |>
        dplyr::ungroup()
    } else {
      basis <- "weight"
      df |>
        dplyr::mutate(revenue = .data$catch_kg)
    }

    logger::log_info(
      "Economic ranking for {country}: basis = {basis} ",
      "(price coverage {round(100 * price_cover)}%)."
    )

    scored |>
      dplyr::group_by(.data$scientific_name) |>
      dplyr::summarise(
        catch_taxon = .data$catch_taxon[[1]],
        revenue = sum(.data$revenue, na.rm = TRUE),
        catch_kg = sum(.data$catch_kg, na.rm = TRUE),
        n_records = dplyr::n(),
        n_trips = dplyr::n_distinct(.data$trip_id),
        .groups = "drop"
      ) |>
      dplyr::filter(!is.na(.data$scientific_name), .data$revenue > 0) |>
      dplyr::arrange(dplyr::desc(.data$revenue)) |>
      dplyr::mutate(
        country = .env$country,
        rank = dplyr::row_number(),
        rank_basis = .env$basis
      )
  })
}

#' Is a Taxon Name Resolved to Species Level?
#'
#' Length-based methods need a species with its own growth parameters, but the
#' landing feed mixes species binomials with coarser family, order or class
#' labels (e.g. `"Clupeidae"`, `"Actinopterygii"`) and genus aggregates
#' (`"Siganus spp"`). This helper flags the true `Genus species` binomials.
#'
#' @param x Character vector of scientific names.
#'
#' @return A logical vector, `TRUE` where `x` is a two-word binomial that is not
#'   a `sp`/`spp` aggregate.
#'
#' @keywords internal
.is_species_level <- function(x) {
  grepl("^[A-Z][a-z]+ [a-z]+$", x) & !grepl(" spp?$", x)
}

#' Select the Top Priority Species per Country
#'
#' Picks the `n_species` most economically important species in each country,
#' subject to a minimum number of records. By default only species-level taxa
#' are eligible, because the coarse family/order labels that dominate the
#' revenue ranking (Clupeidae, Octopodidae, ...) have no species-specific growth
#' parameters and belong to the group-level layer, not per-species LBB. The
#' share of revenue held by those coarse taxa is logged so coverage stays
#' transparent.
#'
#' @param landings A landing-feed tibble with a `country` column (see
#'   [.species_economic_rank()] for the required fields).
#' @param n_species Integer. Number of species to keep per country. Default 5.
#' @param min_records Integer. Minimum landing records a species needs to be
#'   eligible. Default 30.
#' @param species_only Logical. Restrict eligibility to species-level taxa.
#'   Default `TRUE`.
#'
#' @return A tibble of the selected species (the columns of
#'   [.species_economic_rank()]), filtered and re-ranked per country.
#'
#' @keywords workflow modeling
#' @export
select_priority_species <- function(
  landings,
  n_species = 5L,
  min_records = 30L,
  species_only = TRUE
) {
  ranked <- .species_economic_rank(landings) |>
    dplyr::filter(.data$n_records >= .env$min_records)

  if (isTRUE(species_only)) {
    ranked <- ranked |>
      dplyr::mutate(is_species = .is_species_level(.data$scientific_name))

    ranked |>
      dplyr::group_by(.data$country) |>
      dplyr::summarise(
        coarse_share = sum(.data$revenue[!.data$is_species], na.rm = TRUE) /
          sum(.data$revenue, na.rm = TRUE),
        .groups = "drop"
      ) |>
      purrr::pwalk(\(country, coarse_share) {
        logger::log_info(
          "{country}: {round(100 * coarse_share)}% of revenue sits in coarse ",
          "(non-species) taxa, set aside for the group-level layer."
        )
      })

    ranked <- ranked |>
      dplyr::filter(.data$is_species) |>
      dplyr::group_by(.data$country) |>
      dplyr::arrange(dplyr::desc(.data$revenue), .by_group = TRUE) |>
      dplyr::mutate(rank = dplyr::row_number()) |>
      dplyr::ungroup() |>
      dplyr::select(-"is_species")
  }

  selected <- ranked |>
    dplyr::group_by(.data$country) |>
    dplyr::slice_min(.data$rank, n = n_species, with_ties = FALSE) |>
    dplyr::ungroup()

  logger::log_info(
    "Selected {nrow(selected)} priority species across ",
    "{dplyr::n_distinct(selected$country)} countries."
  )
  selected
}

#' Summarise Length-Measurement Coverage for a Species
#'
#' Length-based methods run only on the subset of records that carry a
#' `length_cm` value. This helper reports how much length information exists for
#' one species and returns the raw length vector for downstream fitting.
#'
#' @param landings A landing-feed tibble for a single country.
#' @param scientific_name A single scientific name to summarise.
#'
#' @return A list with `lengths` (numeric vector of measured lengths) and
#'   `coverage` (a one-row tibble: `n_records`, `n_measured`, `frac_measured`,
#'   `n_trips_measured`, `l_min`, `l_max`, `l_mean`).
#'
#' @keywords internal
.length_coverage <- function(landings, scientific_name) {
  sp <- landings |>
    dplyr::filter(.data$scientific_name == .env$scientific_name)

  measured <- sp |>
    dplyr::filter(!is.na(.data$length_cm), .data$length_cm > 0)

  lengths <- measured$length_cm

  coverage <- tibble::tibble(
    scientific_name = scientific_name,
    n_records = nrow(sp),
    n_measured = length(lengths),
    frac_measured = if (nrow(sp) > 0) length(lengths) / nrow(sp) else NA_real_,
    n_trips_measured = dplyr::n_distinct(measured$trip_id),
    l_min = if (length(lengths) > 0) min(lengths) else NA_real_,
    l_max = if (length(lengths) > 0) max(lengths) else NA_real_,
    l_mean = if (length(lengths) > 0) mean(lengths) else NA_real_
  )

  list(lengths = lengths, coverage = coverage)
}

#' Drop Biologically Implausible Lengths
#'
#' Removes non-positive lengths and values above a multiple of the asymptotic
#' length, which are almost always unit or data-entry errors (e.g. a value in
#' millimetres, or a mistyped digit) and would otherwise distort the length
#' indicators and any length-based fit.
#'
#' @param lengths Numeric vector of measured lengths (cm).
#' @param linf Asymptotic length (cm); if `NA` only non-positive values are
#'   dropped.
#' @param max_factor Numeric. Keep lengths up to `max_factor * linf`.
#'   Default 1.2.
#'
#' @return The cleaned numeric vector.
#'
#' @keywords internal
.clean_lengths <- function(lengths, linf, max_factor = 1.2) {
  lengths <- lengths[!is.na(lengths) & lengths > 0]
  if (!is.na(linf) && linf > 0) {
    keep <- lengths <= max_factor * linf
    dropped <- sum(!keep)
    if (dropped > 0) {
      logger::log_info(
        "Dropped {dropped} length(s) above {max_factor} x Linf ",
        "({round(max_factor * linf, 1)} cm) as implausible."
      )
    }
    lengths <- lengths[keep]
  }
  lengths
}

#' Fetch Life-History Priors from FishBase / SeaLifeBase
#'
#' Assembles the priors the length-based methods need — asymptotic length
#' `linf`, growth `k`, natural-mortality ratio `mk`, and length-at-maturity
#' `lm` — for a set of species. Values come from `rfishbase`; where maturity is
#' missing it is filled with the Froese & Binohlan empirical relationship from
#' `linf`, and where `mk` is missing it defaults to the teleost value of 1.5.
#'
#' @param scientific_names Character vector of scientific names.
#' @param default_mk Numeric. Fallback natural-mortality-to-growth ratio when
#'   FishBase has none. Default 1.5.
#'
#' @return A tibble with one row per species: `scientific_name`, `linf`, `k`,
#'   `mk`, `lm`, `a`, `b`, `source`.
#'
#' @keywords workflow modeling
#' @export
get_life_history_priors <- function(scientific_names, default_mk = 1.5) {
  scientific_names <- unique(scientific_names[!is.na(scientific_names)])
  if (length(scientific_names) == 0) {
    return(tibble::tibble())
  }

  est <- tryCatch(
    rfishbase::estimate(scientific_names),
    error = function(e) {
      logger::log_warn("rfishbase::estimate() failed: {conditionMessage(e)}")
      tibble::tibble(Species = scientific_names)
    }
  )

  # rfishbase column names drift between versions and servers, so pull each
  # field defensively rather than assuming a fixed schema.
  col <- function(name) {
    if (name %in% names(est)) est[[name]] else rep(NA_real_, nrow(est))
  }
  est_std <- tibble::tibble(
    scientific_name = if ("Species" %in% names(est)) est$Species else NA,
    linf = dplyr::coalesce(col("Loo"), col("MaxLengthTL")),
    k = col("K"),
    m = col("M"),
    lm = col("Lm"),
    a = col("a"),
    b = col("b")
  )

  priors <- tibble::tibble(scientific_name = scientific_names) |>
    dplyr::left_join(est_std, by = "scientific_name") |>
    dplyr::mutate(
      mk = dplyr::if_else(
        !is.na(.data$m) & !is.na(.data$k) & .data$k > 0,
        .data$m / .data$k,
        .env$default_mk
      ),
      lm = dplyr::if_else(
        is.na(.data$lm) & !is.na(.data$linf),
        10^(0.8979 * log10(.data$linf) - 0.0782),
        .data$lm
      ),
      source = dplyr::if_else(is.na(.data$linf), "missing", "fishbase")
    ) |>
    dplyr::select(
      "scientific_name", "linf", "k", "mk", "lm", "a", "b", "source"
    )

  missing_linf <- priors |> dplyr::filter(is.na(.data$linf))
  if (nrow(missing_linf) > 0) {
    logger::log_warn(
      "No Linf for {nrow(missing_linf)} species; ",
      "length-based methods will be skipped for them."
    )
  }
  priors
}

#' Compute Froese Length-Based Sustainability Indicators
#'
#' The three "keep it simple" indicators (Froese 2004): the share of the catch
#' that is mature, that sits around the optimal length, and that consists of
#' large "mega-spawners", plus the mean-length-to-optimal ratio. These are
#' descriptive proxies, not estimates of biomass or fishing mortality.
#'
#' @param lengths Numeric vector of measured lengths (cm).
#' @param priors A one-row life-history tibble (`linf`, `mk`, `lm`).
#'
#' @return A one-row tibble: `lopt`, `lm`, `p_mature`, `p_opt`, `p_mega`,
#'   `lmean_lopt`, `n`.
#'
#' @keywords workflow modeling
#' @export
assess_length_indicators <- function(lengths, priors) {
  lopt <- priors$linf * 3 / (3 + priors$mk)
  lm <- priors$lm

  tibble::tibble(
    method = "lbi",
    lopt = lopt,
    lm = lm,
    p_mature = mean(lengths >= lm, na.rm = TRUE),
    p_opt = mean(lengths >= 0.9 * lopt & lengths <= 1.1 * lopt, na.rm = TRUE),
    p_mega = mean(lengths > 1.1 * lopt, na.rm = TRUE),
    lmean_lopt = mean(lengths, na.rm = TRUE) / lopt,
    n = length(lengths)
  )
}

#' Estimate Spawning Potential Ratio with LBSPR
#'
#' Wraps [LBSPR::LBSPRfit()] to estimate the spawning potential ratio (SPR),
#' relative fishing pressure `f_m`, and selectivity from a length sample and
#' life-history priors. Returns an `unknown`-flagged empty row (rather than
#' erroring) when the `LBSPR` package is unavailable or the fit fails, so the
#' orchestrator stays robust.
#'
#' @param lengths Numeric vector of measured lengths (cm).
#' @param priors A one-row life-history tibble (`linf`, `mk`, `lm`).
#' @param bin_width Numeric length-bin width (cm). Default `NULL` picks a bin
#'   from the observed range.
#'
#' @return A one-row tibble: `method`, `spr`, `f_m`, `sl50`, `sl95`, `n`.
#'
#' @keywords workflow modeling
#' @export
assess_lbspr <- function(lengths, priors, bin_width = NULL) {
  na_row <- tibble::tibble(
    method = "lbspr", spr = NA_real_, f_m = NA_real_,
    sl50 = NA_real_, sl95 = NA_real_, n = length(lengths)
  )

  if (!requireNamespace("LBSPR", quietly = TRUE)) {
    logger::log_warn("LBSPR not installed; skipping SPR estimation.")
    return(na_row)
  }
  if (is.na(priors$linf) || is.na(priors$lm) || length(lengths) < 20) {
    return(na_row)
  }

  if (is.null(bin_width)) {
    bin_width <- max(1, round(diff(range(lengths)) / 40))
  }

  fit <- tryCatch(
    {
      pars <- methods::new("LB_pars")
      pars@Species <- as.character(priors$scientific_name)
      pars@Linf <- priors$linf
      pars@L50 <- priors$lm
      pars@L95 <- priors$lm * 1.1
      pars@MK <- priors$mk
      pars@BinWidth <- bin_width

      # Let LBSPR bin the raw data against LB_pars, rather than hand-building
      # LMids/LData — the manual route silently mis-aligns the bins and the
      # fit returns NA.
      tmp <- tempfile(fileext = ".csv")
      on.exit(unlink(tmp), add = TRUE)
      utils::write.csv(
        data.frame(Length = lengths), tmp, row.names = FALSE
      )
      lenobj <- methods::new(
        "LB_lengths",
        LB_pars = pars,
        file = tmp,
        dataType = "raw",
        header = TRUE
      )

      LBSPR::LBSPRfit(pars, lenobj, verbose = FALSE)
    },
    error = function(e) {
      logger::log_warn("LBSPR fit failed: {conditionMessage(e)}")
      NULL
    }
  )

  if (is.null(fit)) {
    return(na_row)
  }

  slot1 <- function(s) {
    tryCatch(as.numeric(methods::slot(fit, s))[[1]], error = \(e) NA_real_)
  }
  tibble::tibble(
    method = "lbspr",
    spr = slot1("SPR"),
    f_m = slot1("FM"),
    sl50 = slot1("SL50"),
    sl95 = slot1("SL95"),
    n = length(lengths)
  )
}

#' Estimate Relative Stock Size with LBB (Length-Based Bayesian Biomass)
#'
#' Runs the anchor method, Froese et al. (2018) LBB, through a pluggable engine.
#' LBB depends on JAGS, which is a system library rather than a CRAN package, so
#' the estimator is not bundled: pass the fitting routine via `lbb_engine` (for
#' example the SISTA16 reference model or the Monte-Carlo variant). When no
#' engine is supplied the function returns an `unknown`-flagged row so the rest
#' of the framework still runs on the indicator and SPR methods.
#'
#' @param lengths Numeric vector of measured lengths (cm).
#' @param priors A one-row life-history tibble (`linf`, `mk`, `lm`).
#' @param lbb_engine A function of `(lengths, priors)` returning a list with at
#'   least `bb0` (B/B0), `bbmsy` (B/Bmsy), `fm` (F/M), `lc_lopt` and their
#'   credible-interval widths. Default `NULL`.
#'
#' @return A one-row tibble: `method`, `bb0`, `bbmsy`, `f_m`, `lc_lopt`,
#'   `ci_width`, `n`.
#'
#' @keywords workflow modeling
#' @export
assess_stock_lbb <- function(lengths, priors, lbb_engine = NULL) {
  na_row <- tibble::tibble(
    method = "lbb", bb0 = NA_real_, bbmsy = NA_real_, f_m = NA_real_,
    lc_lopt = NA_real_, ci_width = NA_real_, n = length(lengths)
  )

  if (is.null(lbb_engine)) {
    logger::log_debug("No LBB engine supplied; returning unknown LBB row.")
    return(na_row)
  }
  if (is.na(priors$linf) || length(lengths) < 20) {
    return(na_row)
  }

  fit <- tryCatch(
    lbb_engine(lengths, priors),
    error = function(e) {
      logger::log_warn("LBB engine failed: {conditionMessage(e)}")
      NULL
    }
  )
  if (is.null(fit)) {
    return(na_row)
  }

  tibble::tibble(
    method = "lbb",
    bb0 = fit$bb0 %||% NA_real_,
    bbmsy = fit$bbmsy %||% NA_real_,
    f_m = fit$fm %||% NA_real_,
    lc_lopt = fit$lc_lopt %||% NA_real_,
    ci_width = fit$ci_width %||% NA_real_,
    n = length(lengths)
  )
}

#' Assign a Data-Quality Flag to an Assessment
#'
#' Mirrors the FAO estimator's vocabulary (`pass`, `warn`, `fail`, `unknown`)
#' so the assessment layer reads consistently with the catch estimates. The
#' flag combines length-sample size, how representative the sampled records are,
#' whether the sample reaches near-asymptotic sizes, and — when present — the
#' width of the method's own uncertainty.
#'
#' @param coverage A one-row coverage tibble from [.length_coverage()].
#' @param priors A one-row life-history tibble (for `linf`).
#' @param ci_width Optional numeric relative uncertainty of the estimate
#'   (e.g. LBB credible-interval width or LBSPR CV). Default `NA`.
#'
#' @return A single character flag: `"pass"`, `"warn"`, `"fail"` or
#'   `"unknown"`.
#'
#' @keywords internal
.flag_assessment_quality <- function(coverage, priors, ci_width = NA_real_) {
  if (is.na(coverage$n_measured) || coverage$n_measured == 0) {
    return("unknown")
  }

  reach <- if (!is.na(priors$linf) && priors$linf > 0) {
    coverage$l_max / priors$linf
  } else {
    NA_real_
  }

  score <- 0L
  score <- score + (coverage$n_measured >= 200) + (coverage$n_measured >= 100)
  score <- score + (isTRUE(coverage$frac_measured >= 0.3)) +
    (isTRUE(coverage$frac_measured >= 0.1))
  score <- score + (isTRUE(reach >= 0.8))
  score <- score - (isTRUE(ci_width > 0.5))

  if (score >= 4) {
    "pass"
  } else if (score >= 2) {
    "warn"
  } else {
    "fail"
  }
}

#' Assess One Species with the Requested Methods
#'
#' Runs the length coverage summary, life-history priors and the chosen
#' assessment methods for a single species, and attaches the quality flag. This
#' is the per-species unit the orchestrator maps over.
#'
#' @param landings A landing-feed tibble for a single country.
#' @param scientific_name A single scientific name.
#' @param priors A one-row life-history tibble for that species.
#' @param methods Character vector of methods to run; any of `"lbi"`,
#'   `"lbspr"`, `"lbb"`. Default all three.
#' @param lbb_engine Optional LBB fitting routine (see [assess_stock_lbb()]).
#'
#' @return A tibble with one row per method, carrying the estimate columns of
#'   each method plus `scientific_name`, `n_measured`, `frac_measured` and
#'   `quality`.
#'
#' @keywords workflow modeling
#' @export
assess_species_stock <- function(
  landings,
  scientific_name,
  priors,
  methods = c("lbi", "lbspr", "lbb"),
  lbb_engine = NULL
) {
  cov <- .length_coverage(landings, scientific_name)
  linf <- if (nrow(priors) > 0) priors$linf else NA_real_
  lengths <- .clean_lengths(cov$lengths, linf)
  logger::log_info(
    "Assessing {scientific_name}: {length(lengths)} measured lengths ",
    "({round(100 * (cov$coverage$frac_measured %||% 0))}% of records)."
  )

  if (nrow(priors) == 0 || is.na(priors$linf) || length(lengths) < 20) {
    logger::log_warn(
      "Insufficient length data or priors for {scientific_name}; ",
      "flagging unknown."
    )
    return(tibble::tibble(
      scientific_name = scientific_name,
      method = methods,
      n_measured = length(lengths),
      frac_measured = cov$coverage$frac_measured,
      quality = "unknown"
    ))
  }

  runs <- list()
  if ("lbi" %in% methods) {
    runs$lbi <- assess_length_indicators(lengths, priors)
  }
  if ("lbspr" %in% methods) {
    runs$lbspr <- assess_lbspr(lengths, priors)
  }
  if ("lbb" %in% methods) {
    runs$lbb <- assess_stock_lbb(lengths, priors, lbb_engine = lbb_engine)
  }

  ci_width <- runs$lbb$ci_width %||% NA_real_
  flag <- .flag_assessment_quality(cov$coverage, priors, ci_width)

  purrr::list_rbind(runs) |>
    dplyr::mutate(
      scientific_name = .env$scientific_name,
      n_measured = length(lengths),
      frac_measured = cov$coverage$frac_measured,
      quality = .env$flag,
      .before = 1
    )
}

#' Run the Stock-Assessment Framework
#'
#' Workflow orchestrator. For each country it downloads the validated landing
#' feed, selects the top economically important species, pulls life-history
#' priors, runs the length-based assessment methods with a data-quality flag on
#' each, and uploads a versioned parquet of the results.
#'
#' @param n_species Integer. Priority species per country. Default 5.
#' @param countries Character vector of country keys under `conf$api$trips`.
#'   Default Kenya, Mozambique, Zanzibar.
#' @param methods Character vector of methods (`"lbi"`, `"lbspr"`, `"lbb"`).
#' @param start_date Earliest landing date to include, aligning with the
#'   PDS-covered window. Default `"2024-01-01"`.
#' @param lbb_engine Optional LBB fitting routine (see [assess_stock_lbb()]).
#' @param log_threshold The logging threshold. Default `logger::DEBUG`.
#' @param package Name of the package whose `inst/conf.yml` to read. Default
#'   `"coasts"`.
#'
#' @return Invisibly, a tibble of per-species, per-method assessments with
#'   quality flags.
#'
#' @keywords workflow modeling
#' @export
run_stock_assessment <- function(
  n_species = 5L,
  countries = c("kenya", "mozambique", "zanzibar"),
  methods = c("lbi", "lbspr", "lbb"),
  start_date = "2024-01-01",
  lbb_engine = NULL,
  log_threshold = logger::DEBUG,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  logger::log_info(
    "=== Stock assessment | {paste(countries, collapse = ', ')} ",
    "| top {n_species} species ==="
  )

  landings <- purrr::map_dfr(countries, function(country) {
    logger::log_info("Downloading validated landings for {country} ...")
    download_parquet_from_cloud(
      prefix = conf$api$trips[[country]]$validated$cloud_path,
      provider = conf$storage$google$key,
      options = conf$storage$google$options,
      bucket_name = conf$api$trips$bucket
    ) |>
      dplyr::filter(
        !is.na(.data$gaul_2_name),
        .data$landing_date >= as.Date(.env$start_date)
      ) |>
      dplyr::mutate(country = .env$country)
  })

  if (nrow(landings) == 0) {
    logger::log_warn("Pipeline stopped: no landings after filtering.")
    return(invisible(NULL))
  }

  # Rank a generous species-level pool, then keep only finfish — species with a
  # FishBase growth estimate — so the length-based track excludes invertebrates
  # (crabs, lobsters, octopus) that need molt/carapace methods, not VBGF LBB.
  candidates <- select_priority_species(landings, n_species = n_species * 5L)
  priors <- get_life_history_priors(candidates$scientific_name)

  priority <- candidates |>
    dplyr::left_join(
      priors |> dplyr::select("scientific_name", "linf"),
      by = "scientific_name"
    ) |>
    dplyr::filter(!is.na(.data$linf)) |>
    dplyr::group_by(.data$country) |>
    dplyr::slice_min(.data$rank, n = n_species, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select(-"linf")

  logger::log_info(
    "Finfish retained for length-based assessment: {nrow(priority)} ",
    "(from {nrow(candidates)} species-level candidates)."
  )

  assessments <- purrr::pmap_dfr(
    list(priority$country, priority$scientific_name),
    function(country, sci) {
      assess_species_stock(
        landings = dplyr::filter(landings, .data$country == .env$country),
        scientific_name = sci,
        priors = dplyr::filter(priors, .data$scientific_name == .env$sci),
        methods = methods,
        lbb_engine = lbb_engine
      ) |>
        dplyr::mutate(country = .env$country, .before = 1)
    }
  ) |>
    dplyr::left_join(
      priority |>
        dplyr::select("country", "scientific_name", "catch_taxon", "rank",
          "rank_basis"),
      by = c("country", "scientific_name")
    )

  qual_summary <- assessments$quality |>
    table() |>
    (\(x) paste(names(x), x, sep = ":", collapse = " "))()
  logger::log_info(
    "Assessment complete: {nrow(assessments)} rows | quality {qual_summary}."
  )

  prefix <- conf$stock$assessment$file_prefix %||% "stock-assessment"
  upload_parquet_to_cloud(
    data = assessments,
    prefix = prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("=== Stock assessment pipeline complete ===")
  invisible(assessments)
}
