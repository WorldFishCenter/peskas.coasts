# One-row-per-stock summary of the stock-assessment results.
#
# run_stock_assessment() returns a long table (one row per method and gear,
# species and group rows stacked). Dashboards and reports need one row per
# country x taxon with the headline read, its uncertainty, how far to trust it
# and what to collect next. This is that table.

#' Kobe-Style Status from B/B0 and F/M
#'
#' `healthy` when B/B0 >= 0.5 and F/M <= 1; `overfishing` when biomass is fine
#' but pressure is too high; `overfished` when biomass is low but pressure is
#' fine; `overfished + overfishing` when both are bad.
#'
#' @param bb0,fm Relative biomass and fishing pressure.
#'
#' @return Character vector (`NA` where either input is missing).
#'
#' @keywords internal
.kobe_status <- function(bb0, fm) {
  dplyr::case_when(
    is.na(bb0) | is.na(fm) ~ NA_character_,
    bb0 >= 0.5 & fm <= 1 ~ "healthy",
    bb0 >= 0.5 & fm > 1 ~ "overfishing",
    bb0 < 0.5 & fm <= 1 ~ "overfished",
    TRUE ~ "overfished + overfishing"
  )
}

#' Traffic-Light Reliability of a Stock Read
#'
#' @keywords internal
.reliability <- function(quality, robust_flag, production_band, consensus,
                         selectivity_flag, n_length_classes, confidence,
                         n_measured = NA_real_) {
  dplyr::case_when(
    is.na(quality) | quality == "unknown" ~ "not assessable",
    robust_flag %in% "sensitive" | production_band %in% "straddles" |
      consensus %in% "mixed (methods disagree)" ~ "borderline",
    quality %in% "fail" | selectivity_flag %in% TRUE |
      dplyr::coalesce(n_length_classes < 6, FALSE) | confidence %in% "low" |
      dplyr::coalesce(n_measured < 100, FALSE) ~ "indicative",
    TRUE ~ "reliable"
  )
}

#' Summarise Stock-Assessment Results to One Row per Stock
#'
#' Collapses the long output of [run_stock_assessment()] to one row per
#' country x scale (species / group) x taxon. The headline read is the
#' reference gear's when per-gear reads exist (see `by_gear`), otherwise the
#' pooled read. Prior-robustness fields (`robust_flag`, `linf_ratio`) always
#' come from the pooled read, the only one the robustness grid is run on.
#'
#' @param results The tibble returned by [run_stock_assessment()].
#'
#' @return A tibble, one row per stock, with columns:
#'   * `country`, `scale` (`"species"` / `"group"`), `scientific_name`,
#'     `catch_taxon`, `headline_gear` (`"all"` = pooled across gears);
#'   * `status`: Kobe-style word from LBB (`healthy`, `overfishing`,
#'     `overfished`, `overfished + overfishing`) when LBB fitted; otherwise the
#'     Froese-based word (`healthy`, `borderline`, `overfishing signal`) —
#'     always for groups, and for species whose lengths are too coarse for LBB;
#'   * `status_basis`: `"LBB"` or `"Froese"`;
#'   * `reliability`: `reliable` / `indicative` / `borderline` /
#'     `not assessable` (see Details);
#'   * `consensus`, `n_concern`, `n_methods`: agreement of LBB, Froese and the
#'     production layer;
#'   * `bb0`, `bb0_lo`, `bb0_hi`, `f_m`, `fm_lo`, `fm_hi`: LBB relative
#'     biomass and fishing pressure with the prior-robustness band;
#'   * `e`, `e_lo`, `e_hi`, `x`, `e_x`, `sy_ratio`, `production_status`,
#'     `production_band`: production layer (share of production taken vs the
#'     sustainable share; `sy_ratio` = sustainable catch / current catch);
#'   * `p_mature`, `lmean_lopt`: Froese indicators;
#'   * `n_measured`, `n_length_classes`, `size_reach`, `catch_share`,
#'     `trophic_level`, `confidence` (groups), `quality`, `quality_reasons`;
#'   * `main_gear`, `main_gear_share`, `selectivity_flag`, `robust_flag`,
#'     `linf_ratio`, `misid_flag`;
#'   * `advice`: one data-collection suggestion.
#'
#' @details `reliability` is `not assessable` without a read; `borderline`
#'   when the status flips across plausible priors, the production band
#'   straddles the sustainable line, or the methods disagree; `indicative` when
#'   the read stands but rests on a weak basis (failed quality, fewer than 100
#'   measured fish, a trap / gill net dominated sample, fewer than six length
#'   classes, or a low-confidence group prior); otherwise `reliable`.
#'
#' @keywords workflow modeling
#' @export
summarise_stock_status <- function(results) {
  key <- c("country", "scale", "scientific_name")
  if (!"gear" %in% names(results)) {
    results$gear <- "all"
  }
  if (!"reference_gear" %in% names(results)) {
    results$reference_gear <- NA
  }
  col <- function(df, name, default = NA) {
    if (name %in% names(df)) df[[name]] else rep(default, nrow(df))
  }
  results$method <- col(results, "method", NA_character_)

  headline <- results |>
    dplyr::group_by(dplyr::across(dplyr::all_of(key))) |>
    dplyr::summarise(
      headline_gear = if (any(.data$reference_gear %in% TRUE)) {
        .data$gear[.data$reference_gear %in% TRUE][1]
      } else {
        "all"
      },
      .groups = "drop"
    )

  read <- results |>
    dplyr::inner_join(headline, by = key) |>
    dplyr::filter(.data$gear == .data$headline_gear)

  pick <- function(df, method, cols) {
    out <- df |>
      dplyr::filter(.data$method %in% .env$method) |>
      dplyr::distinct(dplyr::across(dplyr::all_of(key)), .keep_all = TRUE)
    out[, intersect(c(key, cols), names(out)), drop = FALSE]
  }
  base_cols <- c(
    "catch_taxon", "headline_gear", "group_status", "consensus", "n_concern",
    "n_methods", "n_measured", "size_reach", "catch_share", "confidence",
    "quality", "quality_reasons", "advice", "main_gear", "main_gear_share",
    "selectivity_flag", "misid_flag", "p_mature", "lmean_lopt", "e", "e_lo",
    "e_hi", "x", "e_x", "sy_ratio", "production_status", "production_band",
    "trophic_level"
  )
  base <- read |>
    dplyr::distinct(dplyr::across(dplyr::all_of(key)), .keep_all = TRUE)
  base <- base[, intersect(c(key, base_cols), names(base)), drop = FALSE]

  # Species rows are long by method: take each method's own columns.
  lbb <- pick(read, "lbb", c("bb0", "f_m"))
  lbi <- pick(read, "lbi", c("p_mature", "lmean_lopt"))
  prod <- pick(read, "production", c(
    "e", "e_lo", "e_hi", "x", "e_x", "sy_ratio", "production_status",
    "production_band", "trophic_level"
  ))
  # Prior robustness always from the pooled read.
  pooled <- results |>
    dplyr::filter(.data$gear == "all") |>
    dplyr::distinct(dplyr::across(dplyr::all_of(key)), .keep_all = TRUE)
  pooled <- pooled[, intersect(
    c(key, "bb0_lo", "bb0_hi", "fm_lo", "fm_hi", "robust_flag", "linf_ratio"),
    names(pooled)
  ), drop = FALSE]

  fill <- function(df, other) {
    upd <- setdiff(intersect(names(df), names(other)), key)
    df <- dplyr::left_join(df, other, by = key, suffix = c("", ".m"))
    for (v in upd) {
      df[[v]] <- dplyr::coalesce(df[[paste0(v, ".m")]], df[[v]])
      df[[paste0(v, ".m")]] <- NULL
    }
    df
  }
  out <- base |>
    fill(lbb) |>
    fill(lbi) |>
    fill(prod) |>
    dplyr::left_join(pooled, by = key)

  n_cls <- if ("n_length_classes" %in% names(results)) {
    results |>
      dplyr::distinct(dplyr::across(dplyr::all_of(key)), .keep_all = TRUE) |>
      dplyr::select(dplyr::all_of(key), "n_length_classes")
  } else {
    NULL
  }
  if (!is.null(n_cls)) {
    out <- dplyr::left_join(out, n_cls, by = key)
  }

  for (v in c("bb0", "f_m", "bb0_lo", "p_mature", "lmean_lopt", "n_measured", "bb0_hi", "fm_lo", "fm_hi", "robust_flag",
              "linf_ratio", "group_status", "production_band", "consensus",
              "selectivity_flag", "n_length_classes", "confidence", "quality",
              "catch_share")) {
    if (!v %in% names(out)) out[[v]] <- NA
  }

  out |>
    dplyr::mutate(
      status_basis = dplyr::case_when(
        .data$scale == "species" & !is.na(.kobe_status(.data$bb0, .data$f_m)) ~ "LBB",
        !is.na(.group_status(.data$p_mature, .data$lmean_lopt)) |
          !is.na(.data$group_status) ~ "Froese",
        TRUE ~ NA_character_
      ),
      status = dplyr::case_when(
        .data$status_basis == "LBB" ~ .kobe_status(.data$bb0, .data$f_m),
        .data$scale == "group" ~ .data$group_status,
        TRUE ~ .group_status(.data$p_mature, .data$lmean_lopt)
      ),
      reliability = .reliability(
        .data$quality, .data$robust_flag, .data$production_band, .data$consensus,
        .data$selectivity_flag, .data$n_length_classes, .data$confidence,
        .data$n_measured
      )
    ) |>
    dplyr::select(
      dplyr::all_of(key), dplyr::any_of(c("catch_taxon", "headline_gear")),
      "status", "status_basis", "reliability", dplyr::any_of(c("consensus", "n_concern", "n_methods")),
      "bb0", "bb0_lo", "bb0_hi", "f_m", "fm_lo", "fm_hi",
      dplyr::any_of(c(
        "e", "e_lo", "e_hi", "x", "e_x", "sy_ratio", "production_status",
        "production_band", "p_mature", "lmean_lopt", "n_measured",
        "n_length_classes", "size_reach", "catch_share", "trophic_level",
        "confidence", "quality", "quality_reasons", "main_gear",
        "main_gear_share", "selectivity_flag", "robust_flag", "linf_ratio",
        "misid_flag", "advice"
      ))
    ) |>
    dplyr::arrange(.data$country, .data$scale, dplyr::desc(.data$catch_share))
}
