# FAO-aligned catch & effort estimation pipeline.
#
# Top-level entry point estimate_catch_fao() and the country adapter
# prepare_kenya_landings() it dispatches on. The statistical engine
# lives in fao-estimation.R; pure numerical helpers in fao-helpers.R;
# frame-survey and PDS activity helpers in fao-frame.R.
#
# Reference: de Graaf G., Stamatopoulos C., Jarrett T. (2017).
# OPEN ARTFISH and the FAO ODK mobile phone application — a toolkit
# for small-scale fisheries routine data collection. FAO, Rome.

#' Reshape KEFS (Kenya) validated landings into FAO long format
#'
#' KEFS records each trip with a "priority" species (target catch, weighed
#' in full) and a "sample" species (a sub-sample weighed for composition).
#' The FAO estimator wants one row per species per trip with `catch_kg` and
#' `catch_taxon`. This helper unions the priority and sample blocks into
#' that shape, dropping rows with zero or missing weight / taxon.
#'
#' Called automatically by `estimate_catch_fao()` when the KEFS schema is
#' detected. Exposed as an internal helper for testing only.
#'
#' Note: this is a basic union; full FAO compliance for KEFS would also
#' raise the sample weight to total catch via
#' `sample_weight_raised = sample_weight * total_catch_weight / sum(sample_weight)`
#' per trip. Add this when the team confirms the sub-sampling protocol.
#'
#' @param landings_kefs Raw KEFS validated landings.
#'
#' @return Long-format tibble with `catch_kg` and `catch_taxon` ready
#'         for `estimate_catch_fao()`.
#' @keywords internal
prepare_kenya_landings <- function(landings_kefs) {
  carry <- intersect(
    c(
      "submission_id",
      "gaul_1_name",
      "gaul_2_name",
      "landing_site",
      "vessel_type",
      "gear",
      "boat_name",
      "trip_duration",
      "landing_date",
      "total_catch_weight"
    ),
    colnames(landings_kefs)
  )

  # Priority block: revenue estimated from unit price
  #   priority_revenue = priority_weight x total_price_kg
  priority <- landings_kefs |>
    dplyr::mutate(
      .priority_revenue = .data$priority_weight * .data$total_price_kg
    ) |>
    dplyr::select(
      dplyr::all_of(carry),
      catch_taxon = "priority_scientific_name",
      catch_kg = "priority_weight",
      catch_price = ".priority_revenue"
    )

  # Sample block: revenue is given directly per row
  sample <- landings_kefs |>
    dplyr::select(
      dplyr::all_of(carry),
      catch_taxon = "sample_scientific_name",
      catch_kg = "sample_weight",
      catch_price = "sample_price"
    )

  out <- dplyr::bind_rows(priority, sample) |>
    dplyr::filter(
      !is.na(.data$catch_taxon),
      !is.na(.data$catch_kg),
      .data$catch_kg > 0
    )

  logger::log_info(
    "prepare_kenya_landings: {nrow(landings_kefs)} input rows -> ",
    "{nrow(out)} long-format rows ",
    "({dplyr::n_distinct(out$submission_id)} unique trips)."
  )
  out
}


#' FAO-aligned catch & effort estimation pipeline
#'
#' End-to-end wrapper for routine country reporting. Takes validated
#' landings (and optionally a frame survey + activity table) and returns
#' the full set of FAO estimates with quality flags.
#'
#' @param landings         Validated landings. One row per species per trip.
#' @param frame            Optional tibble with frame counts (`F_total` per
#'                         minor stratum x fu). If `NULL` and no `assets`,
#'                         derived from observed boats (placeholder, with
#'                         warning).
#' @param assets           Optional Airtable assets list from
#'                         `download_fao_frame()`. When provided and
#'                         `frame` is `NULL`, the frame survey is
#'                         derived from `assets$frame` automatically.
#'                         If `fu_cols` includes both vessel and gear
#'                         (the default), the vessel-level frame is
#'                         disaggregated to (vessel x gear) using the
#'                         observed gear proportions per
#'                         (district x vessel) -- see
#'                         `disaggregate_frame_by_gear()`.
#' @param activity         Optional tibble with `bac`, `pab`, or `ac` per
#'                         minor stratum x fu x period. If `NULL`, fishing
#'                         days are taken directly from observed records.
#'                         When `pds_trips` and `pds_devices` are also
#'                         provided, the table is built automatically from
#'                         GPS tracking data — see [build_pds_activity()].
#' @param pds_trips        Optional tibble of GPS trips (from the
#'                         `pds-trips` parquet). When provided along with
#'                         `pds_devices`, an activity table is built
#'                         automatically.
#' @param pds_devices      Optional tibble from the Airtable `pds_devices`
#'                         table (with `imei` and `gear class`).
#' @param gear_lookup      Tibble mapping survey gears to PDS macro
#'                         categories. Default [default_gear_lookup].
#'                         Set to `NULL` (or `apply_macros = FALSE`) to
#'                         disable the macro-grouping.
#' @param apply_macros     Logical. If `TRUE` (default), the gear column
#'                         of landings is overwritten with the macro
#'                         category from `gear_lookup` before fishing
#'                         units are built. Reduces ~85 vessel-gear combos
#'                         to ~30 macro-units and dramatically improves
#'                         per-cell sample sizes.
#' @param hybrid_frame     Logical. If `TRUE` (default), cells not covered
#'                         by the explicit/assets frame are backfilled
#'                         with observed-boat counts, so every cell ends
#'                         up with a valid F_total.
#' @param major_stratum    Column for major stratum. Default "gaul_2_name".
#' @param minor_stratum    Column for minor stratum. Default "landing_site".
#' @param fu_cols          Columns defining the fishing unit. Default
#'                         `c("vessel_type", "gear")`. Pass just
#'                         `"vessel_type"` for a coarser (and more
#'                         FAO-orthodox) grouping that maps directly to
#'                         the Airtable frame without disaggregation.
#' @param boat_col         Vessel-identifier column. Default "boat_name".
#'                         If absent in `landings`, falls back to
#'                         `submission_id` with a warning — meaning each
#'                         submission is treated as one fishing-unit-day
#'                         and the compound RE for total catch collapses
#'                         to the CPUE RE.
#' @param metric           Either "catch" (default, weights in kg) or
#'                         "revenue" (monetary value). When "revenue",
#'                         the pipeline uses the `catch_price` column as
#'                         the per-row quantity. Output column names stay
#'                         the same (`total_catch_kg`, `mean_cpue`, ...)
#'                         but contain monetary values; the `metric` field
#'                         in the returned list states which is which.
#' @param period_col       Column with the period label (e.g. "year_month").
#'                         If absent, it's built from `landing_date`.
#' @param duration_col     Trip-duration column. Default "trip_duration".
#' @param duration_units   "hours" (default) or "days".
#' @param alpha            Significance level for RE. Default 0.10.
#' @param re_threshold     RE pass threshold. Default 0.15.
#'
#' @return A list with named tibbles:
#'   \describe{
#'     \item{metric}{"catch" or "revenue" — what the numeric outputs mean}
#'     \item{trips}{trip-level CPUE table}
#'     \item{cpue_summary}{minor stratum x fu x period CPUE summary}
#'     \item{days_summary}{minor stratum x fu x period fishing-days summary}
#'     \item{frame}{frame-survey counts used}
#'     \item{minor}{FAO catch estimates at the minor stratum level}
#'     \item{major}{FAO catch estimates aggregated to the major stratum}
#'     \item{species}{catch by species per (group x species)}
#'     \item{quality}{summary of pass/warn/fail counts}
#'   }
#' @export
estimate_catch_fao <- function(
  landings,
  frame = NULL,
  assets = NULL,
  activity = NULL,
  pds_trips = NULL,
  pds_devices = NULL,
  gear_lookup = default_gear_lookup,
  apply_macros = TRUE,
  hybrid_frame = TRUE,
  metric = c("catch", "revenue"),
  major_stratum = "gaul_2_name",
  minor_stratum = "landing_site",
  fu_cols = c("vessel_type", "gear"),
  boat_col = "boat_name",
  period_col = "year_month",
  duration_col = "trip_duration",
  duration_units = "hours",
  alpha = 0.10,
  re_threshold = 0.15
) {
  metric <- match.arg(metric)
  logger::log_info(
    "FAO aggregation: starting on {nrow(landings)} landing rows ",
    "(metric = '{metric}')"
  )

  # ── 13.0a Apply gear macro-categories to landings BEFORE everything else,
  # so that frame disaggregation, fishing-unit construction, and activity
  # joins all use the same vocabulary. Skip if user opts out or no lookup.
  use_macros <- isTRUE(apply_macros) &&
    !is.null(gear_lookup) &&
    "gear" %in% colnames(landings)
  if (use_macros) {
    landings <- apply_gear_macros(
      landings,
      gear_lookup = gear_lookup,
      gear_col = "gear"
    )
    logger::log_info(
      "Applied gear macro-categories to landings ",
      "({nrow(gear_lookup)} mappings)."
    )
  }

  # ── 13.0b Frame survey from Airtable assets
  # Auto-detect whether the frame uses vessel or gear categories, then
  # disaggregate to (vessel x gear) fishing units using observed proportions
  # from whichever dimension is missing.
  if (!is.null(assets)) {
    if (!is.null(frame)) {
      logger::log_warn(
        "Both `frame` and `assets` were provided -- `assets` will be ignored ",
        "and the explicit `frame` used."
      )
    } else {
      if (!"frame" %in% names(assets)) {
        stop("`assets` does not contain a `frame` element.")
      }
      gaul_2_in_data <- unique(landings$gaul_2_name)
      frame_lookup <- if (use_macros) gear_lookup else NULL
      frame_vessel <- build_frame_table(
        assets$frame,
        level = "vessel",
        gaul_2_filter = gaul_2_in_data,
        gear_lookup = frame_lookup
      )
      frame_gear <- build_frame_table(
        assets$frame,
        level = "gear",
        gaul_2_filter = gaul_2_in_data,
        gear_lookup = frame_lookup
      )

      wants_vessel_gear <- length(fu_cols) >= 2 &&
        all(c("vessel_type", "gear") %in% fu_cols)

      if (nrow(frame_vessel) > 0 && wants_vessel_gear) {
        logger::log_info(
          "Frame is vessel-based ({nrow(frame_vessel)} cells). ",
          "Disaggregating by observed gear proportions."
        )
        frame <- disaggregate_frame_by_gear(frame_vessel, landings)
      } else if (nrow(frame_gear) > 0 && wants_vessel_gear) {
        logger::log_info(
          "Frame is gear-based ({nrow(frame_gear)} cells). ",
          "Disaggregating by observed vessel proportions."
        )
        frame <- disaggregate_frame_by_vessel(frame_gear, landings)
      } else if (nrow(frame_vessel) > 0) {
        logger::log_info(
          "Using vessel-only frame ({nrow(frame_vessel)} cells)."
        )
        frame <- frame_vessel
      } else if (nrow(frame_gear) > 0) {
        logger::log_info("Using gear-only frame ({nrow(frame_gear)} cells).")
        frame <- frame_gear
      } else {
        logger::log_warn(
          "No frame data found in assets for the districts in landings. ",
          "Falling back to observed-boats frame."
        )
        frame <- NULL
      }

      # Disaggregation can silently produce an empty frame when the
      # assets/landings vocabulary doesn't intersect. Treat 0-row as NULL.
      if (!is.null(frame) && nrow(frame) == 0L) {
        logger::log_warn(
          "Frame from assets is empty after disaggregation. ",
          "Falling back to observed-boats frame."
        )
        frame <- NULL
      }
    }
  }

  # ── 13.0c Auto-detect country schema and reshape to FAO long format
  is_kefs <- all(
    c(
      "priority_scientific_name",
      "priority_weight",
      "sample_scientific_name",
      "sample_weight"
    ) %in%
      colnames(landings)
  )

  if (is_kefs) {
    logger::log_info(
      "Detected KEFS (Kenya) schema -- reshaping priority + sample blocks ",
      "into one row per species per trip."
    )
    landings <- prepare_kenya_landings(landings)
  }

  if (!all(c("catch_kg", "catch_taxon") %in% colnames(landings))) {
    stop(
      "Landings must contain `catch_kg` and `catch_taxon` columns. ",
      "Got: ",
      paste(colnames(landings), collapse = ", "),
      ". If this is a country with a different schema, add a branch ",
      "to the schema-detection block in `estimate_catch_fao()`."
    )
  }

  # ── 13.0d Revenue swap: when metric = 'revenue', overwrite catch_kg with
  # the per-row revenue value so the rest of the pipeline runs unchanged.
  # Output column names stay catch_kg / total_catch_kg / mean_cpue, but the
  # `metric` field on the result tells the caller what they actually contain.
  if (metric == "revenue") {
    if (!"catch_price" %in% colnames(landings)) {
      stop(
        "metric = 'revenue' requested but `catch_price` column not found. ",
        "Add a per-row revenue column named `catch_price` to the landings."
      )
    }
    n_swapped <- sum(!is.na(landings$catch_price) & landings$catch_price > 0)
    landings$catch_kg <- landings$catch_price
    logger::log_info(
      "Revenue mode: using `catch_price` as quantity ",
      "({n_swapped} non-zero rows). Output columns keep their `catch_kg` ",
      "names but contain monetary values."
    )
  }

  # ── 13.0e Boat column auto-fallback
  if (!boat_col %in% colnames(landings)) {
    if ("submission_id" %in% colnames(landings)) {
      logger::log_warn(
        "boat_col `",
        boat_col,
        "` not found — falling back to ",
        "`submission_id` as the unit-of-observation. Each submission ",
        "becomes one fishing-unit-day; days-variability term in the ",
        "compound RE collapses to 0. Supply a frame + activity (BAC/PAB) ",
        "table to recover the FAO total-catch RE."
      )
      boat_col <- "submission_id"
    } else {
      stop(
        "Neither `",
        boat_col,
        "` nor `submission_id` is in landings — ",
        "cannot identify fishing units."
      )
    }
  }

  # ── 13.1 Build period column if missing
  if (!period_col %in% colnames(landings)) {
    if (!"landing_date" %in% colnames(landings)) {
      stop("Provide either `", period_col, "` or `landing_date` in landings.")
    }
    landings[[period_col]] <- format(as.Date(landings$landing_date), "%Y-%m")
  }

  # ── 13.2 Build fishing-unit labels
  landings <- build_fishing_units(landings, fu_cols = fu_cols)

  # ── 13.3 Trip-level CPUE
  keep <- c(
    major_stratum,
    minor_stratum,
    "fishing_unit",
    period_col,
    boat_col,
    "landing_date"
  )
  trips <- compute_trip_cpue(
    landings,
    duration_col = duration_col,
    duration_units = duration_units,
    keep_cols = intersect(keep, colnames(landings))
  )

  # ── 13.4 Minor-stratum summaries
  group_minor <- c(major_stratum, minor_stratum, "fishing_unit", period_col)
  cpue_summary <- summarize_cpue(trips, group_by = group_minor, alpha = alpha)

  # If activity coefficients are provided, override observed days with
  # F × AC × D-derived expectation. Otherwise summarise observed days.
  days_summary <- summarize_fishing_days(
    trips,
    group_by = group_minor,
    boat_col = boat_col,
    alpha = alpha
  )
  # If pds_trips and pds_devices are provided, derive activity automatically
  # from GPS tracking data. The result populates `activity` if not already set.
  if (!is.null(pds_trips) && !is.null(pds_devices) && is.null(activity)) {
    logger::log_info(
      "Building BAC activity table from PDS trips + Airtable devices ..."
    )
    activity <- build_pds_activity(
      pds_trips = pds_trips,
      pds_devices = pds_devices,
      landings = landings
    )
  }

  if (!is.null(activity)) {
    logger::log_info("Applying provided activity coefficients (BAC / PAB).")
    ac_cols <- c("bac", "pab", "ac", "days_in_period")
    join_ac <- intersect(group_minor, colnames(activity))

    # Ensure all coalesce columns exist (fill missing as NA) so the join
    # produces a tibble with all three coefficient columns available.
    activity_for_join <- activity |>
      dplyr::select(dplyr::all_of(join_ac), dplyr::any_of(ac_cols))
    for (col in c("bac", "pab", "ac", "days_in_period")) {
      if (!col %in% colnames(activity_for_join)) {
        activity_for_join[[col]] <- NA_real_
      }
    }

    days_summary <- days_summary |>
      dplyr::left_join(activity_for_join, by = join_ac) |>
      dplyr::mutate(
        ac_combined = dplyr::coalesce(.data$bac, .data$pab, .data$ac),
        D = dplyr::coalesce(.data$days_in_period, 30),
        mean_days = dplyr::if_else(
          is.na(.data$ac_combined),
          .data$mean_days,
          .data$ac_combined * .data$D
        )
      )
  }

  # ── 13.5 Frame (with hybrid fallback)
  # If an explicit frame is provided (or derived from assets), the join with
  # the minor-stratum estimates can leave some cells with F_total = NA when
  # the frame doesn't cover every (district x fishing_unit) combination.
  # When `hybrid_frame = TRUE`, we fill those gaps with observed-boat counts
  # aggregated to the SAME granularity as the explicit frame. This is the
  # "best of both worlds": real F from the census where available, observed
  # counts as a placeholder, joined at consistent keys.
  if (is.null(frame)) {
    frame <- derive_frame_observed(
      landings,
      group_by = c(major_stratum, minor_stratum, "fishing_unit"),
      boat_col = boat_col
    )
  } else if (isTRUE(hybrid_frame)) {
    # Match the explicit frame's granularity for the fallback
    frame_keys <- intersect(
      c(major_stratum, minor_stratum, "fishing_unit"),
      colnames(frame)
    )
    observed_frame <- derive_frame_observed(
      landings,
      group_by = frame_keys,
      boat_col = boat_col
    )
    n_before <- nrow(frame)
    frame <- dplyr::bind_rows(
      frame,
      dplyr::anti_join(observed_frame, frame, by = frame_keys)
    )
    logger::log_info(
      "Hybrid frame: {n_before} cells from real frame + ",
      "{nrow(frame) - n_before} fallback cells from observed boats ",
      "(aggregated at keys: {paste(frame_keys, collapse = ' + ')})."
    )
  }

  # ── 13.6 Minor-stratum totals + compound RE
  minor <- estimate_minor_total(
    cpue_summary = cpue_summary,
    days_summary = days_summary,
    frame = frame,
    trips = trips,
    join_keys = group_minor,
    boat_col = boat_col,
    alpha = alpha
  ) |>
    flag_quality(threshold = re_threshold)

  # ── 13.7 Aggregation to major stratum
  major <- aggregate_to_major(
    minor,
    major_keys = c(major_stratum, "fishing_unit", period_col)
  ) |>
    flag_quality(threshold = re_threshold)

  # ── 13.8 Species breakdown
  species <- estimate_species_total(
    landings = landings,
    minor_estimates = minor,
    join_keys = group_minor
  )

  # ── 13.9 Quality summary
  quality <- minor |>
    dplyr::count(quality_overall, name = "n_groups") |>
    dplyr::mutate(pct = n_groups / sum(n_groups))

  logger::log_info(
    "FAO aggregation: {nrow(minor)} minor-stratum estimates | ",
    "{sum(minor$quality_overall == 'pass', na.rm = TRUE)} pass, ",
    "{sum(minor$quality_overall == 'warn', na.rm = TRUE)} warn, ",
    "{sum(minor$quality_overall == 'fail', na.rm = TRUE)} fail"
  )

  list(
    metric = metric,
    trips = trips,
    cpue_summary = cpue_summary,
    days_summary = days_summary,
    frame = frame,
    minor = minor,
    major = major,
    species = species,
    quality = quality
  )
}
