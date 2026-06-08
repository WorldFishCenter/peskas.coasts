# Frame survey integration, gear macro-categories, and PDS-derived activity.
#
# Functions in this file translate the Airtable assets (frame survey,
# pds_devices) and the PDS GPS-trip records into the inputs that
# estimate_catch_fao() consumes (frame counts, gear lookup, activity
# coefficients).

#' Default gear lookup: 23 survey gears -> 5 PDS macro-categories
#'
#' Maps the gear labels used in country landing surveys (ADNAP, KEFS, Zanzibar)
#' onto the 5 PDS macro-categories used by the Airtable `pds_devices` table:
#' `gillnet_trammel`, `hook_and_line`, `seine_encircling`, `line_and_gillnet`,
#' `other_unknown`. The macro grouping reduces the explosion of (vessel x gear)
#' combinations (e.g. ~85 in Kenya) to a handful of homogeneous fishing units,
#' which dramatically improves cell-level sample sizes and FAO pass rates.
#'
#' Override this default by passing your own tibble as `gear_lookup` to
#' [estimate_catch_fao()].
#'
#' @export
default_gear_lookup <- tibble::tribble(
  ~gear           , ~gear_class        ,
  "Gill Net"      , "gillnet_trammel"  ,
  "Trammel Net"   , "gillnet_trammel"  ,
  "Tangle Net"    , "gillnet_trammel"  ,
  "Hand Line"     , "hook_and_line"    ,
  "Long Line"     , "hook_and_line"    ,
  "Pole and Line" , "hook_and_line"    ,
  "Stick Rod"     , "hook_and_line"    ,
  "Trolling Line" , "hook_and_line"    ,
  "Dropline"      , "hook_and_line"    ,
  "Beach Seine"   , "seine_encircling" ,
  "Purse Seine"   , "seine_encircling" ,
  "Ring Net"      , "seine_encircling" ,
  "Reef Seine"    , "seine_encircling" ,
  "Seine"         , "seine_encircling" ,
  "Trawl Net"     , "seine_encircling" ,
  "Scoop Net"     , "seine_encircling" ,
  "Mixed gears"   , "line_and_gillnet" ,
  "Trap"          , "other_unknown"    ,
  "Cage Trap"     , "other_unknown"    ,
  "Cast Net"      , "other_unknown"    ,
  "Spear Gun"     , "other_unknown"    ,
  "Harpoon"       , "other_unknown"    ,
  "Gleaning"      , "other_unknown"    ,
  "Other"         , "other_unknown"
)


#' Apply gear macro-categories to a landings or frame tibble
#'
#' Overwrites the gear-name column with its corresponding macro-category
#' from `gear_lookup`. Operates on a `gear` column by default, but the
#' column name is configurable (e.g. `standard_name` for the Airtable frame).
#'
#' Rows where the gear value has no match in `gear_lookup` keep the original
#' label (a warning is emitted listing the unmapped values).
#'
#' @param data        A tibble containing the gear-name column.
#' @param gear_lookup A tibble with columns `gear` and `gear_class`. Default
#'                    `default_gear_lookup`.
#' @param gear_col    Name of the column to remap. Default `"gear"`.
#'
#' @return The input tibble with `gear_col` overwritten by the macro-category.
#' @export
apply_gear_macros <- function(
  data,
  gear_lookup = default_gear_lookup,
  gear_col = "gear"
) {
  if (!gear_col %in% colnames(data)) {
    logger::log_warn(
      "apply_gear_macros: column `{gear_col}` not in data — skipping."
    )
    return(data)
  }

  lookup <- gear_lookup |>
    dplyr::rename(.gear_in = "gear", .gear_out = "gear_class")

  data_out <- data |>
    dplyr::left_join(lookup, by = setNames(".gear_in", gear_col)) |>
    dplyr::mutate(
      "{gear_col}" := dplyr::coalesce(.data$.gear_out, .data[[gear_col]])
    ) |>
    dplyr::select(-".gear_out")

  unmapped <- setdiff(unique(stats::na.omit(data[[gear_col]])), lookup$.gear_in)
  if (length(unmapped) > 0L) {
    logger::log_warn(
      "apply_gear_macros: {length(unmapped)} unmapped gear value(s): ",
      paste(unmapped, collapse = ", "),
      ". Original labels kept."
    )
  }

  data_out
}


#' Build an activity table from PDS trips and devices
#'
#' Computes a Boat Activity Coefficient (BAC) per gear macro-category by
#' counting how many trips a GPS-tracked vessel makes per month. By default
#' the BAC is calculated **per (gear x year_month)** — i.e. each month gets
#' its own activity estimate — which is the FAO-orthodox approach
#' (de Graaf et al. 2017, Section 3 of the toolkit: "BAC ... the probability
#' that any boat will be active on any day during the month"). Set
#' `by_period = FALSE` to fall back to a single annual mean per gear class.
#'
#' The function joins the PDS trips (which carry `IMEI` and trip
#' timestamps) with the Airtable `pds_devices` table (which carries
#' `gear class` per IMEI), aggregates trips per boat per month, then
#' computes the mean monthly trip count per (gear class [x year_month]).
#' Each unique `(vessel_type x gear_class)` combination in the landings is
#' then assigned the corresponding BAC.
#'
#' When `by_period = TRUE`, months with no PDS observations for a given
#' gear class are simply absent from the output -- the orchestrator will
#' fall back to observed days for those cells. To get a graceful fallback
#' chain (monthly -> annual -> observed), call this function twice (once
#' with `by_period = TRUE`, once with `by_period = FALSE`) and bind the
#' rows, keeping the monthly value where present.
#'
#' @param pds_trips    Tibble of GPS trips with `IMEI` and `Started` columns
#'                     (as returned by reading the `pds-trips` parquet).
#' @param pds_devices  Tibble from the Airtable `pds_devices` table with
#'                     `imei` and `gear class` columns.
#' @param landings     The landings tibble (used to derive which
#'                     `(vessel_type, gear_class)` combinations exist).
#' @param days_in_period Days in the analysis period. Default 30.
#' @param by_period    Logical. If `TRUE` (default, FAO-orthodox), the BAC
#'                     is calculated per (gear x year_month). If `FALSE`,
#'                     a single annual mean per gear class is returned.
#'
#' @return A tibble with columns `fishing_unit`, `bac`, `pab`, `ac`,
#'         `days_in_period`, and (when `by_period = TRUE`) `year_month`.
#'         Consumable by the `activity` parameter of [estimate_catch_fao()].
#' @export
build_pds_activity <- function(
  pds_trips,
  pds_devices,
  landings,
  days_in_period = 30,
  by_period = TRUE
) {
  # Trip counts per (boat x gear_class x year_month) from the raw PDS data.
  # Each row is one boat's monthly trip count -- the unit FAO calls "AverF".
  trips_per_boat_month <- pds_trips |>
    dplyr::mutate(
      .imei = as.character(.data$IMEI),
      year_month = format(.data$Started, "%Y-%m")
    ) |>
    dplyr::left_join(
      pds_devices |>
        dplyr::transmute(
          .imei = as.character(.data$imei),
          boat_name = .data$boat_name,
          gear_class = .data$`gear class`
        ),
      by = ".imei"
    ) |>
    dplyr::filter(!is.na(.data$gear_class), !is.na(.data$boat_name)) |>
    dplyr::count(
      .data$boat_name,
      .data$gear_class,
      .data$year_month,
      name = "n_trips"
    )

  # Aggregate to BAC at the requested temporal grain.
  if (isTRUE(by_period)) {
    trips_summary <- trips_per_boat_month |>
      dplyr::group_by(.data$gear_class, .data$year_month) |>
      dplyr::summarise(
        mean_trips_per_month = mean(.data$n_trips),
        n_boats = dplyr::n_distinct(.data$boat_name),
        .groups = "drop"
      )
    logger::log_info(
      "build_pds_activity (by_period = TRUE, FAO-orthodox): ",
      "{nrow(trips_summary)} (gear x year_month) cells from ",
      "{dplyr::n_distinct(trips_per_boat_month$boat_name)} GPS-tracked boats."
    )
  } else {
    trips_summary <- trips_per_boat_month |>
      dplyr::group_by(.data$gear_class) |>
      dplyr::summarise(
        mean_trips_per_month = mean(.data$n_trips),
        n_boats = dplyr::n_distinct(.data$boat_name),
        .groups = "drop"
      )
    logger::log_info(
      "build_pds_activity (by_period = FALSE, annual mean): ",
      "{nrow(trips_summary)} gear classes from ",
      "{sum(trips_summary$n_boats)} unique GPS-tracked boats."
    )
  }

  # Unique fishing units in the landings (after gear macros have been
  # applied -- if they have, `gear` already holds the macro label).
  unique_fus <- landings |>
    dplyr::filter(!is.na(.data$vessel_type), !is.na(.data$gear)) |>
    dplyr::distinct(.data$vessel_type, .data$gear) |>
    dplyr::mutate(
      fishing_unit = paste(.data$vessel_type, .data$gear, sep = " | ")
    )

  if (isTRUE(by_period)) {
    # Cartesian-expand fishing units by every year_month present in PDS,
    # then drop the (gear x month) combinations not observed by PDS.
    out <- tidyr::crossing(
      unique_fus,
      trips_summary |> dplyr::distinct(.data$year_month)
    ) |>
      dplyr::left_join(
        trips_summary |>
          dplyr::transmute(
            gear = .data$gear_class,
            year_month = .data$year_month,
            bac = .data$mean_trips_per_month /
              .env$days_in_period,
            days_in_period = .env$days_in_period
          ),
        by = c("gear", "year_month")
      ) |>
      dplyr::filter(!is.na(.data$bac)) |>
      dplyr::transmute(
        fishing_unit = .data$fishing_unit,
        year_month = .data$year_month,
        bac = .data$bac,
        pab = NA_real_,
        ac = NA_real_,
        days_in_period = .data$days_in_period
      )
  } else {
    out <- unique_fus |>
      dplyr::left_join(
        trips_summary |>
          dplyr::transmute(
            gear = .data$gear_class,
            bac = .data$mean_trips_per_month /
              .env$days_in_period,
            days_in_period = .env$days_in_period
          ),
        by = "gear"
      ) |>
      dplyr::filter(!is.na(.data$bac)) |>
      dplyr::transmute(
        fishing_unit = .data$fishing_unit,
        bac = .data$bac,
        pab = NA_real_,
        ac = NA_real_,
        days_in_period = .data$days_in_period
      )
  }

  out
}


#' Download the Peskas Airtable assets RDS containing the frame survey
#'
#' The Airtable `assets` RDS bundled by Lorenzo's `model-fishery.R` pipeline
#' is the canonical source for the FAO frame `F_total`. This helper fetches
#' it from cloud storage and returns the full named list:
#'   `geo`, `taxa`, `gear`, `vessels`, `sites`, `forms`, `devices`, `frame`.
#'
#' The `frame` element has one row per (district x vessel-or-gear) with
#' `n_boats` (= F), `standard_name` (canonical fishing-unit label) and
#' `category_kind` ("vessel" or "gear").
#'
#' Note: uses `conf$storage$google$options` (not `options_coasts`).
#'
#' @param conf Output of `read_config()`.
#'
#' @return Named list as stored in the Airtable assets RDS.
#' @export
download_fao_frame <- function(conf) {
  logger::log_info("Downloading Airtable assets RDS for FAO frame ...")

  assets <- cloud_object_name(
    prefix = conf$metadata$airtable$assets,
    provider = conf$storage$google$key,
    version = "latest",
    extension = "rds",
    options = conf$storage$google$options
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds()

  if (!"frame" %in% names(assets)) {
    stop(
      "The downloaded assets RDS has no `frame` element. ",
      "Available elements: ",
      paste(names(assets), collapse = ", ")
    )
  }

  logger::log_info(
    "Frame loaded: {nrow(assets$frame)} rows across ",
    "{dplyr::n_distinct(assets$frame$country)} countries."
  )
  assets
}


#' Build a FAO `frame` tibble from the Airtable assets frame
#'
#' Filters `assets$frame` by `category_kind` and reshapes to the
#' (`gaul_2_name` x `fishing_unit` -> `F_total`) tibble that
#' `estimate_catch_fao()` consumes via its `frame` argument.
#'
#' Defaults to `level = "vessel"` because the Airtable frame stores
#' vessel and gear counts on separate rows -- there is no recorded
#' joint count of (vessel x gear) combinations. FAO toolkit p. 6
#' explicitly allows defining fishing units by vessel alone when gear
#' assignments are diverse or unknown.
#'
#' @param assets_frame The `frame` element of `download_fao_frame()`.
#' @param level        Either "vessel" (default), "gear", or "both".
#' @param country      Optional country filter (e.g. "Mozambique").
#'                     If `NULL`, all countries are kept.
#' @param gaul_2_filter Optional vector of `gaul_2_name` to keep.
#'                     Useful to restrict the frame to the districts
#'                     that appear in the landings being analysed.
#'
#' @return Tibble with columns `gaul_2_name`, `fishing_unit`, `F_total`.
#' @export
build_frame_table <- function(
  assets_frame,
  level = c("vessel", "gear", "both"),
  country = NULL,
  gaul_2_filter = NULL,
  gear_lookup = NULL
) {
  level <- match.arg(level)

  fr <- assets_frame
  if (!is.null(country)) {
    fr <- dplyr::filter(fr, .data$country %in% .env$country)
  }
  if (!is.null(gaul_2_filter)) {
    fr <- dplyr::filter(fr, .data$gaul_2_name %in% .env$gaul_2_filter)
  }

  fr <- switch(
    level,
    vessel = dplyr::filter(fr, .data$category_kind == "vessel"),
    gear = dplyr::filter(fr, .data$category_kind == "gear"),
    both = fr
  )

  # Apply gear macro-categories to the frame's standard_name when requested.
  # Only meaningful for level = "gear" (vessel-level frames have vessel
  # names, not gear names).
  if (!is.null(gear_lookup) && level == "gear") {
    fr <- apply_gear_macros(
      fr,
      gear_lookup = gear_lookup,
      gear_col = "standard_name"
    )
  }

  out <- fr |>
    dplyr::group_by(.data$gaul_2_name, .data$standard_name) |>
    dplyr::summarise(
      F_total = sum(.data$n_boats, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$F_total > 0) |>
    dplyr::rename(fishing_unit = "standard_name")

  logger::log_info(
    "build_frame_table (level = '{level}'): {nrow(out)} ",
    "(district x fishing_unit) cells from frame survey."
  )
  out
}


#' Disaggregate a vessel-level frame to (vessel x gear) using observed proportions
#'
#' The Airtable frame stores `F` separately by vessel and by gear -- it does
#' not record joint counts of (vessel x gear) combinations. This helper
#' bridges that gap by distributing each vessel's frame count across the
#' gears observed for that vessel in the landings, proportionally to trip
#' counts:
#'
#'   F(district, vessel, gear) = F_frame(district, vessel)
#'                              x p_observed(gear | district, vessel)
#'
#' The output is suitable for direct use as the `frame` argument of
#' `estimate_catch_fao()` when `fu_cols = c("vessel_type", "gear")`.
#'
#' Methodological note: the proportional split assumes that the share of
#' observed trips for each gear within a (district x vessel) group is
#' representative of the share of frame vessels using that gear. This is
#' an approximation -- some vessels may make disproportionately more trips
#' than others -- but it is dramatically better than treating
#' heterogeneous gears (e.g. trawl vs gillnet on the same motorised boat)
#' as a single fishing unit, which inflates within-cell variance and
#' destroys the relative-error estimates.
#'
#' Combinations with no observed trips are dropped (we cannot guess the
#' split). Callers may wish to apply a uniform split for unobserved gears
#' as a downstream refinement.
#'
#' @param frame      Tibble with `gaul_2_name`, `fishing_unit` (= vessel),
#'                   `F_total`. Output of `build_frame_table(level = 'vessel')`.
#' @param landings   Validated landings (raw, before `build_fishing_units`).
#' @param vessel_col Name of the vessel column in landings. Default
#'                   "vessel_type".
#' @param gear_col   Name of the gear column in landings. Default "gear".
#' @param sep        Separator between vessel and gear in the new
#'                   `fishing_unit` label. Default " | " (matches
#'                   `build_fishing_units()`).
#'
#' @return Tibble with columns `gaul_2_name`, `fishing_unit` (= vessel | gear),
#'         `F_total` (rounded to integer).
#' @export
disaggregate_frame_by_gear <- function(
  frame,
  landings,
  vessel_col = "vessel_type",
  gear_col = "gear",
  sep = " | "
) {
  # Observed proportions of gear within (district x vessel)
  proportions <- landings |>
    dplyr::filter(!is.na(.data[[vessel_col]]), !is.na(.data[[gear_col]])) |>
    dplyr::distinct(
      .data$gaul_2_name,
      vessel_type = .data[[vessel_col]],
      gear = .data[[gear_col]],
      .data$submission_id
    ) |>
    dplyr::count(
      .data$gaul_2_name,
      .data$vessel_type,
      .data$gear,
      name = "n_trips"
    ) |>
    dplyr::group_by(.data$gaul_2_name, .data$vessel_type) |>
    dplyr::mutate(prop = .data$n_trips / sum(.data$n_trips)) |>
    dplyr::ungroup()

  # Join frame (treating its `fishing_unit` as the vessel name) with
  # observed proportions, then multiply.
  out <- frame |>
    dplyr::rename(.vessel = "fishing_unit") |>
    dplyr::inner_join(
      proportions,
      by = c("gaul_2_name", ".vessel" = "vessel_type")
    ) |>
    dplyr::mutate(
      F_total = as.integer(round(.data$F_total * .data$prop)),
      fishing_unit = paste(.data$.vessel, .data$gear, sep = sep)
    ) |>
    dplyr::filter(.data$F_total > 0) |>
    dplyr::select("gaul_2_name", "fishing_unit", "F_total")

  logger::log_info(
    "disaggregate_frame_by_gear: {nrow(frame)} (district x vessel) -> ",
    "{nrow(out)} (district x vessel x gear) cells."
  )
  out
}


#' Disaggregate a gear-level frame to (vessel x gear) using observed proportions
#'
#' Mirror of `disaggregate_frame_by_gear()` for countries whose Airtable
#' frame records F by gear rather than by vessel (e.g. Kenya).
#'
#'   F(district, vessel, gear) = F_frame(district, gear)
#'                              x p_observed(vessel | district, gear)
#'
#' @param frame      Tibble with `gaul_2_name`, `fishing_unit` (= gear),
#'                   `F_total`. Output of `build_frame_table(level = 'gear')`.
#' @param landings   Validated landings (raw, before `build_fishing_units`).
#' @param vessel_col Name of the vessel column in landings. Default
#'                   "vessel_type".
#' @param gear_col   Name of the gear column in landings. Default "gear".
#' @param sep        Separator between vessel and gear in the new
#'                   `fishing_unit` label. Default " | ".
#'
#' @return Tibble with columns `gaul_2_name`, `fishing_unit` (= vessel | gear),
#'         `F_total` (rounded to integer).
#' @export
disaggregate_frame_by_vessel <- function(
  frame,
  landings,
  vessel_col = "vessel_type",
  gear_col = "gear",
  sep = " | "
) {
  proportions <- landings |>
    dplyr::filter(!is.na(.data[[vessel_col]]), !is.na(.data[[gear_col]])) |>
    dplyr::distinct(
      .data$gaul_2_name,
      vessel_type = .data[[vessel_col]],
      gear = .data[[gear_col]],
      .data$submission_id
    ) |>
    dplyr::count(
      .data$gaul_2_name,
      .data$vessel_type,
      .data$gear,
      name = "n_trips"
    ) |>
    dplyr::group_by(.data$gaul_2_name, .data$gear) |>
    dplyr::mutate(prop = .data$n_trips / sum(.data$n_trips)) |>
    dplyr::ungroup()

  out <- frame |>
    dplyr::rename(.gear = "fishing_unit") |>
    dplyr::inner_join(
      proportions,
      by = c("gaul_2_name", ".gear" = "gear")
    ) |>
    dplyr::mutate(
      F_total = as.integer(round(.data$F_total * .data$prop)),
      fishing_unit = paste(.data$vessel_type, .data$.gear, sep = sep)
    ) |>
    dplyr::filter(.data$F_total > 0) |>
    dplyr::select("gaul_2_name", "fishing_unit", "F_total")

  logger::log_info(
    "disaggregate_frame_by_vessel: {nrow(frame)} (district x gear) -> ",
    "{nrow(out)} (district x vessel x gear) cells."
  )
  out
}
