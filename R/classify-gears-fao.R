#' Classify Peskas Gears Against the FAO ISSCFG
#'
#' @description
#' Pulls the `gears` table from the Peskas Airtable frame base, maps each gear to
#' its FAO ISSCFG abbreviation, code, label and macro-category, and optionally
#' writes the classification back to Airtable.
#'
#' @details
#' Gears are matched on `standard_name`, the stable gear label in Peskas, since
#' the short `code` (e.g. `GN`, `RN`) is reused across countries. The FAO fields
#' already present in Airtable are overwritten from the ISSCFG reference, while an
#' existing `fao_mapping_note` is kept where set. Gears with no mapping are logged
#' and skipped (never written as `NA`).
#'
#' When `update_airtable = TRUE` the FAO fields are patched in batches via
#' [bulk_update_airtable()] with `typecast = TRUE`, so new single-select options
#' (e.g. `FPO`, `GTR`) are created on the fly.
#'
#' @param conf Configuration object from [read_config()]. Read automatically when
#'   `NULL`.
#' @param update_airtable Logical. Write the classification back to the Airtable
#'   `gears` table. Defaults to `FALSE`.
#'
#' @return A tibble with one row per gear and its FAO classification.
#'
#' @keywords workflow
#' @export
classify_gears_fao <- function(conf = NULL, update_airtable = FALSE) {
  if (is.null(conf)) {
    conf <- read_config()
  }

  logger::log_info("Pulling gears table from Airtable")
  gears <- airtable_to_df(
    base_id = conf$airtable$frame$base_id,
    table_name = "gears",
    token = conf$airtable$token
  ) |>
    janitor::clean_names()

  isscfg <- fao_isscfg()
  crosswalk <- gear_fao_crosswalk()

  classified <- gears |>
    dplyr::select(-dplyr::any_of(c(
      "fao_abbrev",
      "fao_code",
      "fao_label",
      "fao_category",
      "fao_mapping_confidence"
    ))) |>
    dplyr::left_join(crosswalk, by = "standard_name") |>
    dplyr::mutate(
      fao_mapping_note = dplyr::coalesce(
        .data$fao_mapping_note,
        .data$mapping_note
      )
    ) |>
    dplyr::select(-"mapping_note") |>
    dplyr::left_join(
      dplyr::select(
        isscfg,
        "fao_abbrev",
        "fao_code",
        "fao_label",
        "fao_category"
      ),
      by = "fao_abbrev"
    ) |>
    dplyr::select(
      "country",
      "standard_name",
      "code",
      "parent_code",
      "fao_abbrev",
      "fao_code",
      "fao_label",
      "fao_category",
      "fao_mapping_confidence",
      "fao_mapping_note",
      "airtable_id"
    ) |>
    dplyr::arrange(.data$country, .data$fao_category, .data$standard_name)

  unmapped <- dplyr::filter(classified, is.na(.data$fao_abbrev))
  if (nrow(unmapped) > 0) {
    logger::log_warn(
      "{nrow(unmapped)} gears without FAO mapping: {paste(sort(unique(unmapped$standard_name)), collapse = ', ')}"
    )
  }

  if (isTRUE(update_airtable)) {
    updates <- classified |>
      dplyr::filter(!is.na(.data$fao_abbrev)) |>
      dplyr::select(
        "airtable_id",
        "fao_abbrev",
        "fao_code",
        "fao_label",
        "fao_category",
        "fao_mapping_confidence",
        "fao_mapping_note"
      )

    logger::log_info("Writing {nrow(updates)} gear classifications to Airtable")
    bulk_update_airtable(
      base_id = conf$airtable$frame$base_id,
      table_name = "gears",
      token = conf$airtable$token,
      updates_df = updates,
      typecast = TRUE
    )
  }

  classified
}

#' FAO ISSCFG gear classification reference
#'
#' Revised International Standard Statistical Classification of Fishing Gear
#' (ISSCFG Rev.1, 2016), from He et al. (2021), FAO Fisheries and Aquaculture
#' Technical Paper 672.
#'
#' @return A tibble of ISSCFG subcategories with `fao_category`, `fao_abbrev`,
#'   `fao_code` and `fao_label`.
#'
#' @noRd
fao_isscfg <- function() {
  tibble::tribble(
    ~fao_category, ~fao_abbrev, ~fao_code, ~fao_label,
    "Surrounding nets", "PS", 1.1, "Purse seines",
    "Surrounding nets", "LA", 1.2, "Surrounding nets without purse lines",
    "Surrounding nets", "SUX", 1.9, "Surrounding nets (nei)",
    "Seine nets", "SB", 2.1, "Beach seines",
    "Seine nets", "SV", 2.2, "Boat seines",
    "Seine nets", "SX", 2.9, "Seine nets (nei)",
    "Trawls", "TBB", 3.11, "Beam trawls",
    "Trawls", "OTB", 3.12, "Single boat bottom otter trawls",
    "Trawls", "OTT", 3.13, "Twin bottom otter trawls",
    "Trawls", "OTP", 3.14, "Multiple bottom otter trawls",
    "Trawls", "PTB", 3.15, "Bottom pair trawls",
    "Trawls", "TB", 3.19, "Bottom trawls (nei)",
    "Trawls", "OTM", 3.21, "Single boat midwater otter trawls",
    "Trawls", "PTM", 3.22, "Midwater pair trawls",
    "Trawls", "TM", 3.29, "Midwater trawls (nei)",
    "Trawls", "TSP", 3.3, "Semipelagic trawls",
    "Trawls", "TX", 3.9, "Trawls (nei)",
    "Dredges", "DRB", 4.1, "Towed dredges",
    "Dredges", "DRH", 4.2, "Hand dredges",
    "Dredges", "DRM", 4.3, "Mechanized dredges",
    "Dredges", "DRX", 4.9, "Dredges (nei)",
    "Lift nets", "LNP", 5.1, "Portable lift nets",
    "Lift nets", "LNB", 5.2, "Boat-operated lift nets",
    "Lift nets", "LNS", 5.3, "Shore-operated stationary lift nets",
    "Lift nets", "LN", 5.9, "Lift nets (nei)",
    "Falling gear", "FCN", 6.1, "Cast nets",
    "Falling gear", "FCO", 6.2, "Cover pots/Lantern nets",
    "Falling gear", "FG", 6.9, "Falling gear (nei)",
    "Gillnets and entangling nets", "GNS", 7.1, "Set gillnets (anchored)",
    "Gillnets and entangling nets", "GND", 7.2, "Drift gillnets",
    "Gillnets and entangling nets", "GNC", 7.3, "Encircling gillnets",
    "Gillnets and entangling nets", "GNF", 7.4, "Fixed gillnets (on stakes)",
    "Gillnets and entangling nets", "GTR", 7.5, "Trammel nets",
    "Gillnets and entangling nets", "GTN", 7.6, "Combined gillnets-trammel nets",
    "Gillnets and entangling nets", "GEN", 7.9, "Gillnets and entangling nets (nei)",
    "Traps", "FPN", 8.1, "Stationary uncovered pound nets",
    "Traps", "FPO", 8.2, "Pots",
    "Traps", "FYK", 8.3, "Fyke nets",
    "Traps", "FSN", 8.4, "Stow nets",
    "Traps", "FWR", 8.5, "Barriers, fences, weirs, etc.",
    "Traps", "FAR", 8.6, "Aerial traps",
    "Traps", "FIX", 8.9, "Traps (nei)",
    "Hooks and lines", "LHP", 9.1, "Handlines and hand-operated pole-and-lines",
    "Hooks and lines", "LHM", 9.2, "Mechanized lines and pole-and-lines",
    "Hooks and lines", "LLS", 9.31, "Set longlines",
    "Hooks and lines", "LLD", 9.32, "Drifting longlines",
    "Hooks and lines", "LL", 9.39, "Longlines (nei)",
    "Hooks and lines", "LVT", 9.4, "Vertical lines",
    "Hooks and lines", "LTL", 9.5, "Trolling lines",
    "Hooks and lines", "LX", 9.9, "Hooks and lines (nei)",
    "Miscellaneous gear", "HAR", 10.1, "Harpoons",
    "Miscellaneous gear", "MHI", 10.2, "Hand implements (spears, tongs, rakes, etc.)",
    "Miscellaneous gear", "MPM", 10.3, "Pumps",
    "Miscellaneous gear", "MEL", 10.4, "Electric fishing",
    "Miscellaneous gear", "MPN", 10.5, "Pushnets",
    "Miscellaneous gear", "MSP", 10.6, "Scoopnets",
    "Miscellaneous gear", "MDR", 10.7, "Drive-in nets",
    "Miscellaneous gear", "MDV", 10.8, "Diving",
    "Miscellaneous gear", "MIS", 10.9, "Gear nei",
    "Gear not known", "NK", 99.9, "Gear not known"
  )
}

#' Peskas gear to FAO abbreviation crosswalk
#'
#' Maps each Peskas `standard_name` to an FAO ISSCFG abbreviation, with a mapping
#' confidence and a note for the ambiguous cases. Confidence is `high` for direct
#' 1:1 matches, `medium` where the gear detail is ambiguous, and `low` for
#' generic parent categories that need finer classification.
#'
#' @return A tibble with `standard_name`, `fao_abbrev`, `fao_mapping_confidence`
#'   and `mapping_note`.
#'
#' @noRd
gear_fao_crosswalk <- function() {
  tibble::tribble(
    ~standard_name, ~fao_abbrev, ~fao_mapping_confidence, ~mapping_note,
    "Gill Net", "GEN", "medium", "Gillnet type not distinguished in Peskas; using gillnets nei. Refine to GNS/GND/GNF if data allows.",
    "Deep Gill Net", "GEN", "medium", "Deep gill net, likely set/anchored (GNS) but kept as nei pending confirmation.",
    "Tangle Net", "GTR", "medium", "Tangle nets are typically trammel nets; confirm locally.",
    "Trammel Net", "GTR", "high", "Direct match to trammel nets.",
    "Ring Net", "LA", "medium", "Ring nets are surrounding nets without a purse line; confirm locally.",
    "Purse Seine", "PS", "high", "Direct match to purse seines.",
    "Beach Seine", "SB", "high", "Direct match to beach seines.",
    "Reef Seine", "SB", "medium", "Reef seine treated as a beach/shore seine variant; confirm operation.",
    "Seine", "SX", "medium", "Generic seine; mapped to seine nets nei.",
    "Cast Net", "FCN", "high", "Direct match to cast nets.",
    "Scoop Net", "MSP", "high", "Direct match to scoopnets.",
    "Nets", "GEN", "low", "Generic parent category; defaulting to gillnets nei. Needs finer classification.",
    "Hand Line", "LHP", "high", "Handlines and hand-operated pole-and-lines.",
    "Stick Rod", "LHP", "high", "Stick rods operated as hand lines.",
    "Pole and Line", "LHP", "high", "Direct match to hand-operated pole-and-lines.",
    "Rod and Reel", "LHP", "high", "Rod and reel operated as a hand line.",
    "Dropline", "LVT", "high", "Droplines are vertical lines.",
    "Long Line", "LL", "high", "Longlines nei; refine to LLS/LLD if set vs drift is recorded.",
    "Trolling Line", "LTL", "high", "Direct match to trolling lines.",
    "Lobster Trap", "FPO", "high", "Lobster traps are pots.",
    "Cage Trap", "FPO", "medium", "Cage traps treated as pots; confirm construction.",
    "Trap", "FIX", "medium", "Generic trap; mapped to traps nei. Refine to FPO if pot-type.",
    "Spear Gun", "MHI", "high", "Spear guns fall under hand implements / spears.",
    "Harpoon", "HAR", "high", "Direct match to harpoons.",
    "Gleaning", "MDV", "high", "Gleaning (hand-collecting at low tide) mapped to diving / hand-gathering.",
    "Trawl Net", "TX", "medium", "Generic trawl; mapped to trawls nei. Confirm trawl type if possible."
  )
}
