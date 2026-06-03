# =============================================================================
# FAO ISSCFG Gear Classification for Peskas Frame
# =============================================================================
# Purpose: Pull the 'gears' table from Airtable and produce a classification
#          data frame mapping each gear to its FAO abbreviation, ISSCFG code,
#          FAO label, FAO category (first-tier), and mapping confidence.
#
# The script does NOT write back to Airtable — output is a CSV for review.
# Reference: He et al. (2021) FAO Fisheries and Aquaculture Technical Paper 672
#            (ISSCFG Rev.1, 2016)
# =============================================================================

library(dplyr)
library(readr)

# Source the airtable helper (adjust path as needed inside your package)
# source("R/airtable.R")

# -----------------------------------------------------------------------------
# 1. Pull gear table from Airtable
# -----------------------------------------------------------------------------
# Requires conf object with conf$airtable$frame$base_id and conf$airtable$token
# conf <- peskas.coasts::read_config()

gears_raw <- airtable_to_df(
  base_id = "appMMEJYlJdfSJEjm",
  table_name = "gears",
  token = conf$airtable$token
) |>
  janitor::clean_names()

# Preview what we pulled
dplyr::glimpse(gears_raw)

# -----------------------------------------------------------------------------
# 2. FAO ISSCFG Rev.1 (2016) lookup table
#    Source: He et al. 2021, FAO TP 672, Table on pp. 2-3
#
# Columns:
#   fao_category   : First-tier gear category (e.g. "Surrounding nets")
#   fao_abbrev     : FAO standard abbreviation (e.g. "PS")
#   fao_code       : ISSCFG numeric code (e.g. 1.1)
#   fao_label      : Second-tier subcategory label (e.g. "Purse seines")
# -----------------------------------------------------------------------------
fao_isscfg <- tibble::tribble(
  ~fao_category,                   ~fao_abbrev, ~fao_code, ~fao_label,
  # 01 Surrounding nets
  "Surrounding nets",               "PS",        1.1,       "Purse seines",
  "Surrounding nets",               "LA",        1.2,       "Surrounding nets without purse lines",
  "Surrounding nets",               "SUX",       1.9,       "Surrounding nets (nei)",
  # 02 Seine nets
  "Seine nets",                     "SB",        2.1,       "Beach seines",
  "Seine nets",                     "SV",        2.2,       "Boat seines",
  "Seine nets",                     "SX",        2.9,       "Seine nets (nei)",
  # 03 Trawls
  "Trawls",                         "TBB",       3.11,      "Beam trawls",
  "Trawls",                         "OTB",       3.12,      "Single boat bottom otter trawls",
  "Trawls",                         "OTT",       3.13,      "Twin bottom otter trawls",
  "Trawls",                         "OTP",       3.14,      "Multiple bottom otter trawls",
  "Trawls",                         "PTB",       3.15,      "Bottom pair trawls",
  "Trawls",                         "TB",        3.19,      "Bottom trawls (nei)",
  "Trawls",                         "OTM",       3.21,      "Single boat midwater otter trawls",
  "Trawls",                         "PTM",       3.22,      "Midwater pair trawls",
  "Trawls",                         "TM",        3.29,      "Midwater trawls (nei)",
  "Trawls",                         "TSP",       3.3,       "Semipelagic trawls",
  "Trawls",                         "TX",        3.9,       "Trawls (nei)",
  # 04 Dredges
  "Dredges",                        "DRB",       4.1,       "Towed dredges",
  "Dredges",                        "DRH",       4.2,       "Hand dredges",
  "Dredges",                        "DRM",       4.3,       "Mechanized dredges",
  "Dredges",                        "DRX",       4.9,       "Dredges (nei)",
  # 05 Lift nets
  "Lift nets",                      "LNP",       5.1,       "Portable lift nets",
  "Lift nets",                      "LNB",       5.2,       "Boat-operated lift nets",
  "Lift nets",                      "LNS",       5.3,       "Shore-operated stationary lift nets",
  "Lift nets",                      "LN",        5.9,       "Lift nets (nei)",
  # 06 Falling gear
  "Falling gear",                   "FCN",       6.1,       "Cast nets",
  "Falling gear",                   "FCO",       6.2,       "Cover pots/Lantern nets",
  "Falling gear",                   "FG",        6.9,       "Falling gear (nei)",
  # 07 Gillnets and entangling nets
  "Gillnets and entangling nets",   "GNS",       7.1,       "Set gillnets (anchored)",
  "Gillnets and entangling nets",   "GND",       7.2,       "Drift gillnets",
  "Gillnets and entangling nets",   "GNC",       7.3,       "Encircling gillnets",
  "Gillnets and entangling nets",   "GNF",       7.4,       "Fixed gillnets (on stakes)",
  "Gillnets and entangling nets",   "GTR",       7.5,       "Trammel nets",
  "Gillnets and entangling nets",   "GTN",       7.6,       "Combined gillnets-trammel nets",
  "Gillnets and entangling nets",   "GEN",       7.9,       "Gillnets and entangling nets (nei)",
  # 08 Traps
  "Traps",                          "FPN",       8.1,       "Stationary uncovered pound nets",
  "Traps",                          "FPO",       8.2,       "Pots",
  "Traps",                          "FYK",       8.3,       "Fyke nets",
  "Traps",                          "FSN",       8.4,       "Stow nets",
  "Traps",                          "FWR",       8.5,       "Barriers, fences, weirs, etc.",
  "Traps",                          "FAR",       8.6,       "Aerial traps",
  "Traps",                          "FIX",       8.9,       "Traps (nei)",
  # 09 Hooks and lines
  "Hooks and lines",                "LHP",       9.1,       "Handlines and hand-operated pole-and-lines",
  "Hooks and lines",                "LHM",       9.2,       "Mechanized lines and pole-and-lines",
  "Hooks and lines",                "LLS",       9.31,      "Set longlines",
  "Hooks and lines",                "LLD",       9.32,      "Drifting longlines",
  "Hooks and lines",                "LL",        9.39,      "Longlines (nei)",
  "Hooks and lines",                "LVT",       9.4,       "Vertical lines",
  "Hooks and lines",                "LTL",       9.5,       "Trolling lines",
  "Hooks and lines",                "LX",        9.9,       "Hooks and lines (nei)",
  # 10 Miscellaneous gear
  "Miscellaneous gear",             "HAR",       10.1,      "Harpoons",
  "Miscellaneous gear",             "MHI",       10.2,      "Hand implements (spears, tongs, rakes, etc.)",
  "Miscellaneous gear",             "MPM",       10.3,      "Pumps",
  "Miscellaneous gear",             "MEL",       10.4,      "Electric fishing",
  "Miscellaneous gear",             "MPN",       10.5,      "Pushnets",
  "Miscellaneous gear",             "MSP",       10.6,      "Scoopnets",
  "Miscellaneous gear",             "MDR",       10.7,      "Drive-in nets",
  "Miscellaneous gear",             "MDV",       10.8,      "Diving",
  "Miscellaneous gear",             "MIS",       10.9,      "Gear nei",
  # 99 Gear not known
  "Gear not known",                 "NK",        99.9,      "Gear not known"
)

# -----------------------------------------------------------------------------
# 3. Peskas gear → FAO mapping
#
# Key: standard_name (from Airtable) is the authoritative gear label in Peskas.
#      We map by standard_name because the same code (e.g. GN, RN) can appear
#      in multiple countries — the standard_name is always stable.
#
# For gillnets (GN): mapped to GEN (nei) with medium confidence because
#   Peskas does not distinguish set vs. drift vs. encircling;
#
# Confidence levels:
#   high   — direct 1:1 match to a named subcategory
#   medium — plausible match but gear detail is ambiguous
#   low    — best-guess; needs field verification
# -----------------------------------------------------------------------------
peskas_to_fao <- tibble::tribble(
  ~standard_name,          ~fao_abbrev, ~fao_mapping_confidence, ~mapping_notes,
  "Gill Net",              "GEN",  "medium", "Gillnet type not distinguished; using GEN 07.9. Refine to GNS/GND/GNF if data allows.",
  "Deep Gill Net",         "GEN",  "medium", "Deep gill net — likely set/anchored (GNS) but using nei pending confirmation.",
  "Tangle Net",            "GTR",  "medium", "Tangle nets are typically trammel nets (GTR 07.5); confirm locally.",
  "Trammel Net",           "GTR",  "high",   "Direct match: Trammel nets (GTR 07.5).",
  "Purse Seine",           "PS",   "high",   "Direct match: Purse seines (PS 01.1).",
  "Beach Seine",           "SB",   "high",   "Direct match: Beach seines (SB 02.1).",
  "Reef Seine",            "SB",   "medium", "Reef seine is a beach/shore seine variant (SB 02.1); confirm operation method.",
  "Cast Net",              "FCN",  "high",   "Direct match: Cast nets (FCN 06.1).",
  "Seine",                 "SX",   "medium", "Generic seine; Seine nets nei (SX 02.9).",
  "Scoop Net",             "MSP",  "high",   "Direct match: Scoopnets (MSP 10.6).",
  "Nets",                  "GEN",  "low",    "Parent/generic category only; defaulting to gillnets nei. Needs finer classification.",
  "Hand Line",             "LHP",  "high",   "Handlines and hand-operated pole-and-lines (LHP 09.1).",
  "Stick Rod",             "LHP",  "high",   "Stick rods are hand-operated lines (LHP 09.1).",
  "Pole and Line",         "LHP",  "high",   "Direct match: Hand-operated pole-and-lines (LHP 09.1).",
  "Rod and Reel",          "LHP",  "high",   "Rod and reel is a hand-operated line (LHP 09.1).",
  "Dropline",              "LVT",  "high",   "Droplines are vertical lines (LVT 09.4).",
  "Long Line",             "LL",   "high",   "Longlines nei (LL 09.39); refine to LLS/LLD if set vs. drift is recorded.",
  "Trolling Line",         "LTL",  "high",   "Direct match: Trolling lines (LTL 09.5).",
  "Lobster Trap",          "FPO",  "high",   "Lobster traps are pots (FPO 08.2).",
  "Cage Trap",             "FPO",  "medium", "Cage traps are functionally pots (FPO 08.2); confirm construction.",
  "Trap",                  "FIX",  "medium", "Generic trap; Traps nei (FIX 08.9). Refine to FPO if pot-type.",
  "Fish Trap",             "FIX",  "medium", "Generic fish trap; Traps nei (FIX 08.9).",
  "Octopus Trap",          "FPO",  "high",   "Octopus traps are pots (FPO 08.2).",
  "Spear Gun",             "MHI",  "high",   "Spear guns: Hand implements / spears (MHI 10.2).",
  "Harpoon",               "HAR",  "high",   "Direct match: Harpoons (HAR 10.1).",
  "Gleaning",              "MDV",  "high",   "Gleaning (hand-collecting at low tide) maps to Diving/hand-gathering (MDV 10.8).",
  "Ring Net",              "LA",   "medium", "Ring nets are surrounding nets without purse line (LA 01.2); confirm locally.",
  "Trawl Net",             "TX",   "medium", "Generic trawl; Trawls nei (TX 03.9). Confirm trawl type if possible."
)

# -----------------------------------------------------------------------------
# 4. Join Airtable gear table → FAO classification
# -----------------------------------------------------------------------------
gears_classified <- gears_raw |>
  dplyr::select(-fao_abbrev, -fao_code, -fao_label,
                -fao_category, -fao_mapping_confidence) |>
  dplyr::left_join(peskas_to_fao, by = "standard_name") |>
  dplyr::mutate(
    fao_mapping_note = dplyr::coalesce(fao_mapping_note, mapping_notes)
  ) |>
  dplyr::select(-mapping_notes) |>
  dplyr::left_join(
    fao_isscfg |> dplyr::select(fao_abbrev, fao_code, fao_label, fao_category),
    by = "fao_abbrev"
  ) |>
  dplyr::select(
    country, standard_name, code, parent_code,
    fao_abbrev, fao_code, fao_label, fao_category,
    fao_mapping_confidence, fao_mapping_note,
    airtable_id
  ) |>
  dplyr::arrange(country, fao_category, standard_name)

# -----------------------------------------------------------------------------
# 5. Inspect unmapped gears (standard_name not in peskas_to_fao)
# -----------------------------------------------------------------------------
unmapped <- gears_classified |>
  dplyr::filter(is.na(fao_abbrev)) |>
  dplyr::select(country, standard_name, code, parent_code)

if (nrow(unmapped) > 0) {
  message("⚠️  The following gears have no FAO mapping — review manually:")
  print(unmapped)
} else {
  message("✅  All gears mapped successfully.")
}

# -----------------------------------------------------------------------------
# 6. Summary by FAO category
# -----------------------------------------------------------------------------
gears_classified |>
  dplyr::count(fao_category, fao_label, fao_abbrev, fao_mapping_confidence) |>
  dplyr::arrange(fao_category) |>
  print(n = Inf)

# -----------------------------------------------------------------------------
# 7. Export for Lorenzo
# -----------------------------------------------------------------------------
output_path <- "gears_fao_classification.csv"

gears_classified |>
  dplyr::select(-airtable_id) |>
  readr::write_csv("gears_fao_classification.csv")

gears_classified |>
  readr::write_csv("gears_fao_classification_full.csv")

message("Exported: ", output_path)
message("Exported: gears_fao_classification_full.csv")
