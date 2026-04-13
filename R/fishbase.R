#' Pull a Table from Both FishBase and SeaLifeBase
#'
#' Downloads the same table from both the FishBase and SeaLifeBase servers and
#' row-binds the results, adding a `server` column to identify the source.
#'
#' @param tbl_name A character string naming the rfishbase table to retrieve
#'   (e.g. `"species"`, `"families"`, `"ecology"`, `"estimate"`,
#'   `"faoareas"`).
#'
#' @return A tibble combining rows from both servers with an additional
#'   `server` column (`"fishbase"` or `"sealifebase"`).
#'
#' @keywords internal
get_combined_tbl <- function(tbl_name) {
  dplyr::bind_rows(
    rfishbase::fb_tbl(tbl_name, server = "fishbase") |>
      dplyr::mutate(server = "fishbase"),
    rfishbase::fb_tbl(tbl_name, server = "sealifebase") |>
      dplyr::mutate(server = "sealifebase")
  )
}

#' Build a Unified Taxonomy Backbone from FishBase and SeaLifeBase
#'
#' Joins the `species` and `families` tables from both FishBase and
#' SeaLifeBase into a single reference table with scientific names and
#' higher-level taxonomy.
#'
#' @return A tibble with columns: `SpecCode`, `sci_name`, `Genus`, `Species`,
#'   `Family`, `Order`, `Class`, `server`.
#'
#' @keywords internal
get_taxa_backbone <- function() {
  spp <- get_combined_tbl("species")
  fam <- get_combined_tbl("families")

  spp |>
    dplyr::select("SpecCode", "Genus", "Species", "FamCode", "server") |>
    tidyr::unite("sci_name", "Genus", "Species", sep = " ", remove = FALSE) |>
    dplyr::left_join(
      fam |> dplyr::select("FamCode", "Family", "Order", "Class", "server"),
      by = c("FamCode", "server")
    ) |>
    dplyr::select(-"FamCode")
}

#' Expand Taxa to FishBase / SeaLifeBase Species Matches
#'
#' Given a data frame of taxa names, searches FishBase and SeaLifeBase for
#' matching species. Matching is attempted at multiple taxonomic ranks: species
#' (binomial), genus, family, order, and class. This allows coarser records
#' (e.g. `"Lutjanidae spp"`) to expand to all species in that family.
#'
#' @param data A data frame with at minimum the following columns:
#'   - `alpha3_code`: Three-letter country/region code.
#'   - `scientific_name`: Scientific name at any taxonomic rank; trailing
#'     ` spp` is stripped before matching.
#'
#' @return A tibble with columns:
#'   - `alpha3_code`: Passed through from input.
#'   - `original_name`: The original `scientific_name` value.
#'   - `SpecCode`: FishBase / SeaLifeBase species identifier.
#'   - `species_found`: Matched binomial species name.
#'   - `server`: Source database (`"fishbase"` or `"sealifebase"`).
#'
#' @details
#' The function builds a lookup dictionary by pivoting the taxonomy backbone
#' wide-to-long across ranks, so a single join resolves names at any level.
#' Records that do not match any rank are silently dropped (inner join).
#'
#' @keywords taxa
#' @export
#'
#' @examples
#' \dontrun{
#' taxa <- data.frame(
#'   alpha3_code = c("KEN", "TZA"),
#'   scientific_name = c("Lethrinus nebulosus", "Lutjanidae")
#' )
#' expand_taxonomic_info(taxa)
#' }
expand_taxonomic_info <- function(data) {
  master_taxa <- get_taxa_backbone()

  taxa_dictionary <- master_taxa |>
    dplyr::mutate(species_found = .data$sci_name) |>
    tidyr::pivot_longer(
      cols = c("sci_name", "Genus", "Family", "Order", "Class"),
      names_to = "rank_level",
      values_to = "search_name"
    ) |>
    dplyr::select("SpecCode", "species_found", "search_name", "server") |>
    dplyr::filter(!is.na(.data$search_name)) |>
    dplyr::distinct()

  data |>
    dplyr::mutate(
      search_name = stringr::str_replace(.data$scientific_name, " spp$", "")
    ) |>
    dplyr::inner_join(taxa_dictionary, by = "search_name") |>
    dplyr::select(
      "alpha3_code",
      original_name = "scientific_name",
      "SpecCode",
      "species_found",
      "server"
    ) |>
    dplyr::distinct() |>
    dplyr::as_tibble()
}

#' Enrich Taxa with FishBase and SeaLifeBase Biological Data
#'
#' Main pipeline function that downloads the taxa metadata from cloud storage,
#' expands each record to matching FishBase / SeaLifeBase species (filtered to
#' FAO Area 57 — Western Indian Ocean), and joins biological attributes
#' including vulnerability, trophic level, feeding guild, and nutrient
#' composition. The final dataset is uploaded as a versioned Parquet file to
#' the project cloud bucket.
#'
#' @param log_threshold Logging threshold passed to [logger::log_threshold()].
#'   Defaults to [logger::DEBUG].
#'
#' @return Invisible NULL. Called for its side effect of uploading the enriched
#'   taxa Parquet file to cloud storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Downloads the latest metadata RDS file (Airtable assets) from GCS.
#' 2. Extracts the `taxa` table and calls [expand_taxonomic_info()].
#' 3. Filters species to FAO Area 57 (or those without an area assignment).
#' 4. Joins species-level data from the `species`, `ecology`, and `estimate`
#'    tables.
#' 5. Deduplicates by taking the first non-`NA` value per group.
#' 6. Cleans column names with [janitor::clean_names()].
#' 7. Uploads the result via [upload_parquet_to_cloud()] using the
#'    `metadata.fishbase.taxa_enriched.file_prefix` configuration key.
#'
#' @keywords workflow
#' @export
#'
#' @examples
#' \dontrun{
#' coasts::enrich_taxa()
#' }
enrich_taxa <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # ── 1. Load taxa from cloud ───────────────────────────────────────────────────
  logger::log_info("Downloading taxa metadata from cloud storage")
  taxa <- cloud_object_name(
    prefix = conf$metadata$airtable$name,
    provider = conf$storage$google$key,
    version = "latest",
    extension = "rds",
    options = conf$storage$google$options
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds() |>
    purrr::keep_at("taxa") |>
    purrr::pluck("taxa") |>
    dplyr::select("alpha3_code", "scientific_name") |>
    dplyr::distinct()

  # ── 2. Expand to FishBase / SeaLifeBase species ───────────────────────────────
  logger::log_info("Expanding taxa against FishBase and SeaLifeBase")
  expanded_assets <- taxa |>
    expand_taxonomic_info()

  # ── 3. Filter to FAO Area 57 (Western Indian Ocean) ──────────────────────────
  logger::log_info("Filtering to FAO Area 57")
  faoareas <- get_combined_tbl("faoareas") |>
    dplyr::filter(.data$SpecCode %in% expanded_assets$SpecCode) |>
    dplyr::select("SpecCode", "AreaCode", "server")

  expanded_assets_filtered <- expanded_assets |>
    dplyr::left_join(faoareas, by = c("SpecCode", "server")) |>
    dplyr::filter(.data$AreaCode %in% c(NA_integer_, 57)) |>
    dplyr::select(-"AreaCode")

  # ── 4. Pull biological tables ─────────────────────────────────────────────────
  logger::log_info("Fetching biological data from FishBase / SeaLifeBase")
  target_codes <- expanded_assets_filtered$SpecCode

  species_tab <- get_combined_tbl("species") |>
    dplyr::filter(.data$SpecCode %in% target_codes) |>
    dplyr::select(
      "SpecCode",
      "server",
      "Vulnerability",
      "VulnerabilityClimate",
      "Dangerous",
      "MainCatchingMethod",
      "Importance",
      "DemersPelag"
    ) |>
    dplyr::distinct()

  trophic_tab <- get_combined_tbl("ecology") |>
    dplyr::filter(.data$SpecCode %in% target_codes) |>
    dplyr::select(
      "SpecCode",
      "server",
      "FoodTroph",
      "DietTroph",
      "FeedingType"
    ) |>
    dplyr::mutate(
      feeding_guild = dplyr::case_when(
        .data$FeedingType %in% c("grazing on aquatic plants") ~ "herbivore",
        .data$FeedingType %in%
          c("filtering plankton", "selective plankton feeding") ~ "planktivore",
        .data$FeedingType %in%
          c(
            "plants/detritus+animals (troph. 2.2-2.79)",
            "variable"
          ) ~ "omnivore",
        .data$FeedingType %in% c("hunting macrofauna (predator)") ~ "carnivore",
        .data$FeedingType %in%
          c(
            "browsing on substrate",
            "sucking food-containing material"
          ) ~ "detritivore",
        .data$FeedingType %in%
          c(
            "feeding on dead animals (scavenger)",
            "feeding on the prey of a host (commensal)",
            "picking parasites off a host (cleaner)",
            "feeding on a host (parasite)"
          ) ~ "specialist",
        TRUE ~ "uncertain"
      )
    )

  nutrients_tab <- get_combined_tbl("estimate") |>
    dplyr::filter(.data$SpecCode %in% target_codes) |>
    dplyr::select(
      "SpecCode",
      "server",
      "Calcium",
      "Iron",
      "Omega3",
      "Protein",
      "VitaminA",
      "Zinc"
    ) |>
    dplyr::distinct()

  # ── 5. Join and deduplicate ───────────────────────────────────────────────────
  logger::log_info("Joining and deduplicating biological data")
  all_dat <- list(
    expanded_assets_filtered,
    species_tab,
    trophic_tab,
    nutrients_tab
  ) |>
    purrr::reduce(dplyr::left_join, by = c("SpecCode", "server")) |>
    dplyr::group_by(
      .data$alpha3_code,
      .data$original_name,
      .data$SpecCode,
      .data$species_found,
      .data$server
    ) |>
    dplyr::summarise(
      dplyr::across(dplyr::everything(), ~ dplyr::first(stats::na.omit(.x))),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      MainCatchingMethod = dplyr::if_else(
        .data$MainCatchingMethod == " ",
        NA_character_,
        .data$MainCatchingMethod
      )
    ) |>
    dplyr::rename(vulnerability_fishing = "Vulnerability") |>
    janitor::clean_names()

  # ── 6. Upload to cloud ────────────────────────────────────────────────────────
  logger::log_info(
    "Uploading enriched taxa data ({nrow(all_dat)} rows) to cloud storage"
  )
  upload_parquet_to_cloud(
    data = all_dat,
    prefix = conf$metadata$fishbase$taxa_enriched$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}
