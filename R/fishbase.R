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

#' Resolve the FAO Major Fishing Area Filter
#'
#' Determines which FAO major fishing area codes to restrict species to.
#' Resolution order is: the explicit `fao_areas` argument, then the
#' `metadata.fishbase.fao_areas` configuration key, then a default of
#' `c(51, 57)` — the Western and Eastern Indian Ocean.
#'
#' @param conf A configuration list as returned by [read_config()].
#' @param fao_areas Optional numeric vector of FAO major fishing area codes. If
#'   `NULL` (default) the value is taken from configuration.
#'
#' @return An integer vector of FAO area codes.
#'
#' @details
#' Releases up to 4.5.0 hardcoded the area as `57`, annotated "Western Indian
#' Ocean". That annotation was wrong. FAO Area 57 is the **Eastern** Indian
#' Ocean; the Western Indian Ocean — the water Kenya, Mozambique and Zanzibar
#' actually fish — is Area **51**.
#'
#' The default from 4.6.0 is `c(51, 57)` rather than 51 alone, because
#' FishBase's area assignments are incomplete at family level. Restricting to 51
#' by itself drops 19 taxon codes entirely, among them `CLP` — Clupeidae,
#' "Herrings, sardines nei" — whose 15 backbone species all carry area
#' assignments, 2 of which include 57 and none of which include 51. Dropping a
#' major Kenyan and Zanzibari fishery to a gap in reference metadata is the
#' wrong trade.
#'
#' Because the filter is a disjunction, `c(51, 57)` is a strict superset of
#' pre-4.6.0 output: every species that resolved before still resolves. Pass
#' `fao_areas = 57` to reproduce pre-4.6.0 output exactly.
#'
#' Countries outside the Indian Ocean must set the key — Timor-Leste, for
#' example, is FAO Area 71 (Pacific, Western Central).
#'
#' @keywords internal
resolve_fao_areas <- function(conf, fao_areas = NULL) {
  fao_areas <- fao_areas %||% conf$metadata$fishbase$fao_areas %||% c(51, 57)

  fao_areas <- as.integer(fao_areas)
  fao_areas <- fao_areas[!is.na(fao_areas)]

  if (length(fao_areas) == 0) {
    stop(
      "`fao_areas` resolved to no usable FAO area codes. Provide at least one ",
      "integer code, e.g. 51 (Indian Ocean, Western) or 71 (Pacific, Western ",
      "Central)."
    )
  }

  fao_areas
}

#' Restrict Expanded Taxa to One or More FAO Major Fishing Areas
#'
#' Joins the `faoareas` table from both servers onto an expanded taxa table and
#' keeps only species recorded in the requested FAO major fishing areas.
#' Species with no area assignment at all are always retained.
#'
#' @param expanded A table as returned by [expand_taxonomic_info()].
#' @param fao_areas An integer vector of FAO major fishing area codes.
#'
#' @return `expanded`, filtered, with the `AreaCode` column dropped.
#'
#' @keywords internal
filter_by_fao_area <- function(expanded, fao_areas) {
  faoareas <- get_combined_tbl("faoareas") |>
    dplyr::filter(.data$SpecCode %in% expanded$SpecCode) |>
    dplyr::select("SpecCode", "AreaCode", "server")

  expanded |>
    dplyr::left_join(faoareas, by = c("SpecCode", "server")) |>
    dplyr::filter(.data$AreaCode %in% c(NA_integer_, fao_areas)) |>
    dplyr::select(-"AreaCode")
}

#' Expand Taxa to FishBase / SeaLifeBase Species Matches
#'
#' Given a data frame of taxa names, searches FishBase and SeaLifeBase for
#' matching species. Matching is attempted at multiple taxonomic ranks: species
#' (binomial), genus, family, order, and class. This allows coarser records
#' (e.g. `"Lutjanidae spp"`) to expand to all species in that family.
#'
#' @param data A data frame with at minimum the following columns:
#'   - `alpha3_code`: FAO 3-alpha (interagency) code identifying the taxon
#'     group, e.g. `"EMP"` for emperors. Passed through untouched.
#'   - `scientific_name`: Scientific name at any taxonomic rank; trailing
#'     ` spp` is stripped before matching.
#' @param strip_parentheticals Logical. If `TRUE`, a trailing parenthetical is
#'   removed from `scientific_name` before matching, so that
#'   `"Haemulidae (=Pomadasyidae)"` matches the family `Haemulidae`. Defaults to
#'   `FALSE`, which preserves the historical behaviour of leaving such names
#'   unmatched. See Details.
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
#' Matching is against the FishBase / SeaLifeBase backbone only. It therefore
#' resolves nothing for names that are not ranks those databases carry —
#' tribes (`"Thunnini"`), infraorders (`"Reptantia"`, `"Brachyura"`), informal
#' groupings (`"Osteichthyes"`, `"Algae"`, `"Selachimorpha"`) — nor for
#' superseded binomials (`"Leiognathus equulus"`, which FishBase now lists as
#' *Leiognathus equula*). Such taxa need a synonym or common-name route.
#'
#' `strip_parentheticals` exists because some FAO names carry an alternative
#' name in brackets. Three appear in the Peskas frame:
#' `"Haemulidae (=Pomadasyidae)"`, `"Selachimorpha (Pleurotremata)"` and
#' `"Labridae (ex Scaridae)"`. It is off by default precisely because turning it
#' on changes results for taxa that existing pipelines already publish — with it
#' on, `Haemulidae` gains 138 species and `Labridae` 569, where today both match
#' nothing. Enable it deliberately, per pipeline, and re-baseline when you do.
#'
#' @keywords taxa
#' @export
#'
#' @examples
#' \dontrun{
#' taxa <- data.frame(
#'   alpha3_code = c("LEN", "SNA"),
#'   scientific_name = c("Lethrinus nebulosus", "Lutjanidae")
#' )
#' expand_taxonomic_info(taxa)
#'
#' # Resolve names that carry a bracketed synonym
#' expand_taxonomic_info(
#'   data.frame(alpha3_code = "GRX", scientific_name = "Haemulidae (=Pomadasyidae)"),
#'   strip_parentheticals = TRUE
#' )
#' }
expand_taxonomic_info <- function(data, strip_parentheticals = FALSE) {
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
      search_name = stringr::str_replace(.data$scientific_name, " spp$", ""),
      search_name = if (isTRUE(strip_parentheticals)) {
        stringr::str_squish(
          stringr::str_remove(.data$search_name, "\\s*\\([^)]*\\)")
        )
      } else {
        .data$search_name
      }
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

#' Length-Weight Coefficients for Expanded Taxa
#'
#' Attaches length-weight relationship coefficients (`a`, `b`) from the
#' FishBase / SeaLifeBase `poplw` table to an expanded taxa table, keyed
#' `alpha3_code` -> species -> coefficients.
#'
#' @param expanded A table as returned by [expand_taxonomic_info()], optionally
#'   already restricted to an FAO area. Must contain `alpha3_code`,
#'   `original_name`, `SpecCode`, `species_found` and `server`.
#'
#' @return A tibble with one row per taxon code and published coefficient
#'   record:
#'   - `alpha3_code`, `original_name`, `SpecCode`, `species_found`, `server`:
#'     carried through from `expanded`.
#'   - `Type`: the length measurement the coefficients are expressed in
#'     (`"TL"`, `"FL"`, `"SL"`, `"WD"`, `"NG"`, `"OT"`, `"PC"`, ...).
#'   - `EsQ`: FishBase's "questionable estimate" flag.
#'   - `a`, `b`: the coefficients of `W = a * L^b`.
#'   - `aTL`: `a` converted to total length, where FishBase provides it.
#'   - `LengthMin`, `LengthMax`: length range the relationship was fitted over.
#'   - `Number`, `Sex`, `Locality`, `C_Code`: study metadata, retained so that
#'     separate published studies stay distinct rows.
#'
#' @details
#' **No filtering on `Type` or `EsQ` is applied.** All measurement types are
#' returned, and it is the caller's job to decide which to keep. This is
#' deliberate: restricting to `Type == "TL"` was measured to discard more than
#' half the matched species for several taxon codes (for Timor-Leste's taxa:
#' `CJX` 10 -> 3 species, `EMP` 25 -> 12, `MOB` 9 -> 4, `YDX` 11 -> 4), which
#' shifts any estimate that aggregates coefficients across a taxon code. Use
#' `EsQ` to drop questionable estimates if wanted, e.g.
#' `dplyr::filter(is.na(EsQ) | tolower(EsQ) != "yes")`.
#'
#' Both servers are queried, so invertebrates and algae — which are absent from
#' FishBase and only present in SeaLifeBase — receive coefficients too.
#'
#' @seealso [get_length_length_coeffs()], [get_taxa_morphometrics()]
#'
#' @keywords taxa
#' @export
#'
#' @examples
#' \dontrun{
#' taxa <- data.frame(
#'   alpha3_code = c("EMP", "OCZ"),
#'   scientific_name = c("Lethrinidae", "Octopus spp")
#' )
#' lw <- taxa |>
#'   expand_taxonomic_info() |>
#'   get_length_weight_coeffs()
#' }
get_length_weight_coeffs <- function(expanded) {
  target_codes <- unique(expanded$SpecCode)

  get_combined_tbl("poplw") |>
    dplyr::filter(.data$SpecCode %in% target_codes) |>
    dplyr::select(
      "SpecCode",
      "server",
      "Type",
      "EsQ",
      "a",
      "b",
      "aTL",
      "LengthMin",
      "LengthMax",
      "Number",
      "Sex",
      "Locality",
      "C_Code"
    ) |>
    dplyr::inner_join(
      expanded |>
        dplyr::select(
          "alpha3_code",
          "original_name",
          "SpecCode",
          "species_found",
          "server"
        ) |>
        dplyr::distinct(),
      by = c("SpecCode", "server"),
      relationship = "many-to-many"
    ) |>
    dplyr::select(
      "alpha3_code",
      "original_name",
      "SpecCode",
      "species_found",
      "server",
      "Type",
      "EsQ",
      "a",
      "b",
      "aTL",
      "LengthMin",
      "LengthMax",
      "Number",
      "Sex",
      "Locality",
      "C_Code"
    ) |>
    dplyr::distinct() |>
    dplyr::as_tibble()
}

#' Length-Length Conversion Coefficients for Expanded Taxa
#'
#' Attaches length-length conversion coefficients from the FishBase /
#' SeaLifeBase `popll` table to an expanded taxa table. These convert one length
#' measurement into another via `Length2 = aL + bL * Length1`, which is required
#' whenever a survey records a different length type from the one the
#' length-weight coefficients are expressed in.
#'
#' @param expanded A table as returned by [expand_taxonomic_info()], optionally
#'   already restricted to an FAO area.
#' @param length_types Character vector of length measurement codes to keep.
#'   Both `Length1` and `Length2` must be in this set. Defaults to
#'   `c("TL", "FL")` — total and fork length. Pass `NULL` to apply no filter.
#'
#' @return A tibble with:
#'   - `alpha3_code`, `original_name`, `SpecCode`, `species_found`, `server`:
#'     carried through from `expanded`.
#'   - `Length1`, `Length2`: the measurement types converted from and to.
#'   - `aL`, `bL`: intercept and slope of the conversion.
#'   - `LengthMin`, `LengthMax`, `Number`, `Sex`, `r2`: study metadata.
#'
#' @details
#' Note the naming: the `popll` table calls the conversion coefficients `a` and
#' `b`, the same names the length-*weight* table uses for entirely different
#' quantities. They are renamed to `aL` / `bL` here so that the two tables can
#' be joined without collision.
#'
#' This is load-bearing wherever a survey's recorded length type differs from
#' the coefficient type. Timor-Leste's v1 survey form records fork length while
#' its coefficients are predominantly total length, so without this conversion
#' every v1 catch record carrying a measured length yields no weight at all.
#'
#' @seealso [get_length_weight_coeffs()], [get_taxa_morphometrics()]
#'
#' @keywords taxa
#' @export
#'
#' @examples
#' \dontrun{
#' taxa <- data.frame(
#'   alpha3_code = "EMP",
#'   scientific_name = "Lethrinidae"
#' )
#' ll <- taxa |>
#'   expand_taxonomic_info() |>
#'   get_length_length_coeffs()
#' }
get_length_length_coeffs <- function(expanded, length_types = c("TL", "FL")) {
  target_codes <- unique(expanded$SpecCode)

  ll <- get_combined_tbl("popll") |>
    dplyr::filter(.data$SpecCode %in% target_codes)

  if (!is.null(length_types)) {
    ll <- ll |>
      dplyr::filter(
        .data$Length1 %in% length_types,
        .data$Length2 %in% length_types
      )
  }

  ll |>
    dplyr::select(
      "SpecCode",
      "server",
      "Length1",
      "Length2",
      aL = "a",
      bL = "b",
      "LengthMin",
      "LengthMax",
      "Number",
      "Sex",
      "r2"
    ) |>
    dplyr::inner_join(
      expanded |>
        dplyr::select(
          "alpha3_code",
          "original_name",
          "SpecCode",
          "species_found",
          "server"
        ) |>
        dplyr::distinct(),
      by = c("SpecCode", "server"),
      relationship = "many-to-many"
    ) |>
    dplyr::select(
      "alpha3_code",
      "original_name",
      "SpecCode",
      "species_found",
      "server",
      "Length1",
      "Length2",
      "aL",
      "bL",
      "LengthMin",
      "LengthMax",
      "Number",
      "Sex",
      "r2"
    ) |>
    dplyr::distinct() |>
    dplyr::as_tibble()
}

#' Build Length-Weight and Length-Length Tables for a Set of Taxa
#'
#' Single entry point for the morphometric half of the taxa pipeline: expands a
#' taxa table against the FishBase / SeaLifeBase backbone, restricts it to the
#' configured FAO major fishing area(s), and returns both the length-weight and
#' the length-length coefficient tables from that one expansion.
#'
#' @param data A data frame with `alpha3_code` and `scientific_name` columns,
#'   as accepted by [expand_taxonomic_info()].
#' @param fao_areas Numeric vector of FAO major fishing area codes to restrict
#'   species to. If `NULL` (default) the value is resolved from `conf` via
#'   [resolve_fao_areas()], which falls back to `c(51, 57)`. Pass
#'   `filter_by_area = FALSE` to apply no restriction at all.
#' @param filter_by_area Logical. If `FALSE`, species are **not** restricted by
#'   FAO area and `fao_areas` is ignored. Default `TRUE`. See Details for the
#'   coverage trade-off, which is substantial.
#' @param length_types Character vector of length measurement codes for the
#'   length-length table. See [get_length_length_coeffs()].
#' @param strip_parentheticals Logical, passed to [expand_taxonomic_info()].
#'   Default `FALSE`.
#' @param conf Optional configuration list. If `NULL` (default) and the area is
#'   being resolved from configuration, [read_config()] is called.
#'
#' @return A named list of three tibbles:
#'   - `expanded`: the species expansion actually used.
#'   - `length_weight`: see [get_length_weight_coeffs()].
#'   - `length_length`: see [get_length_length_coeffs()].
#'
#' @details
#' Because both coefficient tables are derived from a single expansion, the two
#' are guaranteed to be keyed consistently and can be joined on `species_found`
#' and `server` without re-resolving names.
#'
#' No `Type` or `EsQ` filtering is applied to `length_weight` — see
#' [get_length_weight_coeffs()] for why.
#'
#' ## Whether to filter by FAO area
#'
#' Restricting to an area is right for distributional traits, and it is what
#' [enrich_taxa()] does. For length-weight coefficients it is a coverage
#' decision rather than a correctness one: `a` and `b` describe body form, which
#' does not stop applying at an FAO boundary, and FishBase's area assignments are
#' incomplete. Measured over Timor-Leste's 57 taxa:
#'
#' | | area 71 | unrestricted |
#' |---|---|---|
#' | species expanded | 2,469 | 5,150 |
#' | species with `a`/`b` | 764 | 1,283 |
#' | length-weight rows | 2,970 | 5,225 |
#' | taxon codes with length-length | 38 | 39 |
#'
#' Filtering here costs 40% of the species that have usable coefficients. Set
#' `filter_by_area = FALSE` when maximum coefficient coverage matters more than
#' regional specificity, which is generally the case when the coefficients feed
#' a weight estimate aggregated across a whole taxon code.
#'
#' @seealso [expand_taxonomic_info()], [enrich_taxa()]
#'
#' @keywords taxa
#' @export
#'
#' @examples
#' \dontrun{
#' taxa <- data.frame(
#'   alpha3_code = c("EMP", "SNA"),
#'   scientific_name = c("Lethrinidae", "Lutjanus spp")
#' )
#'
#' # Indian Ocean, Western + Eastern (the configured default)
#' m <- get_taxa_morphometrics(taxa)
#'
#' # Western Central Pacific, e.g. Timor-Leste
#' m <- get_taxa_morphometrics(taxa, fao_areas = 71)
#'
#' # Maximum coefficient coverage, no area restriction
#' m <- get_taxa_morphometrics(taxa, filter_by_area = FALSE)
#' }
get_taxa_morphometrics <- function(
  data,
  fao_areas = NULL,
  filter_by_area = TRUE,
  length_types = c("TL", "FL"),
  strip_parentheticals = FALSE,
  conf = NULL
) {
  if (isTRUE(filter_by_area) && is.null(fao_areas)) {
    conf <- conf %||% read_config()
    fao_areas <- resolve_fao_areas(conf, fao_areas)
  }

  logger::log_info(
    "Expanding {nrow(data)} taxa against FishBase and SeaLifeBase"
  )
  expanded <- expand_taxonomic_info(
    data,
    strip_parentheticals = strip_parentheticals
  ) |>
    dplyr::distinct()

  if (isTRUE(filter_by_area)) {
    logger::log_info(
      "Filtering to FAO area(s) {paste(fao_areas, collapse = ', ')}"
    )
    expanded <- filter_by_fao_area(expanded, fao_areas) |>
      dplyr::distinct()
  } else {
    logger::log_info("No FAO area restriction applied")
  }

  logger::log_info("Fetching length-weight coefficients")
  length_weight <- get_length_weight_coeffs(expanded)

  logger::log_info("Fetching length-length conversion coefficients")
  length_length <- get_length_length_coeffs(
    expanded,
    length_types = length_types
  )

  logger::log_info(
    "Built {nrow(length_weight)} length-weight and {nrow(length_length)} \\
     length-length rows for {dplyr::n_distinct(expanded$species_found)} species"
  )

  list(
    expanded = expanded,
    length_weight = length_weight,
    length_length = length_length
  )
}

#' Enrich Taxa with FishBase and SeaLifeBase Biological Data
#'
#' Main pipeline function that downloads the taxa metadata from cloud storage,
#' expands each record to matching FishBase / SeaLifeBase species (restricted to
#' the configured FAO major fishing areas — 51 and 57, the Western and Eastern
#' Indian Ocean, by default), and joins biological attributes including
#' vulnerability, trophic level, feeding guild, and nutrient composition. The
#' final dataset is uploaded as a versioned Parquet file to the shared coasts
#' bucket.
#'
#' @param log_threshold Logging threshold passed to [logger::log_threshold()].
#'   Defaults to [logger::DEBUG].
#' @param fao_areas Optional numeric vector of FAO major fishing area codes to
#'   restrict species to. If `NULL` (default) the value is resolved from the
#'   `metadata.fishbase.fao_areas` configuration key, falling back to
#'   `c(51, 57)` (Western and Eastern Indian Ocean). Timor-Leste is Area 71.
#'   Pass `57` to reproduce pre-4.6.0 output — see [resolve_fao_areas()] for
#'   why 57 alone was never right for the Western Indian Ocean.
#'
#' @return Invisible NULL. Called for its side effect of uploading the enriched
#'   taxa Parquet file to cloud storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Downloads the latest metadata RDS file (Airtable assets) from GCS.
#' 2. Extracts the `taxa` table and calls [expand_taxonomic_info()].
#' 3. Restricts species to the resolved FAO area(s) via [filter_by_fao_area()],
#'    always keeping species with no area assignment.
#' 4. Joins species-level data from the `species`, `ecology`, and `estimate`
#'    tables.
#' 5. Deduplicates by taking the first non-`NA` value per group.
#' 6. Cleans column names with [janitor::clean_names()].
#' 7. Uploads the result via [upload_parquet_to_cloud()] using the
#'    `metadata.fishbase.taxa_enriched.file_prefix` configuration key.
#'
#' Both the assets snapshot it reads and the enriched table it writes resolve
#' through `resolve_storage_opts(conf, "coasts")` — the shared hub bucket, which
#' is where [ingest_pds_trips()] and the other readers of these objects look.
#' Within the `coasts` package itself the hub and country buckets are the same,
#' so this is a no-op; in a downstream package that defines
#' `storage.google.options_coasts` it directs both ends at the hub rather than
#' scattering per-country copies.
#'
#' This function emits traits and nutrients only. For length-weight and
#' length-length coefficients see [get_taxa_morphometrics()].
#'
#' @seealso [get_taxa_morphometrics()], [resolve_fao_areas()],
#'   [resolve_storage_opts()]
#'
#' @keywords workflow
#' @export
#'
#' @examples
#' \dontrun{
#' # Indian Ocean, Western + Eastern (the default)
#' coasts::enrich_taxa()
#'
#' # Western Central Pacific
#' coasts::enrich_taxa(fao_areas = 71)
#' }
enrich_taxa <- function(log_threshold = logger::DEBUG, fao_areas = NULL) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  fao_areas <- resolve_fao_areas(conf, fao_areas)
  coasts_opts <- resolve_storage_opts(conf, "coasts")

  # ── 1. Load taxa from cloud ───────────────────────────────────────────────────
  logger::log_info("Downloading taxa metadata from cloud storage")
  taxa <- cloud_object_name(
    prefix = conf$metadata$airtable$name,
    provider = conf$storage$google$key,
    version = "latest",
    extension = "rds",
    options = coasts_opts
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = coasts_opts
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

  # ── 3. Restrict to the configured FAO major fishing area(s) ──────────────────
  logger::log_info(
    "Filtering to FAO area(s) {paste(fao_areas, collapse = ', ')}"
  )
  expanded_assets_filtered <- filter_by_fao_area(expanded_assets, fao_areas)

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
    options = coasts_opts
  )

  invisible(NULL)
}
