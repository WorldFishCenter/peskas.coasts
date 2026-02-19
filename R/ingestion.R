#' Ingest Fisheries Asset Metadata
#'
#' @description
#' This function handles the automated ingestion of fisheries asset metadata from Airtable.
#' It performs the following operations:
#' 1. Retrieves metadata for taxa, gear types, vessel types, landing sites, and survey forms
#' 2. Removes duplicate records from each asset type
#' 3. Packages all assets into a versioned RDS file
#' 4. Uploads the processed file to configured cloud storage
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#'
#' The function retrieves the following asset types from Airtable:
#' - **Taxa**: Species information including scientific names, alpha3 codes, and English names
#' - **Gear**: Fishing gear types with standardized names
#' - **Vessels**: Vessel types with standardized classifications
#' - **Landing Sites**: Site information with codes and names
#' - **Forms**: Survey form metadata with form IDs and names
#'
#' All assets are deduplicated and stored together in a single RDS file with
#' a versioned filename (includes timestamp and git SHA).
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates a local RDS file containing a list of all asset data frames
#'   - Uploads file to configured cloud storage
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Ingest all fisheries assets from Airtable
#' ingest_assets()
#' }
#'
#' @seealso
#' * [fetch_asset()] for details on how individual assets are retrieved
#' * [add_version()] for details on the file versioning system
#' * [upload_cloud_file()] for details on the cloud upload process
#'
#' @keywords workflow ingestion metadata
#' @export
ingest_assets <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  assets_list <-
    list(
      geo = fetch_asset(
        table_name = "districts",
        select_cols = c(
          "form_id",
          "survey_label",
          "district_code",
          "gaul_1_name",
          "gaul_1_code",
          "gaul_2_name",
          "gaul_2_code",
          "total_boats",
          "country",
          "airtable_id"
        ),
        conf = conf
      ),
      taxa = fetch_asset(
        table_name = "taxa",
        select_cols = c(
          "form_id",
          "survey_label",
          "alpha3_code",
          "scientific_name",
          "english_name"
        ),
        conf = conf
      ),
      gear = fetch_asset(
        table_name = "gears",
        select_cols = c("form_id", "survey_label", "standard_name"),
        conf = conf
      ),
      vessels = fetch_asset(
        table_name = "vessels",
        select_cols = c("form_id", "survey_label", "standard_name"),
        conf = conf
      ),
      sites = fetch_asset(
        table_name = "landing_sites",
        select_cols = c("form_id", "site", "site_code"),
        conf = conf
      ),
      forms = fetch_asset(
        table_name = "forms",
        select_cols = c("form_id", "form_name"),
        conf = conf
      ),
      devices = fetch_asset(
        table_name = "pds_devices",
        select_cols = c(
          "customer_name",
          "imei",
          "boat_name",
          "registration_number",
          "captain",
          "last_seen",
          "gaul_2",
          "region",
          "community"
        ),
        conf = conf
      )
    ) |>
    purrr::map(~ dplyr::distinct(.x))

  # Enrich devices with geo district names
  assets_list$devices <-
    assets_list$devices |>
    dplyr::left_join(
      assets_list$geo |>
        dplyr::select("airtable_id", "gaul_2_name", "gaul_2_code"),
      by = c("gaul_2" = "airtable_id")
    )

  asset_filename <-
    conf$metadata$airtable$name |>
    add_version(extension = "rds")

  readr::write_rds(x = assets_list, file = asset_filename)

  upload_cloud_file(
    file = asset_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
