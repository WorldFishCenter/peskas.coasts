#' Retrieve Data from Kobotoolbox API
#'
#' This function retrieves survey data from Kobotoolbox API for a specific asset.
#' It supports pagination and handles both JSON and XML formats.
#'
#' @param assetid The asset ID of the Kobotoolbox form.
#' @param url The URL of Kobotoolbox (default is "eu.kobotoolbox.org").
#' @param uname Username for Kobotoolbox account.
#' @param pwd Password for Kobotoolbox account.
#' @param encoding Encoding to be used for data retrieval (default is "UTF-8").
#' @param format Format of the data to retrieve, either "json" or "xml" (default is "json").
#' @param limit Number of records per page (default 1000). Maximum allowed is 1000.
#' @param since_id Optional. If provided, only fetch submissions with `_id`
#'   greater than or equal to this value. Useful for incremental data retrieval.
#' @param retry_times Number of retry attempts for failed requests (default is 3).
#' @param progress Logical. Whether to show a progress message (default is TRUE).
#'
#' @return A list containing all retrieved survey results.
#' @keywords ingestion
#' @details
#' As of March 2026, the Kobotoolbox API enforces a maximum page size of 1,000
#' records per request (previously 30,000). The default page size if not specified
#' is 100. This function uses pagination via the `next` field in the API response
#' to iterate through all available records.
#'
#' For incremental data retrieval (e.g., syncing only new submissions), use the
#' `since_id` parameter with the last known `_id` value.
#'
#' Note: This change does NOT affect synchronous export endpoints
#' (`/api/v2/assets/{uid}/export-settings/{uid_export}/data.xlsx|csv`).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Full retrieval
#' kobo_data <- get_kobo_data(
#'   assetid = "your_asset_id",
#'   uname = "your_username",
#'   pwd = "your_password"
#' )
#'
#' # Incremental retrieval (only new records since last sync)
#' new_data <- get_kobo_data(
#'   assetid = "your_asset_id",
#'   uname = "your_username",
#'   pwd = "your_password",
#'   since_id = 52149
#' )
#' }
get_kobo_data <- function(
  assetid,
  url = "eu.kobotoolbox.org",
  uname = NULL,
  pwd = NULL,
  encoding = "UTF-8",
  format = "json",
  limit = 1000,
  since_id = NULL,
  retry_times = 3,
  progress = TRUE
) {
  # --- Input validation ---
  if (is.null(uname) || !is.character(uname) || uname == "") {
    stop("`uname` (username) must be a non-empty string.")
  }
  if (is.null(pwd) || !is.character(pwd) || pwd == "") {
    stop("`pwd` (password) must be a non-empty string.")
  }
  if (is.null(assetid) || !is.character(assetid) || assetid == "") {
    stop("`assetid` must be a non-empty string.")
  }
  if (is.null(url) || !is.character(url) || url == "") {
    stop("`url` must be a non-empty string.")
  }
  if (!format %in% c("json", "xml")) {
    stop("`format` must be either 'json' or 'xml'.")
  }
  if (!is.numeric(limit) || limit < 1 || limit > 1000) {
    stop("`limit` must be a number between 1 and 1000.")
  }
  limit <- as.integer(limit)

  base_url <- paste0(
    "https://",
    url,
    "/api/v2/assets/",
    assetid,
    "/data.",
    format
  )

  if (progress) {
    message("Starting data retrieval from ", base_url)
  }

  # --- Page fetcher ---
  get_page <- function(page_url) {
    response <- tryCatch(
      expr = {
        httr2::request(page_url) |>
          httr2::req_auth_basic(uname, pwd) |>
          httr2::req_retry(max_tries = retry_times) |>
          httr2::req_error(is_error = \(resp) FALSE) |>
          httr2::req_perform()
      },
      error = function(e) {
        warning("Request failed: ", conditionMessage(e))
        return(NULL)
      }
    )

    if (is.null(response)) {
      return(NULL)
    }

    status <- httr2::resp_status(response)
    if (status >= 400) {
      warning(
        "HTTP error ",
        status,
        " when fetching: ",
        page_url,
        "\nBody: ",
        tryCatch(
          httr2::resp_body_string(response),
          error = function(e) "(unable to read body)"
        )
      )
      return(NULL)
    }

    content_type <- httr2::resp_content_type(response)

    if (grepl("json", content_type)) {
      return(httr2::resp_body_json(response, encoding = encoding))
    } else if (grepl("xml", content_type)) {
      return(httr2::resp_body_string(response, encoding = encoding))
    } else {
      warning("Unexpected content type: ", content_type)
      return(NULL)
    }
  }

  # --- Build initial URL with query params ---
  initial_url <- paste0(base_url, "?limit=", limit, "&start=0")

  if (!is.null(since_id)) {
    query_json <- paste0('{"_id":{"$gte":', since_id, '}}')
    initial_url <- paste0(
      initial_url,
      "&query=",
      utils::URLencode(query_json, reserved = TRUE)
    )
  }

  # --- Pagination loop using `next` field ---
  all_results <- list()
  current_url <- initial_url
  page_num <- 1L

  repeat {
    if (progress) {
      message("Fetching page ", page_num, "...")
    }

    page_data <- get_page(current_url)

    if (is.null(page_data)) {
      warning("Failed to retrieve page ", page_num, ". Stopping.")
      break
    }

    new_results <- page_data$results
    if (is.null(new_results) || length(new_results) == 0) {
      if (progress) {
        message("No results on page ", page_num, ". Done.")
      }
      break
    }

    all_results <- c(all_results, new_results)

    if (progress) {
      message(
        "Page ",
        page_num,
        ": retrieved ",
        length(new_results),
        " records (total: ",
        length(all_results),
        " / ",
        if (!is.null(page_data$count)) page_data$count else "unknown",
        ")"
      )
    }

    # Use the `next` URL provided by the API for pagination
    next_url <- page_data$`next`
    if (is.null(next_url) || identical(next_url, "")) {
      if (progress) {
        message("No more pages. Retrieval complete.")
      }
      break
    }

    current_url <- next_url
    page_num <- page_num + 1L
  }

  if (progress) {
    message("Data retrieval complete. Total records: ", length(all_results))
  }

  # --- Check for duplicate submission IDs ---
  if (length(all_results) > 0) {
    submission_ids <- vapply(
      all_results,
      function(x) if (!is.null(x$`_id`)) x$`_id` else NA_integer_,
      integer(1)
    )
    n_unique <- length(unique(submission_ids[!is.na(submission_ids)]))
    if (n_unique != length(all_results)) {
      warning(
        "Found ",
        length(all_results) - n_unique,
        " duplicate submission IDs out of ",
        length(all_results),
        " records."
      )
    }
  }

  all_results
}


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
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @keywords workflow ingestion metadata
#' @export
ingest_assets <- function(log_threshold = logger::DEBUG, package = "coasts") {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

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
        select_cols = c("form_id", "site", "site_code", "gaul_2_code"),
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
          "country_unlink",
          "imei",
          "boat_name",
          "registration_number",
          "captain",
          "last_seen",
          "gaul_2",
          "region",
          "community",
          "gear_class",
          "vessel_class"
        ),
        conf = conf
      ),
      frame = fetch_asset(
        table_name = "frame",
        select_cols = c(
          "gaul_1_name",
          "gaul_1_code",
          "gaul_2_name",
          "gaul_2_code",
          "gear_or_boat_type",
          "category_kind",
          "standard_name",
          "standard_code",
          "n_boats",
          "fishers_male",
          "fishers_female"
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
