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
          "country"
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
          "region",
          "community"
        ),
        conf = conf
      )
    ) |>
    purrr::map(~ dplyr::distinct(.x))

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

#' Ingest Pelagic Data Systems (PDS) Trip Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS).
#' It performs the following operations:
#' 1. Retrieves device metadata from configured cloud storage
#' 2. Filters devices by last seen date (>= 2023-01-01)
#' 3. Downloads all trip data from PDS API (2023-01-01 to present)
#' 4. Filters trips to match active device IMEIs
#' 5. Converts the data to parquet format
#' 6. Uploads the processed file to configured cloud storage
#'
#' @details
#' The function requires specific configuration in the `conf.yml` file with the following structure:
#'
#' ```yaml
#' pds:
#'   token: "your_pds_token"               # PDS API token
#'   secret: "your_pds_secret"             # PDS API secret
#'   pds_trips:
#'     file_prefix: "pds_trips"            # Prefix for output files
#' metadata:
#'   airtable:
#'     name: "metadata_file_prefix"        # Prefix for metadata file in cloud storage
#' storage:
#'   google:                               # Storage provider configuration
#'     key: "google"                       # Storage provider identifier
#'     options:
#'       project: "project-id"             # Cloud project ID
#'       bucket: "bucket-name"             # Storage bucket name
#'       service_account_key: "path/to/key.json"
#' ```
#'
#' The function processes trips as follows:
#' - Downloads device metadata RDS file from cloud storage
#' - Converts device `last_seen` timestamps from Unix milliseconds to Date format
#' - Filters to devices active since 2023-01-01
#' - Retrieves all trips from PDS API (with `deviceInfo` and `withLastSeen` options enabled)
#' - Filters trips to match active device IMEIs
#' - Saves as compressed parquet file (LZ4 compression, level 12)
#' - Uploads to configured cloud storage
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates a versioned parquet file locally with filtered trip data
#'   - Uploads file to configured cloud storage
#'   - Generates logs of the process
#'
#' @note
#' The PDS API does not support IMEI filtering in the request, so all trips are
#' retrieved and filtered locally. This ensures reliable data retrieval but may
#' result in downloading more data than needed.
#'
#' @examples
#' \dontrun{
#' # Run with default debug logging
#' ingest_pds_trips()
#'
#' # Run with info-level logging only
#' ingest_pds_trips(logger::INFO)
#' }
#'
#' @seealso
#' * [get_trips()] for details on the PDS trip data retrieval process
#' * [cloud_object_name()] for generating cloud storage object names
#' * [download_cloud_file()] for downloading files from cloud storage
#' * [upload_cloud_file()] for uploading files to cloud storage
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  assets <- conf$metadata$airtable$name |>
    cloud_object_name(
      provider = conf$storage$google$key,
      extension = "rds",
      options = conf$storage$google$options
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds()

  assets$devices <- assets$devices |>
    dplyr::mutate(
      last_seen = as.Date(as.POSIXct(
        .data$last_seen / 1000,
        origin = "1970-01-01"
      ))
    ) |>
    dplyr::filter(.data$last_seen >= "2023-01-01")

  boats_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = "2023-01-01",
    dateTo = Sys.Date(),
    #imeis = unique(assets$devices$imei),
    deviceInfo = TRUE,
    withLastSeen = TRUE
  ) |>
    dplyr::filter(.data$IMEI %in% as.numeric(unique(assets$devices$imei)))

  filename <- conf$pds$pds_trips$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = boats_trips,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {filename} to cloud storage")
  upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Ingest Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat track data from Pelagic Data Systems (PDS).
#' It downloads and stores only new tracks that haven't been previously uploaded to Google Cloud Storage.
#' Uses parallel processing for improved performance.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param batch_size Optional number of tracks to process. If NULL, processes all new tracks.
#'
#' @return None (invisible). The function performs its operations for side effects.
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  batch_size = NULL
) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # Get trips file from cloud storage
  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    extension = "parquet",
    version = conf$pds$pds_trips$version,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading {pds_trips_parquet}")
  download_cloud_file(
    name = pds_trips_parquet,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  # Read trip IDs
  logger::log_info("Reading trip IDs...")
  trips_data <- arrow::read_parquet(file = pds_trips_parquet) %>%
    dplyr::pull("Trip") %>%
    unique()

  # Clean up downloaded file
  unlink(pds_trips_parquet)

  # List existing files in GCS bucket
  logger::log_info("Checking existing tracks in cloud storage...")
  existing_tracks <-
    googleCloudStorageR::gcs_list_objects(
      bucket = conf$pds_storage$google$options$bucket,
      prefix = conf$pds$pds_tracks$file_prefix
    )$name

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(existing_tracks)
  new_trip_ids <- setdiff(trips_data, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  # Select tracks to process
  process_ids <- if (!is.null(batch_size)) {
    new_trip_ids[1:batch_size]
  } else {
    new_trip_ids
  }
  logger::log_info("Processing {length(process_ids)} new tracks in parallel...")

  # Process tracks in parallel with progress bar
  results <- furrr::future_map(
    process_ids,
    function(trip_id) {
      tryCatch(
        {
          # Create filename for this track
          track_filename <- sprintf(
            "%s_%s.parquet",
            conf$pds$pds_tracks$file_prefix,
            trip_id
          )

          # Get track data
          track_data <- get_trip_points(
            token = conf$pds$token,
            secret = conf$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )

          # Save to parquet
          arrow::write_parquet(
            x = track_data,
            sink = track_filename,
            compression = "lz4",
            compression_level = 12
          )

          # Upload to cloud
          logger::log_info("Uploading track for trip {trip_id}")
          upload_cloud_file(
            file = track_filename,
            provider = conf$pds_storage$google$key,
            options = conf$pds_storage$google$options
          )

          # Clean up local file
          unlink(track_filename)

          list(
            status = "success",
            trip_id = trip_id,
            message = "Successfully processed"
          )
        },
        error = function(e) {
          list(
            status = "error",
            trip_id = trip_id,
            message = e$message
          )
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  # Clean up parallel processing
  future::plan(future::sequential)

  # Summarize results
  successes <- sum(purrr::map_chr(results, "status") == "success")
  failures <- sum(purrr::map_chr(results, "status") == "error")

  logger::log_info(
    "Processing complete. Successfully processed {successes} tracks."
  )
  if (failures > 0) {
    logger::log_warn("Failed to process {failures} tracks.")
    failed_results <- results[purrr::map_chr(results, "status") == "error"]
    failed_trips <- purrr::map_chr(failed_results, "trip_id")
    failed_messages <- purrr::map_chr(failed_results, "message")

    logger::log_warn("Failed trip IDs and reasons:")
    purrr::walk2(
      failed_trips,
      failed_messages,
      ~ logger::log_warn("Trip {.x}: {.y}")
    )
  }
}

#' Extract Trip IDs from Track Filenames
#'
#' @param filenames Character vector of track filenames
#' @return Character vector of trip IDs
#' @keywords internal
extract_trip_ids_from_filenames <- function(filenames) {
  if (length(filenames) == 0) {
    return(character(0))
  }
  # Assuming filenames are in format: pds-tracks_TRIPID.parquet
  gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
}

#' Process Single PDS Track
#'
#' @param trip_id Character. The ID of the trip to process.
#' @param conf List. Configuration parameters.
#' @return List with processing status and details.
#' @keywords internal
process_single_track <- function(trip_id, conf) {
  tryCatch(
    {
      # Create filename for this track
      track_filename <- sprintf(
        "%s_%s.parquet",
        conf$pds$pds_tracks$file_prefix,
        trip_id
      )

      # Get track data
      track_data <- get_trip_points(
        token = conf$pds$token,
        secret = conf$pds$secret,
        id = trip_id,
        deviceInfo = TRUE
      )

      # Save to parquet
      arrow::write_parquet(
        x = track_data,
        sink = track_filename,
        compression = "lz4",
        compression_level = 12
      )

      # Upload to cloud
      logger::log_info("Uploading track for trip {trip_id}")
      upload_cloud_file(
        file = track_filename,
        provider = conf$pds_storage$google$key,
        options = conf$pds_storage$google$options
      )

      # Clean up local file
      unlink(track_filename)

      list(
        status = "success",
        trip_id = trip_id,
        message = "Successfully processed"
      )
    },
    error = function(e) {
      list(
        status = "error",
        trip_id = trip_id,
        message = e$message
      )
    }
  )
}


#' Authenticate with Pelagic Analytics API
#'
#' @description
#' Authenticates with the Pelagic Analytics API using username and password
#' to obtain an access token for subsequent API calls.
#'
#' @param username Character. Your Pelagic Analytics username
#' @param password Character. Your Pelagic Analytics password
#' @param base_url Character. Base URL for the API. Default is "https://analytics.pelagicdata.com"
#'
#' @return List containing the access token and refresh token
#'
#' @examples
#' \dontrun{
#' auth_response <- pelagic_auth("your_username", "your_password")
#' token <- auth_response$token
#' }
#'
#' @export
pelagic_auth <- function(
  username,
  password,
  base_url = "https://analytics.pelagicdata.com"
) {
  # Prepare the request body
  auth_body <- list(
    username = username,
    password = password
  )

  # Make the authentication request
  response <- httr::POST(
    url = paste0(base_url, "/api/auth/login"),
    body = auth_body,
    encode = "json",
    httr::add_headers("Content-Type" = "application/json")
  )

  # Check if request was successful
  if (httr::status_code(response) != 200) {
    stop("Authentication failed. Status code: ", httr::status_code(response))
  }

  # Parse the response
  auth_data <- httr::content(response, as = "parsed")

  logger::log_info("Successfully authenticated with Pelagic Analytics API")

  return(auth_data)
}

#' Refresh Authentication Token
#'
#' @description
#' Refreshes an expired access token using the refresh token.
#'
#' @param refresh_token Character. Refresh token obtained from pelagic_auth()
#' @param base_url Character. Base URL for the API. Default is "https://analytics.pelagicdata.com"
#'
#' @return List containing the new access token and refresh token
#'
#' @export
pelagic_refresh_token <- function(
  refresh_token,
  base_url = "https://analytics.pelagicdata.com"
) {
  # This is a placeholder - you'd need to check Pelagic's API docs for the actual refresh endpoint
  # Most APIs have something like POST /api/auth/refresh

  logger::log_info("Refreshing authentication token...")

  response <- httr::POST(
    url = paste0(base_url, "/api/auth/refresh"),
    body = list(refreshToken = refresh_token),
    encode = "json",
    httr::add_headers("Content-Type" = "application/json")
  )

  if (httr::status_code(response) != 200) {
    stop("Token refresh failed. Status code: ", httr::status_code(response))
  }

  auth_data <- httr::content(response, as = "parsed")
  logger::log_info("Successfully refreshed authentication token")

  return(auth_data)
}

#' Get Boats from Pelagic Analytics API (with server-side filtering)
#'
#' @description
#' Retrieves boat information from the Pelagic Analytics API for a specified customer.
#' Supports server-side filtering to reduce data transfer and processing time.
#'
#' @param token Character. Access token obtained from pelagic_auth()
#' @param customers Character vector. Customer IDs to filter by. If NULL, uses default customer.
#' @param boats Character/Numeric vector. Boat IDs to filter by. If NULL, returns all boats.
#' @param imeis Character/Numeric vector. IMEI numbers to filter by. If NULL, returns all devices.
#' @param columns Character vector. Specific columns to extract. If NULL, returns all columns.
#' @param refresh_token Character. Optional refresh token for automatic token refresh
#' @param username Character. Optional username for re-authentication if refresh fails
#' @param password Character. Optional password for re-authentication if refresh fails
#' @param customer_id Character. Default customer ID if customers parameter is NULL
#' @param base_url Character. Base URL for the API. Default is "https://analytics.pelagicdata.com"
#'
#' @return Data frame containing boat information
#'
#' @examples
#' \dontrun{
#' # Get all boats (default)
#' boats_all <- get_pelagic_boats(auth_response$token)
#'
#' # Get boats with specific IMEIs (server-side filtering)
#' boats_filtered <- get_pelagic_boats(
#'   token = auth_response$token,
#'   imeis = c("864352046XXXX", "1234567890XXXX")
#' )
#'
#' # Get boats for multiple customers
#' boats_multi_customer <- get_pelagic_boats(
#'   token = auth_response$token,
#'   customers = c("customer1-id", "customer2-id")
#' )
#'
#' # Combine server-side filtering with column selection
#' boats_minimal <- get_pelagic_boats(
#'   token = auth_response$token,
#'   imeis = "864352046XXXXX",
#'   columns = c("id", "name", "devices.imei")
#' )
#' }
#'
#' @export
get_pelagic_boats <- function(
  token,
  customers = NULL,
  boats = NULL,
  imeis = NULL,
  columns = NULL,
  refresh_token = NULL,
  username = NULL,
  password = NULL,
  customer_id = NULL,
  base_url = "https://analytics.pelagicdata.com"
) {
  # Helper function to make the actual API request
  make_boats_request <- function(access_token) {
    # Build request body with server-side filters
    request_body <- list(
      customers = if (is.null(customers)) {
        list(list(entityType = "CUSTOMER", id = customer_id))
      } else {
        lapply(customers, function(cust_id) {
          list(entityType = "CUSTOMER", id = cust_id)
        })
      },
      boats = if (is.null(boats)) list() else as.list(boats),
      imeis = if (is.null(imeis)) list() else as.list(as.numeric(imeis))
    )

    logger::log_info(
      "Making API request with filters: customers={length(request_body$customers)}, boats={length(request_body$boats)}, imeis={length(request_body$imeis)}"
    )

    httr::POST(
      url = paste0(base_url, "/api/pds/boats"),
      body = request_body,
      encode = "json",
      httr::add_headers(
        "Content-Type" = "application/json",
        "X-Authorization" = paste("Bearer", access_token)
      )
    )
  }

  # Try the initial request
  response <- make_boats_request(token)

  # If we get a 401 (unauthorized), try to refresh the token
  if (httr::status_code(response) == 401 && !is.null(refresh_token)) {
    logger::log_info("Token expired, attempting to refresh...")

    tryCatch(
      {
        # Try to refresh the token
        new_auth <- pelagic_refresh_token(refresh_token, base_url)
        response <- make_boats_request(new_auth$token)
      },
      error = function(e) {
        # If refresh fails and we have username/password, re-authenticate
        if (!is.null(username) && !is.null(password)) {
          logger::log_info("Token refresh failed, re-authenticating...")
          new_auth <- pelagic_auth(username, password, base_url)
          response <<- make_boats_request(new_auth$token)
        } else {
          stop("Token expired and refresh failed. Please re-authenticate.")
        }
      }
    )
  }

  # Check if final request was successful
  if (httr::status_code(response) != 200) {
    stop("Failed to retrieve boats. Status code: ", httr::status_code(response))
  }

  # Parse the response
  boats_data <- httr::content(response, as = "parsed")

  logger::log_info(
    "Successfully retrieved boats data from Pelagic Analytics API"
  )

  # Convert to data frame if the response is a list
  if (is.list(boats_data) && length(boats_data) > 0) {
    # Use a recursive function to flatten nested lists safely
    flatten_safely <- function(x, prefix = "") {
      result <- list()
      for (name in names(x)) {
        col_name <- if (prefix == "") name else paste(prefix, name, sep = ".")
        if (is.list(x[[name]]) && !is.null(names(x[[name]]))) {
          # Recursively flatten named lists
          result <- c(result, flatten_safely(x[[name]], col_name))
        } else {
          # Keep simple values as-is
          result[[col_name]] <- x[[name]]
        }
      }
      return(result)
    }

    # Convert to data frame if the response is a list
    if (is.list(boats_data) && length(boats_data) > 0) {
      # Use the same recursive function to flatten nested lists safely
      flatten_safely <- function(x, prefix = "") {
        result <- list()
        for (name in names(x)) {
          col_name <- if (prefix == "") name else paste(prefix, name, sep = ".")
          if (is.list(x[[name]]) && !is.null(names(x[[name]]))) {
            # Recursively flatten named lists
            result <- c(result, flatten_safely(x[[name]], col_name))
          } else {
            # Keep simple values as-is
            result[[col_name]] <- x[[name]]
          }
        }
        return(result)
      }

      boats_df <- purrr::map_dfr(
        boats_data,
        ~ {
          flattened <- flatten_safely(.)
          df <- dplyr::as_tibble(flattened, .name_repair = "unique")

          # Select only specified columns if provided
          if (!is.null(columns)) {
            # Find columns that match the requested names (including partial matches)
            available_cols <- names(df)
            selected_cols <- c()

            for (col in columns) {
              # Exact match first
              if (col %in% available_cols) {
                selected_cols <- c(selected_cols, col)
              } else {
                # Partial match (e.g., "devices" matches "devices.id", "devices.name", etc.)
                matches <- available_cols[grepl(
                  paste0("^", col, "(\\..*)?$"),
                  available_cols
                )]
                selected_cols <- c(selected_cols, matches)
              }
            }

            # Remove duplicates and select columns
            selected_cols <- unique(selected_cols)
            if (length(selected_cols) > 0) {
              df <- df[, selected_cols, drop = FALSE]
            } else {
              warning(
                "None of the requested columns were found. Available columns: ",
                paste(available_cols, collapse = ", ")
              )
            }
          }

          return(df)
        },
        .id = "boat_index"
      )

      return(boats_df)
    }

    return(boats_data)
  }
}

#' Get Devices from Pelagic Analytics API (with server-side filtering)
#'
#' @description
#' Retrieves device information from the Pelagic Analytics API for a specified customer.
#' Supports server-side filtering to reduce data transfer and processing time.
#'
#' @param token Character. Access token obtained from pelagic_auth()
#' @param customers Character vector. Customer IDs to filter by. If NULL, uses default customer.
#' @param boats Character/Numeric vector. Boat IDs to filter by. If NULL, returns all boats.
#' @param imeis Character/Numeric vector. IMEI numbers to filter by. If NULL, returns all devices.
#' @param columns Character vector. Specific columns to extract. If NULL, returns all columns.
#' @param customer_id Character. Default customer ID if customers parameter is NULL
#' @param base_url Character. Base URL for the API. Default is "https://analytics.pelagicdata.com"
#'
#' @return Data frame containing device information
#'
#' @examples
#' \dontrun{
#' # Get all devices
#' devices_all <- get_pelagic_devices(auth_response$token)
#'
#' # Get device with specific IMEI (server-side filtering - much faster!)
#' device_specific <- get_pelagic_devices(
#'   token = auth_response$token,
#'   imeis = "86435204XXXXX"
#' )
#'
#' # Get multiple devices by IMEI
#' devices_multiple <- get_pelagic_devices(
#'   token = auth_response$token,
#'   imeis = c("864352046XXXXX", "123456789XXXXX")
#' )
#'
#' # Combine with column selection
#' devices_minimal <- get_pelagic_devices(
#'   token = auth_response$token,
#'   imeis = "86435204XXXXX",
#'   columns = c("id", "name", "imei", "boat.name", "lat", "lng")
#' )
#' }
#'
#' @export
get_pelagic_devices <- function(
  token,
  customers = NULL,
  boats = NULL,
  imeis = NULL,
  columns = NULL,
  customer_id = NULL,
  base_url = "https://analytics.pelagicdata.com"
) {
  # Prepare the request body with server-side filters
  request_body <- list(
    customers = if (is.null(customers)) {
      list(list(entityType = "CUSTOMER", id = customer_id))
    } else {
      lapply(customers, function(cust_id) {
        list(entityType = "CUSTOMER", id = cust_id)
      })
    },
    boats = if (is.null(boats)) list() else as.list(boats),
    imeis = if (is.null(imeis)) list() else as.list(as.numeric(imeis))
  )

  logger::log_info(
    "Making API request with filters: customers={length(request_body$customers)}, boats={length(request_body$boats)}, imeis={length(request_body$imeis)}"
  )

  # Make the API request
  response <- httr::POST(
    url = paste0(base_url, "/api/pds/devices"),
    body = request_body,
    encode = "json",
    httr::add_headers(
      "Content-Type" = "application/json",
      "X-Authorization" = paste("Bearer", token)
    )
  )

  # Check if request was successful
  if (httr::status_code(response) != 200) {
    stop(
      "Failed to retrieve devices. Status code: ",
      httr::status_code(response)
    )
  }

  # Parse the response
  devices_data <- httr::content(response, as = "parsed")

  logger::log_info(
    "Successfully retrieved devices data from Pelagic Analytics API"
  )

  # Convert to data frame if the response is a list
  if (is.list(devices_data) && length(devices_data) > 0) {
    # Use the same recursive function to flatten nested lists safely
    flatten_safely <- function(x, prefix = "") {
      result <- list()
      for (name in names(x)) {
        col_name <- if (prefix == "") name else paste(prefix, name, sep = ".")
        if (is.list(x[[name]]) && !is.null(names(x[[name]]))) {
          # Recursively flatten named lists
          result <- c(result, flatten_safely(x[[name]], col_name))
        } else {
          # Keep simple values as-is
          result[[col_name]] <- x[[name]]
        }
      }
      return(result)
    }

    devices_df <- purrr::map_dfr(
      devices_data,
      ~ {
        flattened <- flatten_safely(.)
        df <- dplyr::as_tibble(flattened, .name_repair = "unique")

        # Select only specified columns if provided
        if (!is.null(columns)) {
          # Find columns that match the requested names (including partial matches)
          available_cols <- names(df)
          selected_cols <- c()

          for (col in columns) {
            # Exact match first
            if (col %in% available_cols) {
              selected_cols <- c(selected_cols, col)
            } else {
              # Partial match (e.g., "boat" matches "boat.id", "boat.name", etc.)
              matches <- available_cols[grepl(
                paste0("^", col, "(\\..*)?$"),
                available_cols
              )]
              selected_cols <- c(selected_cols, matches)
            }
          }

          # Remove duplicates and select columns
          selected_cols <- unique(selected_cols)
          if (length(selected_cols) > 0) {
            df <- df[, selected_cols, drop = FALSE]
          } else {
            warning(
              "None of the requested columns were found. Available columns: ",
              paste(available_cols, collapse = ", ")
            )
          }
        }

        return(df)
      },
      .id = "device_index"
    )

    return(devices_df)
  }

  return(devices_data)
}


#' Ingest Pelagic Boats Data and Sync to Airtable
#'
#' Retrieves boat/device data from Pelagic Data Systems API, processes and cleans
#' the data, maps country information from Airtable, and syncs the results back
#' to Airtable. This function handles the complete workflow for maintaining
#' up-to-date device information.
#'
#' @param conf List. Configuration parameters (defaults to read_config()).
#'   Must contain pds (username, password, customer_id) and airtable
#'   (token, frame base_id) configuration.
#'
#' @return List containing the authentication response and sync results.
#'
#' @details
#' The function performs the following steps:
#' 1. Authenticates with Pelagic Data Systems API
#' 2. Retrieves boat data with device and vessel characteristics
#' 3. Cleans and standardizes the data format
#' 4. Maps customer names to standardized country names
#' 5. Retrieves country IDs from Airtable Countries table
#' 6. Syncs the processed data to the pds_devices Airtable table
#'
#' Country mapping includes: Egypt, Timor-Leste, Mozambique, Zanzibar, Kenya,
#' Malaysia, Malawi, Tanzania, Bangladesh, and India.
#'
#' @examples
#' \dontrun{
#' # Using default configuration
#' result <- ingest_pelagic_boats()
#'
#' # Using custom configuration
#' custom_config <- read_config("custom_conf.yml")
#' result <- ingest_pelagic_boats(custom_config)
#' }
#'
#' @export
ingest_pelagic_boats <- function(conf = NULL) {
  if (is.null(conf)) {
    conf <- read_config()
  }

  logger::log_info("Starting Pelagic boats data ingestion")

  # Authenticate with PDS
  auth_response <- pelagic_auth(
    username = conf$pds$username,
    password = conf$pds$password
  )

  logger::log_info("Retrieved PDS authentication token")

  # Get boats data from PDS API
  boats <- get_pelagic_boats(
    token = auth_response$token,
    customers = conf$pds$customer_id,
    columns = c(
      "device.imei",
      "active",
      "operationalStatus",
      "device.boatName",
      "device.externalId",
      "device.externalBoatId",
      "registrationNumber",
      # Location/Organization
      "device.directCustomer.name",
      "region",
      "community",
      "device.timezone",
      # Vessel characteristics
      "vesselType",
      "vesselSize",
      "gearType",
      "motorType",
      "description",
      # Operations
      "captain",
      "ownerName",
      "deviceOwner",
      # Device status
      "device.lastSeen",
      "device.batteryState",
      "device.healthCategory"
    )
  ) |>
    janitor::clean_names() |>
    dplyr::filter(!is.na(.data$device_imei)) |>
    dplyr::select(
      # Device identification
      imei = "device_imei",
      "active",
      last_seen = "device_last_seen",
      boat_name = "device_boat_name",
      external_id = "device_external_id",
      external_boat_id = "device_external_boat_id",
      "registration_number",
      # Location/Organization
      customer_name = "device_direct_customer_name",
      "region",
      "community",
      "device_timezone",
      # Vessel characteristics
      "vessel_type",
      "vessel_size",
      "gear_type",
      "motor_type",
      "description",
      # Operations/Ownership
      "captain",
      "owner_name",
      "device_owner",
      # Device status
      battery_state = "device_battery_state",
      health_category = "device_health_category"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      dplyr::across(dplyr::everything(), ~ dplyr::if_else(.x == "", NA, .x))
    ) |>
    dplyr::mutate(
      country = dplyr::case_when(
        .data$customer_name == "WorldFish - Egypt" ~ "egypt",
        .data$customer_name == "MAF / WorldFish" ~ "Timor-Leste",
        .data$customer_name == "WorldFish - Mozambique" ~ "mozambique",
        .data$customer_name == "WorldFish - Zanzibar" ~ "zanzibar",
        .data$customer_name == "Traders" ~ "Timor-Leste",
        .data$customer_name == "WorldFish - Kenya" ~ "kenya",
        .data$customer_name == "WorldFish - Malaysia" ~ "malaysia",
        .data$customer_name == "WorldFish - Malawi" ~ "malawi",
        .data$customer_name == "WorldFish - Tanzania AP" ~ "tanzania",
        .data$customer_name == "Kenya" ~ "kenya",
        .data$customer_name == "WorldFish - Bangladesh" ~ "bangladesh",
        .data$customer_name == "FSSP2: Traders" ~ "Timor-Leste",
        .data$customer_name == "Kenya AABS" ~ "kenya",
        .data$customer_name == "WorldFish - India" ~ "india",
        .data$customer_name == "Syberintel - distributor" ~ "mozambique",
        TRUE ~ NA
      ),
      country = stringr::str_to_title(.data$country)
    ) |>
    dplyr::relocate(country = "country", .after = "customer_name")

  logger::log_info(
    "Retrieved and processed {nrow(boats)} boat records from PDS"
  )

  # Get countries data from Airtable
  countries_df <- airtable_to_df(
    base_id = conf$airtable$frame$base_id,
    table_name = "Countries",
    token = conf$airtable$token
  )

  # Join boats with country IDs
  boats_with_country_ids <- boats %>%
    dplyr::left_join(
      countries_df %>%
        dplyr::select(
          country_name = "Country",
          country_id = "airtable_id"
        ),
      by = c("country" = "country_name")
    ) %>%
    dplyr::mutate(
      country = purrr::map(
        .data$country_id,
        ~ if (is.na(.x)) character(0) else .x
      )
    ) %>%
    dplyr::select(-"country_id") |>
    dplyr::mutate(
      country = purrr::map(
        .data$country,
        ~ if (length(.x) > 0) list(.x) else list()
      )
    )

  logger::log_info(
    "Mapped countries for {nrow(boats_with_country_ids)} records"
  )

  # Sync to Airtable
  sync_result <- device_sync(
    boats_df = boats_with_country_ids,
    base_id = conf$airtable$frame$base_id,
    table_name = "pds_devices",
    token = conf$airtable$token
  )

  logger::log_info("Successfully completed Pelagic boats ingestion and sync")

  return(list(
    auth_response = auth_response,
    boats_count = nrow(boats_with_country_ids),
    sync_result = sync_result
  ))
}

#' Backup Pelagic Tracks (Fallback)
#'
#' @description
#' Fallback pipeline to refresh the bound tracks parquet when the main ingestion is unavailable. It:
#' 1. Loads Airtable assets, keeping East Africa devices (Zanzibar, Kenya, Mozambique, Malawi, Tanzania) active since 2024-01-01
#' 2. Pulls the last 90 days of PDS trips and filters to those IMEIs
#' 3. Reads the latest bound tracks parquet from cloud, finds new trip IDs, and downloads missing tracks in parallel
#' 4. Aggregates points to 10-minute medians, binds with existing tracks, deduplicates, and uploads a versioned parquet back to cloud storage
#'
#' @return None (invisible). Updates the cloud parquet with any new track data.
#'
#' @examples
#' \dontrun{
#' backup_tracks()
#' }
#'
#' @keywords workflow ingestion
#' @export
backup_tracks <- function() {
  conf <- read_config()

  assets <- conf$metadata$airtable$name |>
    cloud_object_name(
      provider = conf$storage$google$key,
      extension = "rds",
      options = conf$storage$google$options
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds()

  assets$devices <-
    assets$devices |>
    dplyr::filter(stringr::str_detect(
      .data$customer_name,
      "Zanzibar|Kenya|Mozambique|Malawi|Tanzania"
    )) |>
    dplyr::mutate(
      last_seen = as.Date(as.POSIXct(
        .data$last_seen / 1000,
        origin = "1970-01-01"
      ))
    ) |>
    dplyr::filter(.data$last_seen >= "2024-01-01")

  boats_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = Sys.Date() - 90,
    dateTo = Sys.Date(),
    #imeis = unique(assets$devices$imei),
    deviceInfo = TRUE,
    withLastSeen = TRUE
  ) |>
    dplyr::filter(.data$IMEI %in% as.numeric(unique(assets$devices$imei)))

  logger::log_info("Download latest binded tracks dataframe ...")
  latest_df <-
    download_cloud_file(
      name = paste0(conf$tracks_app$all_tracks$file_prefix, ".parquet"),
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    arrow::read_parquet()

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(unique(latest_df$Trip))
  new_trip_ids <- setdiff(boats_trips$Trip, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info(
    "Processing {length(new_trip_ids)} new tracks in parallel..."
  )

  tracks_list <- furrr::future_map(
    new_trip_ids,
    function(trip_id) {
      tryCatch(
        {
          track_data <- get_trip_points(
            token = conf$pds$token,
            secret = conf$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )
        },
        error = function(e) {
          logger::log_error("Error processing trip {trip_id}: {e$message}")
          NULL
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  tracks_df <- tracks_list |>
    purrr::compact() |>
    dplyr::bind_rows() |>
    dplyr::select("Time", "Trip", "Lat", "Lng") |>
    dplyr::mutate(
      Time = lubridate::floor_date(.data$Time, unit = "10 minutes")
    ) |>
    dplyr::group_by(.data$Trip, .data$Time) |>
    dplyr::summarise(
      Lat = stats::median(.data$Lat),
      Lng = stats::median(.data$Lng),
      .groups = "drop"
    )

  future::plan(future::sequential)

  tracks_df <-
    boats_trips |>
    dplyr::select("Trip", "IMEI") |>
    dplyr::distinct() |>
    dplyr::right_join(tracks_df, by = "Trip") |>
    dplyr::select("IMEI", "Trip", "Time", "Lat", "Lng")

  binded_tracks <-
    dplyr::bind_rows(tracks_df, latest_df) |>
    dplyr::distinct() |>
    dplyr::filter(!is.na(.data$IMEI))

  logger::log_info("Converting data to Parquet format...")

  # Write parquet file
  arrow::write_parquet(
    x = binded_tracks,
    sink = paste0(conf$tracks_app$all_tracks$file_prefix, ".parquet"),
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Upload backup tracks to cloud")

  upload_cloud_file(
    file = paste0(conf$tracks_app$all_tracks$file_prefix, ".parquet"),
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
