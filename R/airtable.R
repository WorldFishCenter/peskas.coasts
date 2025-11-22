#' Get All Records from Airtable with Pagination
#'
#' Retrieves ALL records from an Airtable table, handling pagination automatically.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. The name of the table to retrieve.
#' @param token Character string. Airtable API token for authentication.
#' @param list_handler Character string. "collapse" (default) or "count" for list fields.
#'
#' @return A tibble with all records and an 'airtable_id' column.
#'
#' @export
airtable_to_df <- function(
  base_id,
  table_name,
  token,
  list_handler = "collapse"
) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  all_records <- list()
  offset <- NULL
  page_count <- 0
  total_retrieved <- 0

  repeat {
    page_count <- page_count + 1
    cat("Fetching page", page_count, "...")

    # Make the request
    req <- httr2::request(base_url) %>%
      httr2::req_headers(Authorization = paste("Bearer", token))

    if (!is.null(offset)) {
      req <- req %>% httr2::req_url_query(offset = offset)
    }

    res <- req %>% httr2::req_perform()
    content <- res %>% httr2::resp_body_json()

    # Add records from this page
    page_records <- content$records
    all_records <- c(all_records, page_records)
    total_retrieved <- total_retrieved + length(page_records)

    cat(
      " retrieved",
      length(page_records),
      "records (total:",
      total_retrieved,
      ")\n"
    )

    # Check if there are more pages
    if (is.null(content$offset)) {
      cat("Retrieved all available records\n")
      break
    }

    offset <- content$offset
  }

  cat("Converting", length(all_records), "records to data frame...\n")

  # Convert all records to tibble
  df <- all_records %>%
    purrr::map_dfr(
      ~ {
        fields <- .x$fields
        fields$airtable_id <- .x$id

        # Handle lists
        if (list_handler == "collapse") {
          fields <- fields %>%
            purrr::map_if(is.list, ~ paste(.x, collapse = ", "))
        } else if (list_handler == "count") {
          fields <- fields %>%
            purrr::map_if(is.list, length)
        }

        dplyr::as_tibble(fields)
      }
    )

  cat("Successfully converted", nrow(df), "records to tibble\n")
  return(df)
}

#' Get Writable Fields from Airtable Table
#'
#' Returns which fields can be updated (excludes computed fields).
#'
#' @param base_id Character string. The Airtable base ID.
#' @param token Character string. Airtable API token.
#' @param table_name Character string. Name of the table.
#'
#' @return Character vector of writable field names.
#'
#' @export
get_writable_fields <- function(base_id, token, table_name) {
  schema_url <- glue::glue(
    "https://api.airtable.com/v0/meta/bases/{base_id}/tables"
  )

  response <- httr2::request(schema_url) %>%
    httr2::req_headers(Authorization = paste("Bearer", token)) %>%
    httr2::req_perform()

  content <- response %>% httr2::resp_body_json()

  # Find the table
  target_table <- content$tables %>%
    purrr::keep(~ .x$name == table_name) %>%
    purrr::pluck(1)

  if (is.null(target_table)) {
    available_tables <- content$tables %>% purrr::map_chr("name")
    stop(
      "Table '",
      table_name,
      "' not found. Available: ",
      paste(available_tables, collapse = ", ")
    )
  }

  # Show field types
  cat("Field types in", table_name, "table:\n")
  for (field in target_table$fields) {
    status <- if (
      !is.null(field$options$isComputed) && field$options$isComputed
    ) {
      " [COMPUTED]"
    } else {
      " [WRITABLE]"
    }
    cat("- '", field$name, "' (", field$type, ")", status, "\n", sep = "")
  }

  # Return writable fields
  writable_fields <- target_table$fields %>%
    purrr::keep(~ is.null(.x$options$isComputed) || !.x$options$isComputed) %>%
    purrr::map_chr("name")

  return(writable_fields)
}

#' Update Single Airtable Record
#'
#' Updates specific fields in one record.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#' @param record_id Character string. ID of the record to update.
#' @param updates Named list. Fields and values to update.
#'
#' @return httr2 response object.
#'
#' @export
update_airtable_record <- function(
  base_id,
  table_name,
  token,
  record_id,
  updates
) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}/{record_id}"
  )
  payload <- list(fields = updates)

  tryCatch(
    {
      response <- httr2::request(base_url) %>%
        httr2::req_headers(
          Authorization = paste("Bearer", token),
          `Content-Type` = "application/json"
        ) %>%
        httr2::req_body_json(payload) %>%
        httr2::req_method("PATCH") %>%
        httr2::req_perform()
      return(response)
    },
    error = function(e) {
      if (!is.null(e$resp)) {
        error_body <- e$resp %>% httr2::resp_body_string()
        cat("Error response:\n", error_body, "\n")
      }
      stop(e)
    }
  )
}

#' Bulk Update Multiple Airtable Records
#'
#' Updates multiple records in batches of 10.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#' @param updates_df Data frame with 'airtable_id' column and fields to update.
#'
#' @return List of response objects.
#'
#' @export
bulk_update_airtable <- function(base_id, table_name, token, updates_df) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  # Prepare records
  records <- updates_df %>%
    purrr::pmap(function(airtable_id, ...) {
      updates <- list(...)
      updates$airtable_id <- NULL
      updates <- updates[!is.na(updates)] # Remove NAs

      list(id = airtable_id, fields = updates)
    })

  # Split into batches of 10
  batches <- split(records, ceiling(seq_along(records) / 10))

  # Send each batch
  responses <- purrr::map(batches, function(batch) {
    payload <- list(records = batch)
    cat("Updating batch of", length(batch), "records...\n")

    tryCatch(
      {
        httr2::request(base_url) %>%
          httr2::req_headers(
            Authorization = paste("Bearer", token),
            `Content-Type` = "application/json"
          ) %>%
          httr2::req_body_json(payload) %>%
          httr2::req_method("PATCH") %>%
          httr2::req_perform()
      },
      error = function(e) {
        if (!is.null(e$resp)) {
          error_body <- e$resp %>% httr2::resp_body_string()
          cat("Error response:\n", error_body, "\n")
        }
        stop(e)
      }
    )
  })

  cat("Successfully updated", nrow(updates_df), "records!\n")
  return(responses)
}

#' Create New Airtable Records
#'
#' Creates new records in batches of 10.
#'
#' @param df Data frame containing data to create.
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#'
#' @return List of response objects.
#'
#' @export
df_to_airtable <- function(df, base_id, table_name, token) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  # Convert to records format
  records <- df %>%
    purrr::pmap(function(...) {
      row_data <- list(...)
      row_data <- row_data[!is.na(row_data)] # Remove NAs
      list(fields = row_data)
    })

  # Split into batches of 10
  batches <- split(records, ceiling(seq_along(records) / 10))

  responses <- purrr::map(batches, function(batch) {
    payload <- list(records = batch)
    cat("Creating batch of", length(batch), "records...\n")

    tryCatch(
      {
        httr2::request(base_url) %>%
          httr2::req_headers(
            Authorization = paste("Bearer", token),
            `Content-Type` = "application/json"
          ) %>%
          httr2::req_body_json(payload) %>%
          httr2::req_method("POST") %>%
          httr2::req_perform()
      },
      error = function(e) {
        if (!is.null(e$resp)) {
          error_body <- e$resp %>% httr2::resp_body_string()
          cat("Error response:\n", error_body, "\n")
        }
        stop(e)
      }
    )
  })

  cat("Successfully created", nrow(df), "records!\n")
  return(responses)
}

#' Sync Data with Airtable (Update + Create)
#'
#' Main function for daily syncing. Updates existing records and creates new ones.
#'
#' @param boats_df Data frame with device data. Must include key_field column.
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table (default: "pds_devices").
#' @param token Character string. Airtable API token.
#' @param key_field Character string. Field to match on (default: "imei").
#'
#' @return List with update and create results.
#'
#' @export
device_sync <- function(
  boats_df,
  base_id,
  table_name = "pds_devices",
  token,
  key_field = "imei"
) {
  # Validation
  if (!key_field %in% names(boats_df)) {
    stop("Key field '", key_field, "' not found in data")
  }

  if (nrow(boats_df) == 0) {
    cat("No records to sync\n")
    return(NULL)
  }

  # Remove duplicates
  original_count <- nrow(boats_df)
  boats_df <- boats_df %>%
    dplyr::group_by(!!rlang::sym(key_field)) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()

  if (original_count > nrow(boats_df)) {
    cat("Removed", original_count - nrow(boats_df), "duplicate records\n")
  }

  cat("Syncing", nrow(boats_df), "devices...")

  # Get existing records
  existing_df <- airtable_to_df(base_id, table_name, token)

  # Filter to writable fields only
  tryCatch(
    {
      writable_fields <- get_writable_fields(base_id, token, table_name)
      cat("\nFiltering to writable fields only...\n")
      boats_df <- boats_df %>%
        dplyr::select(dplyr::any_of(c(writable_fields, key_field)))
    },
    error = function(e) {
      cat("Warning: Could not get writable fields\n")
    }
  )

  # Convert key field to character for matching
  boats_df[[key_field]] <- as.character(boats_df[[key_field]])
  existing_df[[key_field]] <- as.character(existing_df[[key_field]])

  # Separate updates vs creates
  updates <- boats_df %>%
    dplyr::filter(!!rlang::sym(key_field) %in% existing_df[[key_field]]) %>%
    dplyr::left_join(
      existing_df %>%
        dplyr::select(dplyr::all_of(key_field), "airtable_id"),
      by = key_field
    ) %>%
    # Remove duplicate airtable_ids
    dplyr::group_by(.data$airtable_id) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()

  creates <- boats_df %>%
    dplyr::filter(!(!!rlang::sym(key_field) %in% existing_df[[key_field]]))

  # Fix data types for Airtable
  numeric_fields <- c("last_seen", "external_id", "external_boat_id")
  for (field in numeric_fields) {
    if (field %in% names(updates)) {
      updates[[field]] <- as.numeric(updates[[field]])
    }
    if (field %in% names(creates)) {
      creates[[field]] <- as.numeric(creates[[field]])
    }
  }

  if ("active" %in% names(updates)) {
    updates$active <- as.logical(updates$active)
  }
  if ("active" %in% names(creates)) {
    creates$active <- as.logical(creates$active)
  }

  results <- list(updates = NULL, creates = NULL)

  # Perform operations
  if (nrow(updates) > 0) {
    results$updates <- bulk_update_airtable(base_id, table_name, token, updates)
  }

  if (nrow(creates) > 0) {
    results$creates <- df_to_airtable(creates, base_id, table_name, token)
  }

  cat("Updated:", nrow(updates), "| Created:", nrow(creates), "\n")
  return(results)
}

#' Sync Device Users to MongoDB and Update Airtable
#'
#' Retrieves device data from Airtable, joins with user data, generates passwords
#' for new users, updates the Airtable users table with new passwords, and pushes
#' the combined dataset to MongoDB. This function maintains user credentials for
#' devices in Africa timezones with complete synchronization between systems.
#'
#' @param conf List. Configuration parameters (defaults to read_config()).
#'   Must contain airtable (token, frame and tracks_app base_ids) and storage
#'   (mongodb tracks_app connection_string, database_name, collection) configuration.
#' @param seed Numeric. Random seed for password generation (default: 123).
#'   Set to NULL for random passwords each time.
#'
#' @return List containing user data counts, Airtable sync result, and MongoDB push result.
#'   - total_users: Number of users synchronized to MongoDB
#'   - new_passwords_generated: Number of new passwords created
#'   - airtable_sync_result: Result of syncing users to Airtable (updates + creates)
#'   - mongodb_result: Result of MongoDB data push
#'
#' @details
#' The function performs the following steps:
#' 1. Retrieves users data from Airtable tracks_app base
#' 2. Retrieves device data from Airtable frame base (pds_devices table)
#' 3. Filters devices to Africa timezones only
#' 4. Joins devices with existing user data by IMEI
#' 5. Generates 8-character alphanumeric passwords for ALL users without passwords
#' 6. Removes duplicate IMEI entries (keeps latest)
#' 7. Syncs complete user dataset to Airtable users table (updates existing, creates new)
#' 8. Pushes the complete dataset to MongoDB
#'
#' Password generation uses letters (upper/lower), numbers (0-9), and is
#' reproducible when a seed is provided. The function ensures complete
#' synchronization by updating both Airtable and MongoDB with the same data.
#'
#' @examples
#' \dontrun{
#' # Using default configuration and seed
#' result <- sync_device_users()
#' cat("Generated", result$new_passwords_generated, "new passwords")
#' cat("Synced", result$total_users, "users to MongoDB")
#'
#' # Using custom configuration and random passwords
#' custom_config <- read_config("custom_conf.yml")
#' result <- sync_device_users(custom_config, seed = NULL)
#'
#' # Check Airtable sync results
#' if (!is.null(result$airtable_sync_result)) {
#'   cat("Successfully synced users to Airtable")
#'   cat("Updates:", length(result$airtable_sync_result$updates))
#'   cat("Creates:", length(result$airtable_sync_result$creates))
#' }
#' }
#'
#' @export
sync_device_users <- function(conf = NULL, seed = 123) {
  if (is.null(conf)) {
    conf <- read_config()
  }
  logger::log_info("Starting device users sync to MongoDB")

  # Set seed if provided
  if (!is.null(seed)) {
    set.seed(seed)
  }

  # Get users data from tracks_app base
  users <- airtable_to_df(
    base_id = conf$airtable$tracks_app$base_id,
    table_name = "users",
    token = conf$airtable$token
  )

  #backup users
  upload_parquet_to_cloud(
    data = users,
    prefix = conf$tracks_app$backup_users$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Retrieved {nrow(users)} user records from Airtable")

  # Get devices data from frame base
  devices <- airtable_to_df(
    base_id = conf$airtable$frame$base_id,
    table_name = "pds_devices",
    token = conf$airtable$token
  )

  logger::log_info("Retrieved {nrow(devices)} device records from Airtable")

  # Process and join device data with users
  devices_joined <- devices |>
    dplyr::select(
      "customer_name",
      IMEI = "imei",
      "captain",
      "vessel_type",
      Boat = "boat_name",
      Region = "region",
      Community = "community",
      "registration_number",
      "device_timezone"
    ) |>
    # Filter to Africa timezones only
    dplyr::filter(stringr::str_detect(.data$device_timezone, "Africa")) |>
    dplyr::mutate(
      Boat = tolower(.data$Boat),
      Country = dplyr::case_when(
        stringr::str_detect(.data$customer_name, "Kenya") ~ "Kenya",
        stringr::str_detect(.data$customer_name, "Zanzibar") ~ "Zanzibar",
        stringr::str_detect(.data$customer_name, "Tanzania") ~ "Tanzania",
        stringr::str_detect(.data$customer_name, "Mozambique") ~ "Mozambique",
        stringr::str_detect(.data$customer_name, "Malawi") ~ "Malawi",
        stringr::str_detect(.data$customer_name, "Egypt") ~ "Egypt",
        TRUE ~ NA
      )
    ) |>
    dplyr::right_join(
      users,
      by = c("IMEI", "Country", "Region", "Community")
    ) |>
    dplyr::mutate(Boat = dplyr::coalesce(.data$Boat.x, .data$Boat.y)) |>
    dplyr::select(-"Boat.x", -"Boat.y")

  logger::log_info("Filtered to {nrow(devices_joined)} Africa-timezone devices")

  # Generate passwords for ALL users without them
  devices_with_passwords <-
    devices_joined |>
    dplyr::rowwise() |>
    dplyr::mutate(
      password = if (is.na(.data$password)) {
        paste(
          sample(c(letters, LETTERS, 0:9), 8, replace = TRUE),
          collapse = ""
        )
      } else {
        .data$password
      }
    ) |>
    dplyr::ungroup() |>
    # Remove duplicates, keeping the latest entry per IMEI
    dplyr::group_by(.data$IMEI) |>
    dplyr::slice_head(n = 1) |>
    dplyr::ungroup()

  # Count how many passwords were generated
  users_before_passwords <- sum(is.na(devices_joined$password))
  new_passwords <- users_before_passwords
  logger::log_info("Generated passwords for {new_passwords} users")

  # Sync all users data to Airtable users table
  airtable_sync_result <- NULL
  if (nrow(devices_with_passwords) > 0) {
    logger::log_info("Syncing user data to Airtable users table")

    # Prepare data for Airtable sync (only relevant fields)
    users_for_sync <-
      devices_with_passwords |>
      dplyr::select(
        "Country",
        "IMEI",
        "Boat",
        "Community",
        "Region",
        "password",
        "airtable_id"
      ) |>
      # Handle missing airtable_id (these will be creates, not updates)
      dplyr::mutate(
        airtable_id = ifelse(is.na(.data$airtable_id), NA, .data$airtable_id)
      )

    # Use device_sync function to handle both updates and creates
    airtable_sync_result <- device_sync(
      boats_df = users_for_sync,
      base_id = conf$airtable$tracks_app$base_id,
      table_name = "users",
      token = conf$airtable$token,
      key_field = "IMEI"
    )

    logger::log_info("Successfully synced user data to Airtable")
  }

  # Remove airtable_id from final dataset for MongoDB
  devices_with_passwords <-
    devices_with_passwords |>
    dplyr::select(
      "IMEI",
      "Country",
      "Boat",
      "captain",
      "vessel_type",
      "Community",
      "Region",
      "password"
    )
  # Push to MongoDB
  push_result <- mdb_collection_push(
    data = devices_with_passwords,
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = conf$storage$mongodb$tracks_app$collection$users,
    db_name = conf$storage$mongodb$tracks_app$database_name,
    geo = FALSE # No geospatial indexing needed
  )

  logger::log_info(
    "Successfully pushed {nrow(devices_with_passwords)} device users to MongoDB"
  )

  return(list(
    total_users = nrow(devices_with_passwords),
    new_passwords_generated = new_passwords,
    airtable_sync_result = airtable_sync_result,
    mongodb_result = push_result
  ))
}

#' Fetch and Filter Asset Data from Airtable
#'
#' @description
#' Retrieves data from a specified Airtable table
#'
#' @param table_name Character. Name of the Airtable table to fetch.
#' @param select_cols Character vector. Column names to select from the table.
#' @param conf Configuration object from read_config().
#'
#' @return A filtered and selected data frame from Airtable
#'
#' @keywords preprocessing helper
#' @export
fetch_asset <- function(
  table_name = NULL,
  select_cols = NULL,
  conf = NULL
) {
  airtable_to_df(
    base_id = conf$airtable$frame$base_id,
    table_name = table_name,
    token = conf$airtable$token
  ) |>
    janitor::clean_names() |>
    dplyr::select(dplyr::all_of(select_cols))
}

#' Fetch Multiple Asset Tables from Airtable
#'
#' @description
#' Fetches taxa, gear, vessels, and landing sites data from Airtable.
#' Returns distinct records for each table.
#'
#' @param conf Configuration object from read_config().
#'
#' @return A named list containing four data frames:
#'   \itemize{
#'     \item \code{taxa}: Contains survey_label, alpha3_code, and scientific_name columns
#'     \item \code{gear}: Contains survey_label and standard_name columns
#'     \item \code{vessels}: Contains survey_label and standard_name columns
#'     \item \code{sites}: Contains site and site_code columns
#'   }
#'
#' @details
#' Each table is fetched separately using `fetch_asset()` and filtered to return
#' only distinct rows to avoid duplicates in the mapping tables.
#'
#' @keywords preprocessing helper
#' @export
fetch_assets <- function(conf = NULL) {
  assets_list <-
    list(
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
      sites = fetch_asset(
        table_name = "forms",
        select_cols = c("form_id", "form_name", "site_code"),
        conf = conf
      )
    )

  purrr::map(assets_list, ~ dplyr::distinct(.x))
}
