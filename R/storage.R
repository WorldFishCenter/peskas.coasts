#' #' Download Parquet File from Cloud Storage
#'
#' This function handles the process of downloading a parquet file from cloud storage
#' and reading it into memory.
#'
#' @param prefix The file prefix path in cloud storage
#' @param provider The cloud storage provider key
#' @param options Cloud storage provider options
#'
#' @return A tibble containing the data from the parquet file
#'
#' @examples
#' \dontrun{
#' raw_data <- download_parquet_from_cloud(
#'   prefix = conf$ingestion$koboform$catch$legacy$raw,
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
#'
#' @keywords storage
#' @export
download_parquet_from_cloud <- function(prefix, provider, options) {
  # Generate cloud object name
  parquet_file <- cloud_object_name(
    prefix = prefix,
    provider = provider,
    extension = "parquet",
    options = options
  )

  # Log and download file
  logger::log_info("Retrieving {parquet_file}")
  download_cloud_file(
    name = parquet_file,
    provider = provider,
    options = options
  )

  # Read parquet file
  arrow::read_parquet(file = parquet_file)
}

#' Upload Processed Data to Cloud Storage
#'
#' This function handles the process of writing data to a parquet file and
#' uploading it to cloud storage.
#'
#' @param data The data frame or tibble to upload
#' @param prefix The file prefix path in cloud storage
#' @param provider The cloud storage provider key
#' @param options Cloud storage provider options
#' @param compression Compression algorithm to use (default: "lz4")
#' @param compression_level Compression level (default: 12)
#'
#' @return Invisible NULL
#'
#' @keywords storage
#' @examples
#' \dontrun{
#' upload_parquet_to_cloud(
#'   data = processed_data,
#'   prefix = conf$ingestion$koboform$catch$legacy$preprocessed,
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
#' @export
upload_parquet_to_cloud <- function(data, prefix, provider, options,
                                    compression = "lz4", compression_level = 12) {
  # Generate filename with version
  preprocessed_filename <- prefix %>%
    add_version(extension = "parquet")

  # Write parquet file
  arrow::write_parquet(
    x = data,
    sink = preprocessed_filename,
    compression = compression,
    compression_level = compression_level
  )

  # Log and upload file
  logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = provider,
    options = options
  )

  invisible(NULL)
}
#'
#' Authenticate to a Cloud Storage Provider
#'
#' This function is primarily used internally by other functions to establish authentication
#' with specified cloud providers such as Google Cloud Services (GCS) or Amazon Web Services (AWS).
#'
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of options specific to the cloud provider (see details).
#'
#' @details For GCS, the options list must include:
#' - `service_account_key`: The contents of the authentication JSON file from your Google Project.
#'
#' This function wraps [googleCloudStorageR::gcs_auth()] to handle GCS authentication.
#'
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_storage_authenticate("gcs", list(service_account_key = authentication_details))
#' #'
#' }
cloud_storage_authenticate <- function(provider, options) {
  if ("gcs" %in% provider) {
    # Only need to authenticate if there is no token for downstream requests
    if (isFALSE(googleAuthR::gar_has_token())) {
      service_account_key <- options$service_account_key
      temp_auth_file <- tempfile(fileext = "json")
      writeLines(service_account_key, temp_auth_file)
      googleCloudStorageR::gcs_auth(json_file = temp_auth_file)
    }
  }
}

#' Upload File to Cloud Storage
#'
#' Uploads a local file to a specified cloud storage bucket, supporting both single and multiple files.
#'
#' @param file A character vector specifying the path(s) of the file(s) to upload.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of provider-specific options including the bucket and authentication details.
#' @param name (Optional) The name to assign to the file in the cloud. If not specified, the local file name is used.
#'
#' @details For GCS, the options list must include:
#' - `bucket`: The name of the bucket to which files are uploaded.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' This function utilizes [googleCloudStorageR::gcs_upload()] for file uploads to GCS.
#'
#' @return A list of metadata objects for the uploaded files if successful.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' upload_cloud_file(
#'   "path/to/local_file.csv",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket")
#' )
#' }
#'
upload_cloud_file <- function(file, provider, options, name = file) {
  cloud_storage_authenticate(provider, options)

  out <- list()
  if ("gcs" %in% provider) {
    # Iterate over multiple files (and names)
    google_output <- purrr::map2(
      file, name,
      ~ googleCloudStorageR::gcs_upload(
        file = .x,
        bucket = options$bucket,
        name = .y,
        predefinedAcl = "bucketLevel"
      )
    )

    out <- c(out, google_output)
  }

  out
}

#' Retrieve Full Name of Versioned Cloud Object
#'
#' Gets the full name(s) of object(s) in cloud storage matching the specified prefix, version, and file extension.
#'
#' @param prefix A string indicating the object's prefix.
#' @param version A string specifying the version ("latest" or a specific version string).
#' @param extension The file extension to filter by. An empty string ("") includes all extensions.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param exact_match A logical indicating whether to match the prefix exactly.
#' @param options A named list of provider-specific options including the bucket and authentication details.
#'
#' @details For GCS, the options list should include:
#' - `bucket`: The bucket name.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' @return A vector of names of objects matching the criteria.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_object_name(
#'   "prefix",
#'   "latest",
#'   "json",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket")
#' )
#' #'
#' }
cloud_object_name <- function(prefix, version = "latest", extension = "",
                              provider, exact_match = FALSE, options) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    gcs_files <- googleCloudStorageR::gcs_list_objects(
      bucket = options$bucket,
      prefix = prefix
    )

    if (nrow(gcs_files) == 0) {
      return(character(0))
    }

    gcs_files_formatted <- gcs_files %>%
      tidyr::separate(
        col = .data$name,
        into = c("base_name", "version", "ext"),
        # Version is separated with the "__" string
        sep = "__",
        remove = FALSE
      ) %>%
      dplyr::filter(stringr::str_detect(.data$ext, paste0(extension, "$"))) %>%
      dplyr::group_by(.data$base_name, .data$ext)

    if (isTRUE(exact_match)) {
      selected_rows <- gcs_files_formatted %>%
        dplyr::filter(.data$base_name == prefix)
    } else {
      selected_rows <- gcs_files_formatted
    }

    if (version == "latest") {
      selected_rows <- selected_rows %>%
        dplyr::filter(max(.data$updated) == .data$updated)
    } else {
      this_version <- version
      selected_rows <- selected_rows %>%
        dplyr::filter(.data$version == this_version)
    }

    selected_rows$name
  }
}


#' Download Object from Cloud Storage
#'
#' Downloads an object from cloud storage to a local file.
#'
#' @param name The name of the object in the storage bucket.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of provider-specific options including the bucket and authentication details.
#' @param file (Optional) The local path to save the downloaded object. If not specified, the object name is used.
#'
#' @details For GCS, the options list should include:
#' - `bucket`: The name of the bucket from which the object is downloaded.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' @return The path to the downloaded file.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' download_cloud_file(
#'   "object_name.json",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket"),
#'   "local_path/to/save/object.json"
#' )
#' }
#'
download_cloud_file <- function(name, provider, options, file = name) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    purrr::map2(
      name, file,
      ~ googleCloudStorageR::gcs_get_object(
        object_name = .x,
        bucket = options$bucket,
        saveToDisk = .y,
        overwrite = ifelse(is.null(options$overwrite), TRUE, options$overwrite)
      )
    )
  }

  file
}

#' Retrieve Data from MongoDB
#'
#' This function connects to a MongoDB database and retrieves all documents from a specified collection,
#' maintaining the original column order if available.
#'
#' @param connection_string A character string specifying the MongoDB connection URL. Default is NULL.
#' @param collection_name A character string specifying the name of the collection to query. Default is NULL.
#' @param db_name A character string specifying the name of the database. Default is NULL.
#'
#' @return A data frame containing all documents from the specified collection, with columns ordered
#'         as they were when the data was originally pushed to MongoDB.
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Retrieve data from a MongoDB collection
#' result <- mdb_collection_pull(
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_pull <- function(connection_string = NULL, collection_name = NULL, db_name = NULL) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(collection = collection_name, db = db_name, url = connection_string)

  # Retrieve the metadata document
  metadata <- collection$find(query = '{"type": "metadata"}')

  # Retrieve all data documents
  data <- collection$find(query = '{"type": {"$ne": "metadata"}}')

  if (nrow(metadata) > 0 && "columns" %in% names(metadata)) {
    stored_columns <- metadata$columns[[1]]

    # Ensure all stored columns exist in the data
    for (col in stored_columns) {
      if (!(col %in% names(data))) {
        data[[col]] <- NA
      }
    }

    # Reorder columns to match stored order, and include any extra columns at the end
    data <- data[, c(stored_columns, setdiff(names(data), stored_columns))]
  }

  return(data)
}

#' Push Data to MongoDB Collection
#'
#' @description
#' Uploads data to a MongoDB collection, optionally creating a geospatial index.
#'
#' @param data A data frame or simple features (sf) object to upload.
#' @param connection_string Character. MongoDB connection string.
#' @param collection_name Character. Name of the MongoDB collection.
#' @param db_name Character. Name of the MongoDB database.
#' @param geo Logical. Whether to create a 2dsphere index on the geometry field. Default is FALSE.
#'
#' @return Logical. TRUE if successful.
#'
#' @examples
#' \dontrun{
#' # Push data without geospatial indexing
#' mdb_collection_push(
#'   data = my_data,
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "mycollection",
#'   db_name = "mydb",
#'   geo = FALSE
#' )
#'
#' # Push geospatial data with 2dsphere indexing
#' mdb_collection_push(
#'   data = sf_data,
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "geospatial_collection",
#'   db_name = "mydb",
#'   geo = TRUE
#' )
#' }
#'
#' @importFrom mongolite mongo
#' @keywords database mongodb
#' @export
mdb_collection_push <- function(data = NULL, connection_string = NULL, collection_name = NULL, db_name = NULL, geo = FALSE) {
  # Validate inputs
  if (is.null(data) || is.null(connection_string) || is.null(collection_name) || is.null(db_name)) {
    stop("All parameters must be provided: data, connection_string, collection_name, db_name")
  }

  # Connect to the MongoDB collection
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  # Remove all existing documents in the collection
  collection$remove("{}")

  # Insert the data
  collection$insert(data)

  # Create a 2dsphere index on the geometry field if geo is TRUE
  if (geo) {
    # Try direct command execution for creating geospatial index
    index_command <- sprintf(
      '{"createIndexes": "%s", "indexes": [{"key": {"geometry": "2dsphere"}, "name": "geometry_2dsphere"}]}',
      collection_name
    )
    tryCatch(
      {
        collection$run(index_command)
        logger::log_info("Created 2dsphere index on geometry field")
      },
      error = function(e) {
        logger::log_warn("Failed to create 2dsphere index: {e$message}")
        logger::log_info("Index may need to be created manually via MongoDB shell")
      }
    )
  }

  return(TRUE)
}
#' Get metadata tables
#'
#' Get Metadata tables from Google sheets. This function downloads
#' the tables that include information about the fishery. You can specify
#' a single table to download or get all available tables.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' metadata:
#'   google_sheets:
#'     sheet_id:
#'     tables:
#'       - table1
#'       - table2
#' ```
#'
#' @param table Character. Name of the specific table to download. If NULL (default),
#'   all tables specified in the configuration will be downloaded.
#' @param log_threshold The logging threshold level. Default is logger::DEBUG.
#'
#' @return A named list containing the requested tables as data frames. If a single
#'   table is requested, the list will contain only that table. If no table is
#'   specified, the list will contain all available tables.
#'
#' @export
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Ensure you have the necessary configuration in conf.yml
#'
#' # Download all metadata tables
#' metadata_tables <- get_metadata()
#'
#' # Download a specific table
#' catch_table <- get_metadata(table = "devices")
#' }
get_metadata <- function(table = NULL, log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = conf$storage$google$options$service_account_key,
    use_oob = TRUE
  )

  # If table is specified, validate it exists in the configuration
  if (!is.null(table)) {
    if (!table %in% conf$metadata$google_sheets$tables) {
      stop(sprintf(
        "Table '%s' not found in configuration. Available tables: %s",
        table,
        paste(conf$metadata$google_sheets$tables, collapse = ", ")
      ))
    }

    logger::log_info(sprintf("Downloading metadata table: %s", table))
    tables <- list(googlesheets4::range_read(
      ss = conf$metadata$google_sheets$sheet_id,
      sheet = table,
      col_types = "c"
    ))
    names(tables) <- table
  } else {
    logger::log_info("Downloading all metadata tables")
    tables <- conf$metadata$google_sheets$tables %>%
      rlang::set_names() %>%
      purrr::map(~ googlesheets4::range_read(
        ss = conf$metadata$google_sheets$sheet_id,
        sheet = .x,
        col_types = "c"
      ))
  }

  tables
}


#' Retrieve Trip Details from Pelagic Data API
#'
#' This function retrieves trip details from the Pelagic Data API for a specified time range,
#' with options to filter by IMEIs and include additional information.
#'
#' @param token Character string. The API token for authentication.
#' @param secret Character string. The API secret for authentication.
#' @param dateFrom Character string. Start date in 'YYYY-MM-dd' format.
#' @param dateTo Character string. End date in 'YYYY-MM-dd' format.
#' @param imeis Character vector. Optional. Filter by IMEI numbers.
#' @param deviceInfo Logical. If TRUE, include device IMEI and ID fields in the response. Default is FALSE.
#' @param withLastSeen Logical. If TRUE, include device last seen date in the response. Default is FALSE.
#' @param tags Character vector. Optional. Filter by trip tags.
#'
#' @return A data frame containing trip details.
#' @keywords ingestion
#' @examples
#' \dontrun{
#' trips <- get_trips(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2020-05-01",
#'   dateTo = "2020-05-03",
#'   imeis = c("123456789", "987654321"),
#'   deviceInfo = TRUE,
#'   withLastSeen = TRUE,
#'   tags = c("tag1", "tag2")
#' )
#' }
#'
#' @export
#'
get_trips <- function(
    token = NULL,
    secret = NULL,
    dateFrom = NULL,
    dateTo = NULL,
    imeis = NULL,
    deviceInfo = FALSE,
    withLastSeen = FALSE,
    tags = NULL) {
  # Base URL
  base_url <- paste0("https://analytics.pelagicdata.com/api/", token, "/v1/trips/", dateFrom, "/", dateTo)

  # Build query parameters
  query_params <- list()
  if (!is.null(imeis)) {
    query_params$imeis <- paste(imeis, collapse = ",")
  }
  if (deviceInfo) {
    query_params$deviceInfo <- "true"
  }
  if (withLastSeen) {
    query_params$withLastSeen <- "true"
  }
  if (!is.null(tags)) {
    query_params$tags <- paste(tags, collapse = ",")
  }

  # Build the request
  req <- httr2::request(base_url) %>%
    httr2::req_headers(
      "X-API-SECRET" = secret,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_url_query(!!!query_params)

  # Perform the request
  resp <- req %>% httr2::req_perform()

  # Check for HTTP errors
  if (httr2::resp_status(resp) != 200) {
    stop("Request failed with status: ", httr2::resp_status(resp), "\n", httr2::resp_body_string(resp))
  }

  # Read CSV content
  content_text <- httr2::resp_body_string(resp)
  trips_data <- readr::read_csv(content_text, show_col_types = FALSE)

  return(trips_data)
}



#' Get Trip Points from Pelagic Data Systems API
#'
#' Retrieves trip points data from the Pelagic Data Systems API. The function can either
#' fetch data for a specific trip ID or for a date range. The response can be returned
#' as a data frame or written directly to a file.
#'
#' @param token Character string. Access token for the PDS API.
#' @param secret Character string. Secret key for the PDS API.
#' @param id Numeric or character. Optional trip ID. If provided, retrieves points for
#'   specific trip. If NULL, dateFrom and dateTo must be provided.
#' @param dateFrom Character string. Start date for data retrieval in format "YYYY-MM-DD".
#'   Required if id is NULL.
#' @param dateTo Character string. End date for data retrieval in format "YYYY-MM-DD".
#'   Required if id is NULL.
#' @param path Character string. Optional path where the CSV file should be saved.
#'   If provided, the function returns the path instead of the data frame.
#' @param imeis Vector of character or numeric. Optional IMEI numbers to filter the data.
#' @param deviceInfo Logical. If TRUE, includes device information in the response.
#'   Default is FALSE.
#' @param errant Logical. If TRUE, includes errant points in the response.
#'   Default is FALSE.
#' @param withLastSeen Logical. If TRUE, includes last seen information.
#'   Default is FALSE.
#' @param tags Vector of character. Optional tags to filter the data.
#' @param overwrite Logical. If TRUE, will overwrite existing file when path is provided.
#'   Default is TRUE.
#'
#' @return If path is NULL, returns a tibble containing the trip points data.
#'   If path is provided, returns the file path as a character string.
#'
#' @examples
#' \dontrun{
#' # Get data for a specific trip
#' trip_data <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   id = "12345",
#'   deviceInfo = TRUE
#' )
#'
#' # Get data for a date range
#' date_data <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2024-01-01",
#'   dateTo = "2024-01-31"
#' )
#'
#' # Save data directly to file
#' file_path <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   id = "12345",
#'   path = "trip_data.csv"
#' )
#' }
#'
#' @keywords ingestion
#'
#' @export
get_trip_points <- function(token = NULL,
                            secret = NULL,
                            id = NULL,
                            dateFrom = NULL,
                            dateTo = NULL,
                            path = NULL,
                            imeis = NULL,
                            deviceInfo = FALSE,
                            errant = FALSE,
                            withLastSeen = FALSE,
                            tags = NULL,
                            overwrite = TRUE) {
  # Build base URL based on whether ID is provided
  if (!is.null(id)) {
    base_url <- paste0(
      "https://analytics.pelagicdata.com/api/",
      token,
      "/v1/trips/",
      id,
      "/points"
    )
  } else {
    if (is.null(dateFrom) || is.null(dateTo)) {
      stop("dateFrom and dateTo are required when id is not provided")
    }
    base_url <- paste0(
      "https://analytics.pelagicdata.com/api/",
      token,
      "/v1/points/",
      dateFrom,
      "/",
      dateTo
    )
  }

  # Build query parameters
  query_params <- list()
  if (!is.null(imeis)) {
    query_params$imeis <- paste(imeis, collapse = ",")
  }
  if (deviceInfo) {
    query_params$deviceInfo <- "true"
  }
  if (errant) {
    query_params$errant <- "true"
  }
  if (withLastSeen) {
    query_params$withLastSeen <- "true"
  }
  if (!is.null(tags)) {
    query_params$tags <- paste(tags, collapse = ",")
  }
  # Add format=csv if saving to file
  if (!is.null(path)) {
    query_params$format <- "csv"
  }

  # Build the request
  req <- httr2::request(base_url) %>%
    httr2::req_headers(
      "X-API-SECRET" = secret,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_url_query(!!!query_params)

  # Perform the request
  resp <- req %>% httr2::req_perform()

  # Check for HTTP errors first
  if (httr2::resp_status(resp) != 200) {
    stop(
      "Request failed with status: ",
      httr2::resp_status(resp),
      "\n",
      httr2::resp_body_string(resp)
    )
  }

  # Handle the response based on whether path is provided
  if (!is.null(path)) {
    # Write the response content to file
    writeBin(httr2::resp_body_raw(resp), path)
    result <- path
  } else {
    # Read CSV content
    content_text <- httr2::resp_body_string(resp)
    result <- readr::read_csv(content_text, show_col_types = FALSE)
  }

  return(result)
}
