#' Download Parquet File from Cloud Storage
#'
#' Downloads a Parquet file from cloud storage and loads it into memory.
#' The local file is automatically cleaned up after reading.
#'
#' @param prefix A character string specifying the file prefix path in cloud storage.
#' @param provider A character string specifying the cloud storage provider key.
#' @param options A named list of cloud storage provider options.
#' @param bucket_name Optional character string specifying the GCS bucket name.
#'   If provided, overrides the bucket in `options`. If NULL (default), uses the
#'   bucket defined in `options`.
#'
#' @return A tibble containing the data from the Parquet file.
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' # Download from default bucket
#' data <- download_parquet_from_cloud(
#'   prefix = "raw-data/survey-data",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#'
#' # Download from a specific bucket
#' data <- download_parquet_from_cloud(
#'   prefix = "pds-trips",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options,
#'   bucket_name = conf$storage$google$buckets$mozambique
#' )
#' }
download_parquet_from_cloud <- function(
  prefix,
  provider,
  options,
  bucket_name = NULL
) {
  if (!is.null(bucket_name)) {
    options$bucket <- bucket_name
    logger::log_info("Using bucket: {bucket_name}")
  }

  parquet_file <- cloud_object_name(
    prefix = prefix,
    provider = provider,
    extension = "parquet",
    options = options
  )

  # Ensure local directory exists for nested paths
  dir.create(dirname(parquet_file), recursive = TRUE, showWarnings = FALSE)

  logger::log_info("Retrieving {parquet_file}")
  download_cloud_file(
    name = parquet_file,
    provider = provider,
    options = options
  )

  on.exit(
    {
      unlink(parquet_file)
      parent <- dirname(parquet_file)
      if (parent != "." && length(list.files(parent)) == 0) {
        unlink(parent, recursive = TRUE)
      }
    },
    add = TRUE
  )

  arrow::read_parquet(file = parquet_file)
}

#' Upload Data as Parquet File to Cloud Storage
#'
#' Writes a data frame to a versioned Parquet file and uploads it to cloud storage.
#'
#' @param data A data frame containing the data to be uploaded.
#' @param prefix A character string specifying the file prefix path.
#' @param provider A character string specifying the cloud storage provider key.
#' @param options A named list of cloud storage provider options.
#' @param compression A character string specifying compression type. Default is "lz4".
#' @param compression_level An integer specifying compression level. Default is 12.
#' @param bucket_name Optional character string specifying the GCS bucket name.
#'   If provided, overrides the bucket in `options`. If NULL (default), uses the
#'   bucket defined in `options`.
#'
#' @return Invisible NULL (called for side effects).
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' upload_parquet_to_cloud(
#'   data = survey_data,
#'   prefix = "raw-data/survey-data",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
upload_parquet_to_cloud <- function(
  data,
  prefix,
  provider,
  options,
  compression = "lz4",
  compression_level = 12,
  bucket_name = NULL
) {
  if (!is.null(bucket_name)) {
    options$bucket <- bucket_name
    logger::log_info("Using bucket: {bucket_name}")
  }

  preprocessed_filename <- prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = data,
    sink = preprocessed_filename,
    compression = compression,
    compression_level = compression_level
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = provider,
    options = options
  )

  invisible(NULL)
}
#' Upload File to Cloud Storage
#'
#' Uploads one or more local files to a cloud storage bucket.
#'
#' @param file A character vector of file paths to upload.
#' @param provider A character string specifying the cloud storage provider.
#' @param options A named list of cloud storage provider options.
#' @param name A character vector of names to assign files in cloud storage.
#'   Defaults to local filenames.
#'
#' @return A list of upload metadata.
#'
#' @keywords storage
#' @export
upload_cloud_file <- function(file, provider, options, name = file) {
  cloud_storage_authenticate(provider, options)

  out <- list()
  if ("gcs" %in% provider) {
    google_output <- purrr::map2(
      file,
      name,
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
#' Download File from Cloud Storage
#'
#' Downloads one or more files from cloud storage.
#'
#' @param name A character vector of object names in cloud storage.
#' @param provider A character string specifying the cloud storage provider.
#' @param options A named list of cloud storage provider options.
#' @param file A character vector of local file paths. Defaults to object names.
#'
#' @return A character vector of local file paths.
#'
#' @keywords storage
#' @export
download_cloud_file <- function(name, provider, options, file = name) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    purrr::map2(
      name,
      file,
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

#' Generate Cloud Object Name
#'
#' Generates the full object name for a versioned file in cloud storage.
#' When `version = "latest"`, returns the most recently updated object
#' matching the prefix and extension.
#'
#' @param prefix A character string specifying the file prefix.
#' @param version A character string specifying the version. Default is "latest".
#' @param extension A character string specifying the file extension.
#' @param provider A character string specifying the cloud storage provider.
#' @param exact_match A logical value indicating whether to match prefix exactly.
#' @param options A named list of cloud storage provider options.
#'
#' @return A character string with the full object name, or `character(0)` if
#'   no matching objects are found.
#'
#' @keywords storage internal
#' @export
cloud_object_name <- function(
  prefix,
  version = "latest",
  extension = "",
  provider,
  exact_match = FALSE,
  options
) {
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

    return(selected_rows$name[1])
  }
}

#' Authenticate to a Cloud Storage Provider
#'
#' Establishes authentication with the specified cloud provider. Skips
#' re-authentication if a valid token already exists.
#'
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of options specific to the cloud provider. For
#'   GCS, must include `service_account_key`.
#'
#' @return Invisible NULL (called for side effects).
#'
#' @keywords storage
#' @export
cloud_storage_authenticate <- function(provider, options) {
  if ("gcs" %in% provider) {
    # Only authenticate if there is no valid token
    if (isFALSE(googleAuthR::gar_has_token())) {
      temp_key_file <- tempfile(fileext = ".json")
      writeLines(options$service_account_key, temp_key_file)
      googleCloudStorageR::gcs_auth(json_file = temp_key_file)
      unlink(temp_key_file)
    }
  } else if (provider == "aws") {
    stop("AWS authentication not yet implemented")
  }

  invisible(NULL)
}


#' Retrieve Data from MongoDB
#'
#' Connects to a MongoDB database and retrieves all documents from a specified
#' collection, maintaining the original column order if a metadata document
#' is present.
#'
#' @param connection_string A character string specifying the MongoDB connection URL.
#' @param collection_name A character string specifying the name of the collection.
#' @param db_name A character string specifying the name of the database.
#'
#' @return A data frame containing all documents from the specified collection.
#'
#' @keywords storage
#' @export
mdb_collection_pull <- function(
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
) {
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  metadata <- collection$find(query = '{"type": "metadata"}')
  data <- collection$find(query = '{"type": {"$ne": "metadata"}}')

  if (nrow(metadata) > 0 && "columns" %in% names(metadata)) {
    stored_columns <- metadata$columns[[1]]

    for (col in stored_columns) {
      if (!(col %in% names(data))) {
        data[[col]] <- NA
      }
    }

    data <- data[, c(stored_columns, setdiff(names(data), stored_columns))]
  }

  data
}


#' Push Data to MongoDB Collection
#'
#' Uploads data to a MongoDB collection, optionally creating a geospatial index.
#' For non-geo collections, stores a metadata document preserving column order
#' (used by [mdb_collection_pull()] to restore structure).
#'
#' @param data A data frame or sf object to upload.
#' @param connection_string Character. MongoDB connection string.
#' @param collection_name Character. Name of the MongoDB collection.
#' @param db_name Character. Name of the MongoDB database.
#' @param geo Logical. Whether to create a 2dsphere index on the geometry field.
#'   Default is FALSE. When TRUE, the collection is dropped entirely before
#'   reinserting to avoid index conflicts with complex geometries.
#'
#' @return The number of data documents inserted (excluding metadata), or
#'   invisible TRUE for geo collections.
#'
#' @keywords storage
#' @export
mdb_collection_push <- function(
  data = NULL,
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL,
  geo = FALSE
) {
  if (
    is.null(data) ||
      is.null(connection_string) ||
      is.null(collection_name) ||
      is.null(db_name)
  ) {
    stop(
      "All parameters must be provided: data, connection_string, collection_name, db_name"
    )
  }

  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  if (geo) {
    # Drop entirely (documents + indexes) to avoid "Can't extract geo keys"
    # errors with complex MultiPolygon features
    collection$drop()

    # Reconnect: mongolite objects become invalid after drop()
    collection <- mongolite::mongo(
      collection = collection_name,
      db = db_name,
      url = connection_string
    )

    collection$insert(data)

    # Create 2dsphere index
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
        logger::log_info(
          "Index may need to be created manually via MongoDB shell"
        )
      }
    )

    return(invisible(TRUE))
  }

  # Non-geo path: clear documents only, preserve collection structure
  collection$remove("{}")

  # Store column order metadata for mdb_collection_pull()
  metadata <- list(
    type = "metadata",
    columns = names(data),
    timestamp = Sys.time()
  )
  collection$insert(metadata)
  collection$insert(data)

  collection$count() - 1
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
  tags = NULL
) {
  # Base URL
  base_url <- paste0(
    "https://analytics.pelagicdata.com/api/",
    token,
    "/v1/trips/",
    dateFrom,
    "/",
    dateTo
  )

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
    stop(
      "Request failed with status: ",
      httr2::resp_status(resp),
      "\n",
      httr2::resp_body_string(resp)
    )
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
get_trip_points <- function(
  token = NULL,
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
  overwrite = TRUE
) {
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
