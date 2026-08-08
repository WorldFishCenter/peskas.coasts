#' Download Parquet File from Cloud Storage
#'
#' Downloads a Parquet file from cloud storage and loads it into memory.
#' The local file is automatically cleaned up after reading.
#'
#' @param prefix A character string specifying the file prefix path in cloud storage.
#' @param provider A character string specifying the cloud storage provider key.
#' @param options A named list of cloud storage provider options.
#' @param version A character string specifying the version to retrieve.
#'   Default is "latest", which returns the most recently updated object.
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
#' # Download latest version from default bucket
#' data <- download_parquet_from_cloud(
#'   prefix = "raw-data/survey-data",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#'
#' # Download a specific version
#' data <- download_parquet_from_cloud(
#'   prefix = "raw-data/survey-data",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options,
#'   version = "20250101T120000"
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
  version = "latest",
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
    version = version,
    options = options
  )

  local_file <- basename(parquet_file)

  logger::log_info("Retrieving {parquet_file}")
  download_cloud_file(
    name = parquet_file,
    provider = provider,
    options = options,
    file = local_file
  )

  on.exit(unlink(local_file), add = TRUE)

  arrow::read_parquet(file = local_file)
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
  # Always get a fresh token before uploading. Service-account tokens expire
  # after 1 hour; long upstream jobs (e.g. predict_pds_tracks) can exhaust
  # this window. Forcing re-auth here means gcs_upload() never needs to
  # auto-refresh a stale token mid-flight — the source of the HTTP/2
  # PROTOCOL_ERROR failures (newer gargle uses httr2 for token refresh,
  # which is unaffected by httr::set_config).
  cloud_storage_authenticate(provider, options, force = TRUE)

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
#' Resolves **one** versioned object in cloud storage. When `version = "latest"`,
#' returns the most recently updated object matching the prefix and extension.
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
#' @section Resolves one object, never enumerates a bucket:
#' This function is **not** a listing helper. It always returns a single name —
#' `selected_rows$name[1]` — even though it groups internally by `base_name` and
#' `ext`. Point it at a prefix that matches many *distinct* base names (for
#' example one object per GPS trip, where tens of thousands share a prefix) and
#' it silently hands back an arbitrary one of them rather than erroring. It is
#' built for the versioned-snapshot pattern `<prefix>__<version>__.<ext>`, where
#' the prefix identifies exactly one logical dataset and the only question is
#' which version of it to take.
#'
#' To enumerate objects, use [cloud_object_names()], which returns every match.
#'
#' The single-name return type is depended upon by downstream pipelines and will
#' not change.
#'
#' @seealso [cloud_object_names()] for enumeration.
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
        dplyr::filter(!is.na(.data$updated)) %>%
        dplyr::filter(.data$updated == max(.data$updated))
    } else {
      this_version <- version
      selected_rows <- selected_rows %>%
        dplyr::filter(.data$version == this_version)
    }

    return(selected_rows$name[1])
  }
}

#' List Cloud Object Names Matching a Prefix
#'
#' Enumerates **every** object matching a prefix and extension, unlike
#' [cloud_object_name()] which resolves a single versioned object. Use this when
#' a prefix legitimately covers many distinct objects — for example one Parquet
#' file per GPS trip.
#'
#' @param prefix A character string specifying the object name prefix.
#' @param provider A character string specifying the cloud storage provider.
#' @param options A named list of cloud storage provider options.
#' @param extension A character string the object name must end with. Default
#'   `""`, i.e. no extension filter.
#' @param latest_only Logical. If `TRUE`, keeps only the most recently updated
#'   object per distinct `base_name`, applying [cloud_object_name()]'s
#'   "latest" semantics to each base name independently rather than across the
#'   whole match set. Default `FALSE`, which returns everything.
#'
#' @return A character vector of object names, sorted, possibly of length zero.
#'   Never `NA`.
#'
#' @details
#' Names that do not follow the `<base_name>__<version>__.<ext>` convention are
#' still returned when `latest_only = FALSE`; they are skipped when
#' `latest_only = TRUE`, because there is no version to compare.
#'
#' @seealso [cloud_object_name()] for resolving a single versioned object.
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' # Every stored track object
#' all_tracks <- cloud_object_names(
#'   prefix = "pds-track",
#'   provider = conf$storage$google$key,
#'   options = conf$pds_storage$google$options,
#'   extension = "parquet"
#' )
#'
#' # The latest version of each distinct base name under a prefix
#' latest_each <- cloud_object_names(
#'   prefix = "trips-",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options,
#'   extension = "parquet",
#'   latest_only = TRUE
#' )
#' }
cloud_object_names <- function(
  prefix,
  provider,
  options,
  extension = "",
  latest_only = FALSE
) {
  cloud_storage_authenticate(provider, options)

  if (!("gcs" %in% provider)) {
    stop("cloud_object_names() currently supports only the 'gcs' provider")
  }

  gcs_files <- googleCloudStorageR::gcs_list_objects(
    bucket = options$bucket,
    prefix = prefix
  )

  if (nrow(gcs_files) == 0) {
    return(character(0))
  }

  matching <- gcs_files %>%
    dplyr::filter(
      stringr::str_detect(.data$name, paste0(extension, "$"))
    )

  if (nrow(matching) == 0) {
    return(character(0))
  }

  if (isTRUE(latest_only)) {
    matching <- matching %>%
      tidyr::separate(
        col = .data$name,
        into = c("base_name", "version", "ext"),
        sep = "__",
        remove = FALSE,
        fill = "right",
        extra = "merge"
      ) %>%
      dplyr::filter(!is.na(.data$version), !is.na(.data$updated)) %>%
      dplyr::group_by(.data$base_name, .data$ext) %>%
      dplyr::filter(.data$updated == max(.data$updated)) %>%
      dplyr::ungroup()
  }

  sort(unique(matching$name))
}

#' Retry a Cloud Storage Operation with Exponential Backoff
#'
#' Wraps a function so that transient failures — dropped connections, expired
#' tokens, HTTP 5xx from the storage backend — are retried with a randomised
#' exponential backoff instead of failing the whole pipeline run.
#'
#' @param f The function to wrap.
#' @param max_times Maximum number of attempts. Default `10`.
#' @param pause_cap Longest pause between attempts, in seconds. Default `300`.
#' @param quiet Logical. If `FALSE` (default), each retry is logged at WARN
#'   level.
#'
#' @return A function with the same formals as `f` that retries on error.
#'
#' @details
#' A thin wrapper over [purrr::insistently()] with
#' [purrr::rate_backoff()], centralising the retry policy that the country
#' pipelines previously each maintained by hand.
#'
#' @seealso [insistent_upload_cloud_file()], [insistent_download_cloud_file()]
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' insistent_get <- with_storage_retry(get_trips, max_times = 5)
#' }
with_storage_retry <- function(
  f,
  max_times = 10,
  pause_cap = 300,
  quiet = FALSE
) {
  purrr::insistently(
    f,
    rate = purrr::rate_backoff(pause_cap = pause_cap, max_times = max_times),
    quiet = quiet
  )
}

#' Upload File to Cloud Storage, Retrying on Failure
#'
#' [upload_cloud_file()] wrapped in [with_storage_retry()]. Uploads are the most
#' failure-prone step in the pipelines — large objects, hour-long service-account
#' token windows, and peers that reset connections mid-stream.
#'
#' @inheritParams upload_cloud_file
#' @param max_times Maximum number of attempts. Default `10`.
#' @param pause_cap Longest pause between attempts, in seconds. Default `300`.
#'
#' @return A list of upload metadata, as [upload_cloud_file()].
#'
#' @seealso [upload_cloud_file()], [with_storage_retry()]
#'
#' @keywords storage
#' @export
insistent_upload_cloud_file <- function(
  file,
  provider,
  options,
  name = file,
  max_times = 10,
  pause_cap = 300
) {
  with_storage_retry(
    upload_cloud_file,
    max_times = max_times,
    pause_cap = pause_cap
  )(file = file, provider = provider, options = options, name = name)
}

#' Download File from Cloud Storage, Retrying on Failure
#'
#' [download_cloud_file()] wrapped in [with_storage_retry()].
#'
#' @inheritParams download_cloud_file
#' @param max_times Maximum number of attempts. Default `10`.
#' @param pause_cap Longest pause between attempts, in seconds. Default `300`.
#'
#' @return A character vector of local file paths, as [download_cloud_file()].
#'
#' @seealso [download_cloud_file()], [with_storage_retry()]
#'
#' @keywords storage
#' @export
insistent_download_cloud_file <- function(
  name,
  provider,
  options,
  file = name,
  max_times = 10,
  pause_cap = 300
) {
  with_storage_retry(
    download_cloud_file,
    max_times = max_times,
    pause_cap = pause_cap
  )(name = name, provider = provider, options = options, file = file)
}

#' Authenticate to a Cloud Storage Provider
#'
#' Establishes authentication with the specified cloud provider. Skips
#' re-authentication if a valid token already exists.
#'
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of options specific to the cloud provider. For
#'   GCS, must include `service_account_key`.
#' @param force A logical value indicating whether to force re-authentication.
#'   Default is FALSE.
#'
#' @return Invisible NULL (called for side effects).
#'
#' @keywords storage
#' @export
cloud_storage_authenticate <- function(provider, options, force = FALSE) {
  if ("gcs" %in% provider) {
    if (force || isFALSE(googleAuthR::gar_has_token())) {
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
#' @param max_tries Integer. Maximum number of attempts before giving up.
#'   Default is 5. Set to 1 to disable retrying.
#' @param max_url_chars Integer. Longest request URL to build before splitting
#'   an `imeis` filter across several requests. Default `7000`, comfortably
#'   inside the 8192-byte limit that nginx, Apache and most CDNs impose.
#'
#' @return A data frame containing trip details.
#'
#' @details
#' This request covers the entire trip history in a single streaming response,
#' which makes it the largest and most failure-prone call in the pipeline. It is
#' therefore wrapped in [httr2::req_retry()] with `retry_on_failure = TRUE`, so
#' low-level transport failures — notably
#' `Recv failure: Connection reset by peer`, which has failed production runs —
#' are retried with exponential backoff rather than aborting the run. HTTP 429,
#' 500 and 503 are also treated as transient.
#'
#' ## IMEI filters are chunked automatically
#'
#' `imeis` is sent as a comma-separated query parameter, so a long device list
#' becomes a very long URL. At roughly 16 characters per IMEI, ~500 devices is
#' all that fits inside the usual 8192-byte limit, past which the server answers
#' **HTTP 400 Bad Request** — a failure that looks nothing like "your URL is too
#' long". When the list would overflow `max_url_chars`, it is split into as many
#' requests as needed and the results row-bound and deduplicated. Callers see one
#' data frame either way.
#'
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
  tags = NULL,
  max_tries = 5,
  max_url_chars = 7000
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

  # A long `imeis` list overflows the server's URL limit and comes back as an
  # opaque HTTP 400. Split it and recombine rather than letting that happen.
  if (!is.null(imeis)) {
    imeis <- unique(as.character(imeis))
    budget <- max_url_chars - nchar(base_url) - 200L # headroom for other params
    if (budget < 1L) {
      stop(
        "max_url_chars (",
        max_url_chars,
        ") leaves no room for an imeis filter after the ",
        nchar(base_url),
        "-character base URL."
      )
    }

    if (nchar(paste(imeis, collapse = ",")) > budget) {
      per_chunk <- max(1L, budget %/% (max(nchar(imeis)) + 1L))
      chunks <- split(imeis, ceiling(seq_along(imeis) / per_chunk))
      logger::log_info(
        "imeis filter too long for one URL; splitting {length(imeis)} \\
         devices across {length(chunks)} requests"
      )

      return(
        chunks |>
          purrr::map(
            ~ get_trips(
              token = token,
              secret = secret,
              dateFrom = dateFrom,
              dateTo = dateTo,
              imeis = .x,
              deviceInfo = deviceInfo,
              withLastSeen = withLastSeen,
              tags = tags,
              max_tries = max_tries,
              max_url_chars = max_url_chars
            )
          ) |>
          dplyr::bind_rows() |>
          dplyr::distinct()
      )
    }
  }

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
    httr2::req_url_query(!!!query_params) %>%
    # This pulls the whole trip history in one streaming response; a peer reset
    # part-way through must not fail the run.
    httr2::req_retry(
      max_tries = max_tries,
      retry_on_failure = TRUE,
      is_transient = function(resp) {
        httr2::resp_status(resp) %in% c(429, 500, 502, 503, 504)
      }
    )

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
#' @param max_tries Integer. Maximum number of attempts before giving up.
#'   Default is 5. Set to 1 to disable retrying. Transport failures and HTTP
#'   429/5xx are retried with exponential backoff via [httr2::req_retry()].
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
  overwrite = TRUE,
  max_tries = 5
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
    httr2::req_url_query(!!!query_params) %>%
    httr2::req_retry(
      max_tries = max_tries,
      retry_on_failure = TRUE,
      is_transient = function(resp) {
        httr2::resp_status(resp) %in% c(429, 500, 502, 503, 504)
      }
    )

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
