#' Merge GPS Trips with Survey Data
#'
#' @description
#' Combines validated API trip data with GPS-matched survey records across
#' all regions and uploads the result to cloud storage.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'
#' @return Invisible NULL. Uploads merged trip data to cloud storage.
#'
#' @examples
#' \dontrun{
#' merged_data <- merge_survey_trips()
#' }
#'
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @keywords workflow export
#' @export
merge_survey_trips <- function(
  log_threshold = logger::DEBUG,
  package = "coasts"
) {
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  countries <- c("kenya", "mozambique", "zanzibar")

  api_prefixes <- c(
    paste0(
      conf$api$trips$kenya$validated$cloud_path,
      "/",
      conf$api$trips$kenya$validated$file_prefix
    ),
    paste0(
      conf$api$trips$mozambique$validated$cloud_path,
      "/",
      conf$api$trips$mozambique$validated$file_prefix
    ),
    paste0(
      conf$api$trips$zanzibar$validated$cloud_path,
      "/",
      conf$api$trips$zanzibar$validated$file_prefix
    )
  )

  merged_prefixes <- c(
    conf$surveys$kenya$merged,
    conf$surveys$mozambique$adnap$merged,
    conf$surveys$zanzibar$wf$merged
  )

  merged_buckets <- c(
    conf$storage$google$buckets$kenya,
    conf$storage$google$buckets$mozambique,
    conf$storage$google$buckets$zanzibar
  )
  # TO DO:ensure all have pds trip start/end/distance
  logger::log_info("Downloading merged trip data from regional buckets...")
  merged_trips <-
    purrr::map2(
      merged_prefixes,
      merged_buckets,
      ~ download_parquet_from_cloud(
        prefix = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options,
        bucket_name = .y
      )
    ) |>
    rlang::set_names(countries) |>
    purrr::map(
      ~ .x |>
        dplyr::select(
          "submission_id",
          pds_trip = "trip"
        ) |>
        dplyr::mutate(
          submission_id = as.character(.data$submission_id),
          pds_trip = as.character(.data$pds_trip)
        ) |>
        dplyr::distinct()
    )

  api_trips <-
    purrr::map(
      api_prefixes,
      ~ download_parquet_from_cloud(
        prefix = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options,
        bucket_name = conf$api$trips$bucket
      )
    ) |>
    rlang::set_names(countries) |>
    purrr::map(
      ~ .x |>
        dplyr::rename(submission_id = "trip_id") |>
        dplyr::mutate(
          submission_id = stringr::str_replace(.data$submission_id, "TRIP_", "")
        )
    )

  matched_trips <-
    purrr::map2(
      api_trips,
      merged_trips,
      ~ dplyr::left_join(.x, .y, by = "submission_id")
    ) |>
    purrr::map(
      ~ .x |>
        dplyr::filter(!is.na(.data$submission_id) & !is.na(.data$pds_trip)) |>
        dplyr::relocate("pds_trip", .after = "submission_id")
    ) |>
    dplyr::bind_rows(.id = "country")

  upload_parquet_to_cloud(
    data = matched_trips,
    prefix = conf$trips$matched,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Select and rename trip columns
#'
#' @param data A data frame of trip records.
#' @param col_spec A named character vector for column selection and renaming.
#'
#' @return A data frame with selected columns and duplicates removed.
#'
#' @keywords internal
select_trip_columns <- function(data, col_spec) {
  data |>
    dplyr::select(!!!col_spec) |>
    dplyr::distinct()
}
