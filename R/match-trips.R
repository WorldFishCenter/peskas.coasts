#' Match GPS Trips with Survey Data
#'
#' @description
#' Downloads matched GPS and survey trip data from regional buckets
#' (Kenya, Mozambique, Zanzibar), filters valid records, and harmonizes
#' columns into a single combined dataset.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'
#' @return A data frame of combined trip records across all regions with
#'   harmonized columns including country, submission_id, pds_trip,
#'   landing_date, and catch information.
#'
#' @examples
#' \dontrun{
#' merged_data <- merge_survey_trips()
#' }
#'
#' @keywords workflow
#' @export
merge_survey_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  countries <- c("kenya", "mozambique", "zanzibar")

  prefixes <- c(
    conf$surveys$kenya$kefs$merged,
    conf$surveys$mozambique$adnap$merged,
    conf$surveys$zanzibar$wf$merged
  )

  buckets <- c(
    conf$storage$google$buckets$kenya,
    conf$storage$google$buckets$mozambique,
    conf$storage$google$buckets$zanzibar
  )

  col_specs <- list(
    c(
      "submission_id",
      pds_trip = "trip",
      "landing_date",
      "gaul_1_name",
      "gaul_2_name",
      alpha3_code = "sample_alpha3_code",
      "sample_weight",
      "sample_price",
      "total_catch_weight",
      "total_catch_price"
    ),
    c(
      "submission_id",
      pds_trip = "trip",
      "landing_date",
      "gaul_1_name",
      "gaul_2_name",
      "alpha3_code",
      "catch_kg",
      "catch_price"
    ),
    c(
      "submission_id",
      pds_trip = "trip",
      "landing_date",
      alpha3_code = "catch_taxon",
      "catch_kg",
      "catch_price"
    )
  )

  logger::log_info("Downloading merged trip data from regional buckets...")
  merged_trips <-
    purrr::map2(
      prefixes,
      buckets,
      ~ download_parquet_from_cloud(
        prefix = .x,
        provider = conf$storage$google$key,
        options = conf$storage$google$options,
        bucket_name = .y
      )
    ) |>
    rlang::set_names(countries) |>
    purrr::map(
      ~ dplyr::filter(.x, !is.na(.data$submission_id) & !is.na(.data$trip))
    ) |>
    purrr::map(
      ~ dplyr::mutate(.x, submission_id = as.character(.data$submission_id))
    )

  logger::log_info("Harmonizing columns across regions...")
  matched_trips <- purrr::map2(merged_trips, col_specs, select_trip_columns) |>
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
