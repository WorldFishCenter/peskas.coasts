#' Export Geospatial Data and Regional Metrics to MongoDB
#'
#' @description
#' Downloads and processes geospatial data from two sources (Kenya and Zanzibar),
#' harmonizes their structures, applies currency conversions to regional time series
#' metrics, and exports all data to MongoDB collections with appropriate indexing.
#'
#' @details
#' This function performs several key operations:
#'
#' **Geospatial Data Processing:**
#' 1. Downloads GeoJSON files for Kenya and Zanzibar regions from cloud storage
#' 2. Reads and combines the regional boundary data into a single dataset
#' 3. Uploads combined geospatial data to MongoDB with 2dsphere indexing for spatial queries
#'
#' **Regional Metrics Processing:**
#' 4. Downloads monthly summary parquet files for both countries from cloud storage
#' 5. Applies currency conversion factors to monetary metrics:
#'    - Zanzibar: multiplies values by 0.00037 (TZS to USD conversion)
#'    - Kenya: multiplies values by 0.0077 (KES to USD conversion)
#' 6. Converts the following monetary fields: mean_rpue, mean_rpua, mean_price_kg
#' 7. Uploads regional metrics to MongoDB without geospatial indexing
#'
#' **PDS Track Grid Processing:**
#' 8. Downloads PDS track grid summaries from cloud storage
#' 9. Uploads grid summaries to a separate MongoDB collection
#'
#' The function requires appropriate configuration parameters for cloud storage access
#' and MongoDB connection details, typically loaded via `read_config()`.
#'
#' @return None (invisible). Creates and uploads data to three MongoDB collections:
#'   - Regional boundary geometries (with 2dsphere index)
#'   - Regional time series metrics (currency-converted)
#'   - PDS track grid summaries
#'
#' @examples
#' \dontrun{
#' # Export all geospatial data and metrics to MongoDB
#' export_geos()
#' }
#'
#' @importFrom logger log_info log_success
#' @importFrom purrr map walk set_names
#' @importFrom dplyr select mutate bind_rows case_when %>%
#' @importFrom sf read_sf
#' @importFrom arrow read_parquet
#'
#' @seealso
#' \code{\link{mdb_collection_push}} for MongoDB upload functionality
#' \code{\link{download_cloud_file}} for cloud storage operations
#' \code{\link{read_config}} for configuration management
#'
#' @keywords database export geospatial mongodb timeseries
#' @export
export_geos <- function() {
  # Load configuration settings
  pars <- read_config()
  logger::log_info("Loading geospatial data from cloud storage...")

  # Step 1: Download and read geospatial files from cloud storage
  maps <-
    c("KE_regions", "ZAN_regions") |>
    purrr::set_names() |>
    purrr::map(
      ~ cloud_object_name(
        prefix = .x,
        provider = pars$storage$google$key,
        options = pars$storage$google$options,
        extension = "geojson",
        version = "latest"
      )
    ) |>
    purrr::walk(
      ~ download_cloud_file(
        name = .x,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
    ) |>
    purrr::map(
      ~ sf::read_sf(.x)
    ) |>
    dplyr::bind_rows()

  # Step 2: Download and read time series data files from cloud storage
  series <-
    c("kenya_monthly_summaries_map", "zanzibar_monthly_summaries_map") |>
    purrr::set_names() |>
    purrr::map(
      ~ cloud_object_name(
        prefix = .x,
        provider = pars$storage$google$key,
        options = pars$storage$google$options,
        extension = "parquet",
        version = "latest"
      )
    ) |>
    purrr::walk(
      ~ download_cloud_file(
        name = .x,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
    ) |>
    purrr::map(
      ~ arrow::read_parquet(.x)
    ) |>
    dplyr::bind_rows() |>
    dplyr::mutate(
      mean_rpue = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_rpue * 0.00037,
        country == "kenya" ~ .data$mean_rpue * 0.0077
      ),
      mean_rpua = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_rpua * 0.00037,
        country == "kenya" ~ .data$mean_rpua * 0.0077
      ),
      mean_price_kg = dplyr::case_when(
        country == "zanzibar" ~ .data$mean_price_kg * 0.00037,
        country == "kenya" ~ .data$mean_price_kg * 0.0077
      )
    )

  # Step 6: Push combined geospatial data to MongoDB with 2dsphere indexing
  logger::log_info("Pushing combined geospatial data to MongoDB...")
  mdb_collection_push(
    data = maps,
    connection_string = pars$storage$mongodb$connection_string,
    collection_name = pars$storage$mongodb$collection$wio_map,
    db_name = pars$storage$mongodb$database_name,
    geo = TRUE # Create 2dsphere index on geometry field
  )

  logger::log_info("Pushing regional ime series metrics to MongoDB...")
  mdb_collection_push(
    data = series,
    connection_string = pars$storage$mongodb$connection_string,
    collection_name = pars$storage$mongodb$collection$regional_metrics,
    db_name = pars$storage$mongodb$database_name,
    geo = FALSE # Create 2dsphere index on geometry field
  )

  # Step 7: Download and process PDS track grid summaries
  logger::log_info("Downloading PDS track grid summaries...")
  grid_summaries <-
    download_parquet_from_cloud(
      prefix = "pds-tracks-grid_summaries",
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )

  # Step 8: Push grid summaries to MongoDB (without geospatial indexing)
  logger::log_info("Pushing PDS track grid summaries to MongoDB...")
  mdb_collection_push(
    data = grid_summaries,
    connection_string = pars$storage$mongodb$connection_string,
    collection_name = pars$storage$mongodb$collection$pds_grids,
    db_name = pars$storage$mongodb$database_name,
    geo = FALSE # No geospatial indexing needed
  )

  logger::log_success("Successfully exported geospatial data to MongoDB")
  return(invisible(NULL))
}
