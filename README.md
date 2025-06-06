
<!-- README.md is generated from README.Rmd. Please edit that file -->

# coasts

<!-- badges: start -->

<!-- badges: end -->

The `coasts` package provides a comprehensive data pipeline for
processing and analyzing coastal fisheries data from the Western Indian
Ocean (WIO) region. It integrates GPS tracking data from Pelagic Data
Systems (PDS), regional fisheries metadata, and geospatial information
to support fisheries research and management.

## Overview

This package is designed to handle the complete workflow for coastal
fisheries data processing:

1.  **Data Ingestion**: Automated retrieval of GPS boat tracking data
    from Pelagic Data Systems
2.  **Data Preprocessing**: Spatial gridding and summarization of
    fishing activity patterns  
3.  **Data Export**: Integration with MongoDB for data storage and
    geospatial analysis
4.  **Metadata Management**: Handling of device information and regional
    boundaries

The package supports data from Kenya and Zanzibar fisheries, with
built-in currency conversion and regional harmonization capabilities.

## Key Features

- **GPS Track Processing**: Ingest and preprocess boat GPS tracks from
  PDS API
- **Spatial Analysis**: Grid-based summarization of fishing activity at
  multiple scales (100m-1km)
- **Cloud Storage Integration**: Seamless upload/download from Google
  Cloud Storage
- **MongoDB Integration**: Geospatial data storage with 2dsphere
  indexing
- **Parallel Processing**: Efficient handling of large datasets using
  parallel computation
- **Automated Pipeline**: GitHub Actions workflow for continuous data
  processing

## Installation

You can install the development version of coasts from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("WorldFishCenter/peskas.coasts")
```

## Configuration

The package requires a `conf.yml` configuration file with the following
structure:

``` yaml
pds:
  token: "your_pds_token"
  secret: "your_pds_secret"
  pds_trips:
    file_prefix: "pds_trips"
    version: "latest"
  pds_tracks:
    file_prefix: "pds-tracks"
    version: "latest"

storage:
  google:
    key: "google"
    options:
      project: "your-project-id"
      bucket: "your-bucket-name"
      service_account_key: "path/to/service-account.json"
  mongodb:
    connection_string: "mongodb://connection-string"
    database_name: "your_database"
    collection:
      wio_map: "regional_boundaries"
      regional_metrics: "time_series_data"
      pds_grids: "fishing_grids"

metadata:
  google_sheet_id: "your-google-sheet-id"
```

## Main Functions

### Data Ingestion

``` r
library(coasts)

# Ingest GPS trip data from PDS
ingest_pds_trips()

# Ingest detailed GPS track data
ingest_pds_tracks()
```

### Data Preprocessing

``` r
# Preprocess tracks into spatial grids
preprocess_pds_tracks(grid_size = 500)  # 500m grid cells

# Available grid sizes: 100, 250, 500, 1000 meters
preprocess_pds_tracks(grid_size = 1000)  # 1km grid cells
```

### Data Export

``` r
# Export processed data to MongoDB
export_geos()
```

### Metadata Management

``` r
# Get device metadata from Google Sheets
devices <- get_metadata(table = "devices")

# Get all metadata tables
all_metadata <- get_metadata()
```

## Data Pipeline Workflow

The package implements an automated data pipeline that runs every 2 days
via GitHub Actions:

1.  **Build Container**: Creates a Docker container with R and all
    dependencies
2.  **Ingest PDS Trips**: Downloads trip metadata from PDS API
3.  **Ingest PDS Tracks**: Downloads detailed GPS tracks for each trip
4.  **Preprocess Tracks**: Creates spatial grid summaries of fishing
    activity
5.  **Export Data**: Uploads processed data to MongoDB with geospatial
    indexing

## Data Products

The pipeline produces several key data products:

- **Trip Data**: Basic trip information (start/end times, vessel info)
- **Track Data**: Detailed GPS points with speed, heading, and temporal
  information
- **Grid Summaries**: Spatial aggregations showing:
  - Time spent fishing in each grid cell
  - Average speed and vessel metrics
  - Temporal patterns of activity
- **Regional Metrics**: Time series data with currency-converted
  economic indicators

## Spatial Analysis Capabilities

The package supports multi-scale spatial analysis:

``` r
# Fine-scale analysis (100m grids)
preprocess_pds_tracks(grid_size = 100)

# Broad-scale patterns (1km grids)  
preprocess_pds_tracks(grid_size = 1000)
```

Grid summaries include: - `time_spent_mins`: Total fishing time per grid
cell - `mean_speed`: Average vessel speed - `n_points`: Number of GPS
observations - `first_seen`/`last_seen`: Temporal extent of activity

## Cloud Storage Integration

The package seamlessly integrates with Google Cloud Storage:

``` r
# Upload processed data
upload_cloud_file(
  file = "processed_data.parquet",
  provider = "google",
  options = list(bucket = "your-bucket")
)

# Download data for analysis
download_cloud_file(
  name = "pds_trips_v1.0.0.parquet",
  provider = "google", 
  options = list(bucket = "your-bucket")
)
```

## MongoDB Integration

Geospatial data is stored in MongoDB with appropriate indexing:

``` r
# Push data with geospatial indexing
mdb_collection_push(
  data = spatial_data,
  connection_string = "mongodb://...",
  collection_name = "fishing_areas",
  geo = TRUE  # Creates 2dsphere index
)
```

## Contributing

This package is part of the WorldFish Center’s Peskas initiative for
small-scale fisheries monitoring. Contributions are welcome via GitHub
issues and pull requests.

## License

GPL (\>= 3)
