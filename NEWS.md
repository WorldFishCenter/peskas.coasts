# coasts 0.1.0

* Initial release of the coastal fisheries data pipeline for Western Indian Ocean region.

## New Features

### Data Ingestion
* `ingest_pds_trips()` - Automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS) API
* `ingest_pds_tracks()` - Parallel processing of detailed GPS track data with batch processing capabilities
* `get_metadata()` - Retrieval of fishery metadata from Google Sheets

### Data Preprocessing  
* `preprocess_pds_tracks()` - Spatial gridding and summarization of fishing activity patterns
* Multi-scale spatial analysis support (100m, 250m, 500m, 1000m grid cells)
* Parallel processing for efficient handling of large datasets
* `preprocess_track_data()` - Core function for converting GPS tracks to spatial grid summaries

### Data Export and Storage
* `export_geos()` - Comprehensive export of geospatial data and regional metrics to MongoDB
* MongoDB integration with 2dsphere geospatial indexing
* Currency conversion for Kenya (KES to USD) and Zanzibar (TZS to USD) economic indicators
* Support for regional boundary data and time series metrics

### Cloud Storage Integration
* `upload_cloud_file()` and `download_cloud_file()` - Google Cloud Storage integration
* `cloud_object_name()` - Versioned object naming and retrieval
* `upload_parquet_to_cloud()` and `download_parquet_from_cloud()` - Optimized parquet file handling
* Automatic file compression using LZ4 algorithm

### Database Operations
* `mdb_collection_push()` and `mdb_collection_pull()` - MongoDB collection management
* Geospatial indexing support for spatial queries
* Bulk data operations with error handling

### API Integration
* `get_trips()` - PDS API integration for trip data retrieval
* `get_trip_points()` - Detailed GPS point data from PDS API
* Authentication and token management for external APIs

### Automation and Workflow
* GitHub Actions workflow for automated data pipeline execution
* Runs every 2 days with complete data processing pipeline
* Docker containerization for reproducible execution environment
* Configuration management through `conf.yml` files

## Geographic Coverage
* Kenya coastal fisheries data processing
* Zanzibar fisheries data integration
* Regional harmonization and standardization

## Technical Features
* Parallel processing using `future` and `furrr` packages
* Efficient data formats using Apache Arrow/Parquet
* Comprehensive logging with configurable thresholds
* Error handling and recovery mechanisms
* Versioned data management system
