# coasts 1.1.0

* **NEW** - Integrate (Beta) Cabo Delgado (Mozambique) estimates
* **NEW** - Ddeveloping code to integrate catch events records from tracks-app

# coasts 1.0.0

## Major New Features

### Airtable Integration System
* **NEW** `airtable_to_df()` - Convert Airtable tables to R data frames with pagination support
* **NEW** `df_to_airtable()` - Create new records in Airtable tables with batch processing  
* **NEW** `bulk_update_airtable()` - Update multiple Airtable records efficiently (10 record batches)
* **NEW** `update_airtable_record()` - Update individual Airtable records
* **NEW** `get_writable_fields()` - Identify writable fields in Airtable tables (excludes computed fields)
* **NEW** `device_sync()` - Comprehensive sync function for device data (updates existing, creates new)
* **NEW** `ingest_pelagic_boats()` - Complete workflow for PDS boat data ingestion and Airtable sync
* **NEW** `sync_device_users()` - Sync device users to MongoDB with password generation and Airtable updates

### Enhanced PDS API Integration
* **NEW** `pelagic_auth()` - Authentication with Pelagic Analytics API
* **NEW** `pelagic_refresh_token()` - Token refresh functionality for sustained API access
* **NEW** `get_pelagic_boats()` - Retrieve boat information with server-side filtering and column selection
* **NEW** `get_pelagic_devices()` - Retrieve device information with advanced filtering capabilities
* Enhanced `ingest_pds_tracks()` with improved error handling and parallel processing

### Automated Workflows
* **NEW** GitHub Actions workflow: `ingest-pelagic-boats.yaml` (runs every 15 days)
* **NEW** GitHub Actions workflow: `sync-device-users.yaml` (runs every 10 days)
* Enhanced main data pipeline workflow with improved container management

### Configuration System Improvements
* **BREAKING CHANGE** Restructured MongoDB configuration to support dual databases:
  - `mongodb.coasts_portal` - For main coasts geospatial data
  - `mongodb.tracks_app` - For tracks application user data
* **BREAKING CHANGE** Enhanced Airtable configuration with separate base IDs:
  - `airtable.frame` - For device and country metadata
  - `airtable.tracks_app` - For user management
* Updated environment variable requirements for production deployments

### Documentation and Development
* **NEW** Professional pkgdown website with enhanced theming and navigation
* Enhanced README with status badges and improved structure
* Fixed pkgdown configuration issues with pipe operators and tidy evaluation functions
* Updated function documentation with detailed examples and use cases

## Bug Fixes and Improvements

### Data Processing
* Fixed KES to USD conversion units in `export_geos()`
* Improved MongoDB collection references to use new dual-database configuration
* Enhanced error handling in data ingestion functions
* Better logging and progress tracking across all functions

### API and Authentication
* Robust token refresh mechanisms for long-running processes
* Improved error messages for authentication failures
* Server-side filtering for PDS API calls to reduce data transfer

### Workflow and Deployment
* Streamlined Docker image build process with better caching
* Enhanced GitHub Actions workflows with proper credential management
* Improved container registry integration

## Technical Improvements
* Password generation system for new users with reproducible seeding
* Comprehensive data validation and duplicate handling
* Enhanced country mapping for global fisheries data (13 countries supported)
* Improved spatial data processing with WGS84 coordinate system standardization
* Advanced MongoDB operations with geospatial indexing (2dsphere)

## Geographic Coverage Expansion
* Enhanced support for multi-country deployments
* Improved regional data harmonization
* Currency conversion support for multiple regions (KES, TZS to USD)

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
