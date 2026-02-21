# coasts 1.5.0

## New Features

### Survey & Fleet Analysis Pipeline
* **NEW** `summarize_data()` - End-to-end summarization of WorldFish survey data into five output tables (monthly, taxa, district, gear, grid summaries) uploaded to cloud storage as versioned parquet files
* **NEW** `calculate_fishery_metrics()` - Transforms catch-level records into normalized fishery indicators (site-level CPUE/RPUE, predominant gear, species composition) in long format for portal consumption
* **NEW** `generate_fleet_analysis()` - Orchestrates full fleet activity estimation pipeline and uploads aggregated results to cloud storage
* **NEW** `prepare_boat_registry()` - Constructs a boat registry from asset metadata for scaling GPS-tracked data to fleet-wide estimates
* **NEW** `process_trip_data()` - Processes PDS API trip records by device IMEI into per-trip summaries
* **NEW** `calculate_monthly_trip_stats()` - Aggregates trip data to monthly statistics per district
* **NEW** `estimate_fleet_activity()` - Scales GPS-sampled trips to fleet-wide activity estimates using boat registry sampling rates
* **NEW** `calculate_district_totals()` - Joins fleet estimates with survey summaries to produce district-level catch and revenue totals

### Data Export
* **NEW** `export_portal()` - Downloads WorldFish summary datasets from cloud storage, joins modelled aggregate estimates, pivots to long format, and uploads all tables to MongoDB portal collections

## Enhancements

### Multi-Package Architecture
* **ENHANCED** `read_config()` - Added `package` argument (default `"coasts"`). Downstream packages that ship their own `inst/conf.yml` can now call `read_config(package = "mypackage")` to load their own configuration instead of the `coasts` defaults
* **ENHANCED** All 12 top-level pipeline functions now accept a `package` argument threaded through to `read_config()`: `ingest_pds_trips()`, `ingest_pds_tracks()`, `backup_tracks()`, `ingest_assets()`, `preprocess_pds_tracks()`, `merge_survey_trips()`, `get_metadata()`, `summarize_data()`, `export_geos()`, `export_fishers_stats()`, `export_portal()`, `generate_fleet_analysis()`

### Automated Workflows
* **ENHANCED** `app-usage-report.yaml`, `sync-devices-users.yaml`, `tracks-backup.yaml` - All jobs now carry an explicit `if: github.ref == 'refs/heads/main'` guard, ensuring workflows triggered via `workflow_dispatch` on non-main branches are safely skipped

## Package Infrastructure
* **ENHANCED** `DESCRIPTION` - Migrated `Author`/`Maintainer` fields to `Authors@R: person(...)` format (fixes R CMD check WARNING); removed spurious `LazyData: true` (no `data/` directory); added `URL` and `BugReports` fields pointing to the GitHub repository
* **ENHANCED** `_pkgdown.yml` - Added new "Survey & Fleet Analysis" reference section; added `export_portal()` to "Data Export & Storage"; removed two non-exported internal helpers that would have caused build errors

# coasts 1.4.0

## New Features

### GPS-Survey Trip Matching
* **NEW** `merge_survey_trips()` - Downloads matched GPS and survey trip data across regions, harmonizes columns, and combines into a single dataset

### Automated Workflows
* **ENHANCED** GitHub Actions data pipeline with new `match-trips` job


### Multi-Bucket Regional Storage
* **ENHANCED** `download_parquet_from_cloud()` and `upload_parquet_to_cloud()` - Added `bucket_name` parameter to download/upload from regional buckets (Kenya, Mozambique, Zanzibar)
* **ENHANCED** `inst/conf.yml` - Regional bucket configuration with environment-specific bucket names (dev vs prod)

## Code Organization
* Refactored PDS ingestion and API functions into dedicated files (`R/ingestion-pds.R`, `R/pds-api.R`)
* Updated pkgdown reference index with tracks app and preprocessing sections

# coasts 1.3.0


### Fisher Performance Analytics
* **NEW** `export_fishers_stats()` - Comprehensive fisher performance analysis and export
  - Integrates catch events from tracks-app with GPS tracking data from PDS API
  - Matches fisher-reported landings with automated trip tracking by date and device
  - Calculates fishing efficiency metrics: CPUE (kg/hour, kg/km), search efficiency ratios
  - Estimates fuel consumption and catch per liter efficiency
  - Categorizes trips by distance (nearshore, mid-range, offshore)
  - Exports aggregated fisher statistics and trip-level performance metrics to MongoDB

### Automated Workflows
* **ENHANCED** GitHub Actions data pipeline workflow
  - Added `export-fishers-stats` job to automated pipeline
  - Runs after track preprocessing to ensure data availability
  - Automatically exports fisher performance data on every pipeline run

### Development Experience
* **NEW** `.Rprofile` - Interactive environment switching for local development
  - Added helper functions: `use_prod()`, `use_local()`, `use_default()`
  - Visual environment indicator on R session startup
  - Quick commands reference displayed in interactive sessions
  - Simplified testing across different configuration profiles

## Configuration Updates

### MongoDB Collections
* **ENHANCED** tracks-app MongoDB configuration in `inst/conf.yml`
  - Added `fishers-stats` collection for aggregated fisher summaries
  - Added `fishers-performance` collection for trip-level efficiency metrics
  - Improved data organization for analytics and reporting

# coasts 1.2.0

## Breaking Changes

### Configuration System Migration
* **BREAKING CHANGE** - Migrated from auth folder to `.env`-based credentials management
  - Removed `local` configuration profile from `inst/conf.yml`
  - All environments now use environment variables loaded via `.env` file in local development
  - Added `dotenv` package dependency for automatic `.env` file loading
  - Created `.env.example` template with all required environment variables
  - Updated `.gitignore` to properly handle `.env` files while tracking `.env.example`
  - **Migration Guide**: Copy `.env.example` to `.env` and fill in credentials (see updated README)

## New Features

### Asset Management
* **ENHANCED** `ingest_assets()` - Comprehensive fisheries asset metadata ingestion
  - Added `log_threshold` parameter for configurable logging
  - Now includes PDS device metadata from Airtable (`pds_devices` table)
  - Retrieves 6 asset types: taxa, gear, vessels, landing sites, forms, and devices
  - Changed output format from parquet to RDS for better R object serialization
  - Added complete roxygen documentation following package standards

### Data Ingestion Improvements
* **ENHANCED** `ingest_pds_trips()` - Improved trip data ingestion workflow
  - Now downloads device metadata from cloud storage instead of Google Sheets
  - Filters devices by `last_seen` date (>= 2023-01-01) for active devices only
  - Enhanced PDS API calls with `deviceInfo` and `withLastSeen` parameters
  - Client-side IMEI filtering for reliable data retrieval
  - Updated documentation with detailed configuration examples and notes

## Documentation

### Package Documentation
* **ENHANCED** README with `.env`-based configuration instructions
  - Added step-by-step local development setup guide
  - Documented all required environment variables with descriptions
  - Separated local and production deployment instructions
* **UPDATED** CLAUDE.md with new configuration system details
  - Revised Configuration System section to explain `.env` approach
  - Updated Configuration Requirements with clear setup steps
  - Added `dotenv` to Key Dependencies section
* **NEW** `.env.example` - Template file for local development credentials
  - Includes all 12 required environment variables with helpful comments
  - Proper formatting examples for complex values (JSON keys, connection strings)

### Function Documentation
* **ENHANCED** `ingest_assets()` with comprehensive roxygen documentation
  - Detailed @description with step-by-step operations
  - Complete @details section with YAML configuration structure
  - Asset type descriptions (Taxa, Gear, Vessels, Landing Sites, Forms, Devices)
  - Added @examples, @seealso, and @keywords tags

## Technical Improvements

### Configuration Loading
* **ENHANCED** `read_config()` function in `R/utils.R`
  - Automatic detection and loading of `.env` file if present
  - Seamless integration with existing `config::get()` workflow
  - Informative logging when `.env` file is loaded

### Development Experience
* Simplified credentials management for local development
* Consistent approach with other peskas packages (e.g., peskas.kenya.data.pipeline)
* Improved security with proper `.gitignore` configuration
* Easier onboarding for new developers with template file

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
