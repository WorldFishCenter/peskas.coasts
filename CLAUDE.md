# peskas.coasts

R package `coasts`, the shared hub of Peskas: storage/Mongo/KoBo/Airtable helpers used by
every country pipeline, PDS ingestion, the WIO regional portal data (grids, GAUL maps,
regional metrics) and the tracks-app data. It also runs its own pipeline for the regional
products.
Ecosystem context (other repos, data flow, cross-repo contracts): see PESKAS.md, loaded via CLAUDE.local.md.

## Commands
```r
devtools::load_all()
devtools::test()        # tests/testthat
devtools::check()
devtools::document()    # after any roxygen change; man/ is committed
```
- Pipeline steps: read `.github/workflows/data-pipeline.yaml` (one `Rscript -e 'coasts::fn()'` per job).
- Other workflows: `sync-devices-users.yaml` (Airtable devices to tracks-app users),
  `tracks-backup.yaml`, `app-usage-report.yaml`, `release.yaml` (GitHub release from the
  top block of `NEWS.md` on push to `main`).
- CI image: `Dockerfile.prod` (rocker/geospatial base).

## Architecture
- **Hub role.** Country pipelines call coasts as `coasts::fn(..., package = "peskas.<country>.data.pipeline")`.
  `package` goes to `read_config(package =)`, which loads that package's `inst/conf.yml` or
  `inst/config.yml`, so the same function runs against the country's buckets and databases.
  Any exported function with a `package` argument is a cross-repo API.
- **Where things live.** PDS ingestion (`ingest_pds_trips`, `ingest_pds_tracks`,
  `select_country_trips`, `backup_tracks`) is in `R/ingestion-pds.R`. `R/ingestion.R` holds
  `get_kobo_data` and `ingest_assets`. `export_portal`, `export_geos` and
  `export_fishers_stats` are in `R/export.R`; `sync_device_users` is in `R/airtable.R`.
- **Portal.** `export_portal` runs once per country, called from each country pipeline with
  that country's config (`conf$country`), and pushes the `summarize_data()` tables to the
  country's `portal-*` database (collections and the FishBase-traits dependency: PESKAS.md
  "Portal collections" and "FishBase traits"). `export_geos` in the coasts pipeline reads the
  per-country `<country>_monthly_summaries_map` files (hard-coded list: kenya, zanzibar,
  mozambique, timor) and pushes the combined `wio_gaul*`, `metrics_gaul*` and `pds_grids`
  collections.
- **`api.trips`** in `inst/conf.yml` (`peskas-api-dev` / `peskas-api-prod`) lists each
  country's `raw`/`validated` paths in the peskas-api bucket. `merge_survey_trips()`
  (`R/match-trips.R`) reads every country's validated trips from there, plus the
  `surveys.*` merged files from country buckets.
- **`pds.customers` / `select_by`** in `inst/conf.yml` pick the trips coasts ingests for its
  own WIO-wide products, covering every country at once. Country pipelines carry their own
  `pds` block. `?select_country_trips` explains the device/community/exclude rules.
- **Tracks app.** `storage.mongodb.tracks_app` collections (`users`, `catch-events`,
  `fishers-stats`, `fishers-performance`) are read by `tracks-explorer` (`api/fisher-stats/`,
  `api/waypoints.js`). `sync_device_users` writes `users` (with the generated login
  passwords); `export_fishers_stats` writes stats and performance.
- **Currency.** `export_geos` converts RPUE and price to USD with hard-coded rates in
  `R/export.R` (TZS 0.00037, KES 0.0077, MZN 0.016).
- `mdb_collection_push(geo = TRUE)` drops the collection, reinserts, and builds a
  `geometry` 2dsphere index.

## Rules
- Ship a breaking change to anything a pipeline calls (signature, config keys read, output
  columns) as a coasts release first (bump DESCRIPTION, add the `NEWS.md` block), then bump
  each pipeline. Grep `../peskas.*.data.pipeline` for callers before changing a signature.
- When a function starts reading a new config key, add it to every calling country's
  `inst/config.yml` too (PESKAS.md: coasts reads country config).
- Add a name to `pds.customers` only when all its devices belong to one country; one entry
  widens the filter for the whole regional product.
- Adding a country to the regional portal means editing the hard-coded list in
  `export_geos` and, if it reports in local currency, the conversion rates.

## Gotchas
- `read_config()` stops if the calling package ships neither `inst/conf.yml` nor `inst/config.yml`.
- `select_country_trips` errors on zero kept trips on purpose: an empty parquet would become
  `latest` and empty the portal. Do not soften it to a warning.
- Before switching any config to `select_by: community`, run both rules over one trip table
  and compare counts: communities come from current device records, so a site with no device
  left drops its trips.
- Renaming a tracks-app collection or field breaks `tracks-explorer`; renaming portal
  collections breaks `peskas.dashboard` (see PESKAS.md).
- Stray `*.rds`/`*.parquet` files in the repo root are local downloads (gitignored); do not
  rely on them.
- `export_frame_data()` is run by hand when a new census arrives (every few years), not in a
  workflow. Its input is broken since Aug 2026: the `frame` table in the assets snapshot has no
  `country` column (only GAUL codes; `geo$country` holds Airtable record ids). Fix that before
  the next run; the coasts portal serves the May 2026 `frame-gears.json` until then.
