#' Spatial CPUE Estimation 
#'
#' Supports Zanzibar, Kenya, and Mozambique.
#'
#' Data sources
#' ------------
#' - conf$trips$matched ("trips-matched"):
#'     Created by merge_survey_trips(). Contains validated Kobo catch records
#'     already linked to PDS trip IDs via submission_id. Authoritative join
#'     between GPS tracks and landings — no fuzzy boat name matching needed.
#'
#' - conf$pds$pds_tracks_predicted ("predicted-pds-tracks"):
#'     Pre-classified fishing-only GPS points (trip, timestamp, lat, lon).
#'     One parquet file per trip. Produced by predict_pds_tracks().
#'
#' Two effort datasets
#' -------------------
#' - effort_all    : ALL predicted trips → used for fishing effort maps
#' - effort_matched: only trips in trips-matched → used for CPUE model
#'
#' CPUE methods
#' ------------
#' method = "weighted" (default, recommended for sparse data):
#'   CPUE_h = sum(catch_kg for trips visiting cell h) /
#'            sum(fishing_hours in cell h)
#'   Direct ratio per cell. Always produces a result. Robust with few trips.
#'
#' method = "nnls" (for denser data, >= ~200 trips):
#'   Solves min ||Xq - y||^2 s.t. q >= 0 across all cells simultaneously.
#'   More statistically rigorous but requires more trips than H3 cells.
#'
#' Usage:
#'   out <- model_cpue("zanzibar", method = "weighted")
#'   out$map_effort   # ALL fishing effort by year
#'   out$map_cpue     # CPUE by species


# ── 0. Dependencies ────────────────────────────────────────────────────────────
library(coasts)
library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)
library(stringr)
library(glue)
library(logger)
library(nnls)
library(h3jsr)
library(leaflet)
library(htmltools)
library(htmlwidgets)
library(sf)
library(tibble)
library(arrow)
library(googleCloudStorageR)


# ── 1. Storage helpers ─────────────────────────────────────────────────────────

#' GCS options for the PDS tracks bucket
#' @keywords internal
pds_storage_opts <- function(conf) {
  list(
    provider = conf$pds_storage$google$key,
    options  = conf$pds_storage$google$options
  )
}


# ── 2. Data loading ────────────────────────────────────────────────────────────

#' Download the trips-matched dataset filtered to a country
#'
#' Contains validated Kobo catch records already linked to PDS trip IDs.
#' Columns: pds_trip, landing_date, catch_taxon, catch_kg, gear, ...
#'
#' @keywords internal
download_matched_trips <- function(country, conf) {
  log_info("Downloading trips-matched for {country} ...")
  
  matched <- download_parquet_from_cloud(
    prefix   = conf$trips$matched,
    provider = conf$storage$google$key,
    options  = conf$storage$google$options
  ) |>
    dplyr::filter(
      .data$country == !!country,
      !is.na(.data$pds_trip),
      !is.na(.data$catch_taxon), .data$catch_taxon != "",
      !is.na(.data$catch_kg),    .data$catch_kg > 0
    ) |>
    dplyr::mutate(
      pds_trip     = as.character(.data$pds_trip),
      landing_date = as.Date(.data$landing_date),
      catch_taxon  = stringr::str_to_title(stringr::str_trim(.data$catch_taxon)),
      catch_kg     = as.numeric(.data$catch_kg)
    )
  
  log_info(
    "Matched trips: {nrow(matched)} catch records |",
    " {n_distinct(matched$pds_trip)} unique PDS trips |",
    " {format(min(matched$landing_date), '%Y-%m-%d')}",
    " -> {format(max(matched$landing_date), '%Y-%m-%d')}"
  )
  matched
}


#' Download predicted tracks — internal helper
#' @keywords internal
.download_tracks <- function(conf, trip_ids = NULL, max_files = NULL,
                             label = "tracks") {
  pds_opts    <- pds_storage_opts(conf)
  file_prefix <- conf$pds$pds_tracks_predicted$file_prefix
  
  gcs_auth(json_file = Sys.getenv("GCP_SA_KEY"))
  
  file_list <- gcs_list_objects(
    bucket = pds_opts$options$bucket,
    prefix = file_prefix
  )
  
  if (is.null(file_list) || nrow(file_list) == 0) {
    stop(
      "No predicted track files found in bucket '",
      pds_opts$options$bucket, "' with prefix '", file_prefix, "'.\n",
      "Run predict_pds_tracks() first."
    )
  }
  
  predicted_ids <- as.numeric(
    stringr::str_extract(file_list$name, "trip_(\\d+)_", group = 1)
  )
  
  if (!is.null(trip_ids)) {
    file_list <- file_list[predicted_ids %in% as.numeric(trip_ids), ]
    log_info(
      "{label}: {nrow(file_list)} files matching trip IDs",
      " (out of {length(predicted_ids)} total)"
    )
  } else {
    log_info("{label}: {nrow(file_list)} files (all predicted trips)")
  }
  
  if (nrow(file_list) == 0) stop("No files to download for {label}.")
  
  if (!is.null(max_files)) {
    file_list <- head(file_list, max_files)
    log_info("TEST MODE: limiting {label} to {max_files} files")
  }
  
  log_info("Downloading {nrow(file_list)} files for {label} ...")
  
  tracks <- purrr::map(
    file_list$name,
    function(obj_name) {
      tmp <- tempfile(fileext = ".parquet")
      on.exit(if (file.exists(tmp)) file.remove(tmp))
      tryCatch({
        gcs_get_object(
          object_name = obj_name,
          bucket      = pds_opts$options$bucket,
          saveToDisk  = tmp,
          overwrite   = TRUE
        )
        arrow::read_parquet(tmp)
      }, error = function(e) {
        log_warn("Skipping {obj_name}: {conditionMessage(e)}")
        NULL
      })
    },
    .progress = TRUE
  ) |>
    purrr::compact() |>
    dplyr::bind_rows()
  
  log_info(
    "{label}: {format(nrow(tracks), big.mark = ',')} fishing points",
    " across {n_distinct(tracks$trip)} trips"
  )
  tracks
}


#' Download ALL predicted tracks (for effort map)
#' @keywords internal
download_all_tracks <- function(conf, max_files = NULL) {
  .download_tracks(conf, trip_ids = NULL, max_files = max_files,
                   label = "All tracks")
}


#' Download predicted tracks filtered to matched trip IDs (for NNLS)
#' @keywords internal
download_matched_tracks <- function(conf, matched, max_files = NULL) {
  .download_tracks(conf,
                   trip_ids  = unique(matched$pds_trip),
                   max_files = max_files,
                   label     = "Matched tracks")
}


# ── 3. Preparation ────────────────────────────────────────────────────────────

#' Add dt_hours and H3 cells to predicted tracks
#' @keywords internal
prepare_tracks <- function(tracks, h3_res) {
  tracks |>
    dplyr::mutate(
      trip      = as.character(trip),
      timestamp = lubridate::as_datetime(timestamp),
      year      = lubridate::year(timestamp)
    ) |>
    dplyr::arrange(trip, timestamp) |>
    dplyr::group_by(trip) |>
    dplyr::mutate(
      dt_hours = as.numeric(difftime(timestamp, dplyr::lag(timestamp), units = "hours")),
      dt_hours = dplyr::coalesce(dt_hours, 0),
      dt_hours = pmin(dt_hours, 4)   # cap gaps > 4h (device off / GPS loss)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      h3_cell = h3jsr::point_to_cell(cbind(longitude, latitude), res = h3_res)
    )
}


#' Aggregate fishing effort per trip x H3 cell
#' @keywords internal
aggregate_effort <- function(tracks, label = "effort") {
  effort <- tracks |>
    dplyr::group_by(trip, h3_cell, year) |>
    dplyr::summarise(
      fishing_hours = sum(dt_hours, na.rm = TRUE),
      .groups       = "drop"
    )
  
  log_info(
    "{label}: {nrow(effort)} rows |",
    " {n_distinct(effort$trip)} trips |",
    " {n_distinct(effort$h3_cell)} H3 cells"
  )
  effort
}


#' Build catch matrix: one row per trip, one col per species
#' @keywords internal
build_catch_wide <- function(matched) {
  log_info("Building catch matrix from trips-matched ...")
  
  catch_wide <- matched |>
    dplyr::group_by(pds_trip, catch_taxon) |>
    dplyr::summarise(catch_kg = sum(catch_kg, na.rm = TRUE), .groups = "drop") |>
    tidyr::pivot_wider(
      id_cols     = pds_trip,
      names_from  = catch_taxon,
      values_from = catch_kg,
      values_fill = 0
    )
  
  log_info(
    "Catch matrix: {nrow(catch_wide)} trips x",
    " {ncol(catch_wide) - 1} species"
  )
  catch_wide
}


# ── 4. Join effort x catch ────────────────────────────────────────────────────

#' Join effort and catch on trip ID (exact join — no date tolerance needed)
#' @keywords internal
join_effort_catch <- function(effort, catch_wide) {
  log_info("Joining effort and catch on trip ID ...")
  
  trips <- effort |>
    dplyr::inner_join(catch_wide, by = c("trip" = "pds_trip"))
  
  log_info(
    "Matched: {nrow(trips)} rows |",
    " {n_distinct(trips$trip)} trips |",
    " {n_distinct(trips$h3_cell)} H3 cells"
  )
  
  if (nrow(trips) == 0) {
    log_warn("No matches — check that predicted tracks cover trips-matched trips.")
  }
  
  trips
}


# ── 5. CPUE models ────────────────────────────────────────────────────────────

#' Identify top N species and shared helper to finalise cpue_df
#' @keywords internal
.top_species <- function(trips, meta_cols, top_n) {
  species_cols <- setdiff(colnames(trips), meta_cols)
  
  species_totals <- trips |>
    dplyr::summarise(dplyr::across(
      dplyr::all_of(species_cols), \(x) sum(x, na.rm = TRUE)
    )) |>
    tidyr::pivot_longer(
      dplyr::everything(),
      names_to  = "species",
      values_to = "total_kg"
    ) |>
    dplyr::filter(total_kg > 0) |>
    dplyr::arrange(dplyr::desc(total_kg))
  
  log_info("Catch totals from matched trips:")
  print(as.data.frame(species_totals))
  
  top_sp <- species_totals |>
    dplyr::slice_head(n = top_n) |>
    dplyr::pull(species)
  
  log_info("Modelling: {paste(top_sp, collapse = ', ')}")
  top_sp
}


#' Attach trip counts and cell centroids, apply min_trips filter
#' @keywords internal
.finalise_cpue <- function(cpue_long, trips, min_trips) {
  trip_counts <- trips |>
    dplyr::group_by(h3_cell) |>
    dplyr::summarise(n_trips = dplyr::n_distinct(trip), .groups = "drop")
  
  centroids <- h3jsr::cell_to_point(unique(cpue_long$h3_cell)) |>
    sf::st_coordinates() |>
    tibble::as_tibble() |>
    dplyr::rename(lon = X, lat = Y) |>
    dplyr::mutate(h3_cell = unique(cpue_long$h3_cell))
  
  cpue_df <- cpue_long |>
    dplyr::left_join(trip_counts, by = "h3_cell") |>
    dplyr::left_join(centroids,   by = "h3_cell") |>
    dplyr::filter(n_trips >= min_trips, cpue > 0)
  
  log_info("CPUE table: {nrow(cpue_df)} rows (min_trips = {min_trips})")
  cpue_df
}


#' Weighted CPUE: catch / effort ratio per H3 cell
#'
#' CPUE_h = sum(catch_kg for trips visiting h) / sum(fishing_hours in h)
#' Direct ratio — robust with sparse data, fills all visited cells.
#'
#' @keywords internal
run_weighted_cpue <- function(trips, top_n, min_trips) {
  log_info("Running weighted CPUE model (catch / effort per cell) ...")
  
  meta_cols   <- c("trip", "h3_cell", "year", "fishing_hours")
  top_species <- .top_species(trips, meta_cols, top_n)
  
  # catch per trip (constant across H3 cell rows for same trip)
  catch_per_trip <- trips |>
    dplyr::group_by(trip) |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(top_species),
                    \(x) dplyr::first(x[!is.na(x)])),
      .groups = "drop"
    )
  
  # for each cell x species: sum catch of all trips visiting that cell,
  # divided by total fishing hours in that cell
  cpue_long <- purrr::map(top_species, function(sp) {
    trips |>
      dplyr::select(trip, h3_cell, fishing_hours) |>
      dplyr::left_join(
        catch_per_trip |> dplyr::select(trip, !!sp),
        by = "trip"
      ) |>
      dplyr::group_by(h3_cell) |>
      dplyr::summarise(
        total_catch   = sum(.data[[sp]],      na.rm = TRUE),
        total_hours   = sum(fishing_hours,    na.rm = TRUE),
        .groups       = "drop"
      ) |>
      dplyr::mutate(
        cpue    = dplyr::if_else(total_hours > 0, total_catch / total_hours, 0),
        species = sp
      ) |>
      dplyr::select(h3_cell, cpue, species)
  }) |>
    dplyr::bind_rows()
  
  log_info(
    "Weighted CPUE: {sum(cpue_long$cpue > 0)} non-zero cell x species combinations"
  )
  
  cpue_df <- .finalise_cpue(cpue_long, trips, min_trips)
  list(cpue = cpue_df, species = top_species)
}


#' NNLS CPUE: spatially deconvolved catch / effort
#'
#' Solves min ||Xq - y||^2 s.t. q >= 0 for each species.
#' More rigorous but requires more trips than H3 cells (overdetermined system).
#' Use when n_trips >> n_h3_cells (rule of thumb: at least 3x more trips).
#'
#' @keywords internal
run_nnls_cpue <- function(trips, top_n, min_trips) {
  log_info("Running NNLS CPUE model ...")
  
  meta_cols   <- c("trip", "h3_cell", "year", "fishing_hours")
  top_species <- .top_species(trips, meta_cols, top_n)
  
  X_wide <- trips |>
    dplyr::distinct(trip, h3_cell, fishing_hours) |>
    tidyr::pivot_wider(
      id_cols     = trip,
      names_from  = h3_cell,
      values_from = fishing_hours,
      values_fn   = sum,
      values_fill = 0
    )
  
  trip_ids <- X_wide$trip
  h3_cells <- setdiff(colnames(X_wide), "trip")
  X        <- as.matrix(X_wide[, h3_cells])
  
  catch_per_trip <- trips |>
    dplyr::group_by(trip) |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(top_species),
                    \(x) dplyr::first(x[!is.na(x)])),
      .groups = "drop"
    ) |>
    dplyr::arrange(match(trip, trip_ids))
  
  Y <- as.matrix(dplyr::select(catch_per_trip, dplyr::all_of(top_species)))
  
  log_info("Design matrix: {nrow(X)} trips x {ncol(X)} H3 cells")
  
  if (ncol(X) >= nrow(X)) {
    log_warn(
      "Under-determined: {ncol(X)} cells >= {nrow(X)} trips.",
      " NNLS will produce sparse results. Consider method = 'weighted'."
    )
  }
  
  cpue_long <- purrr::map(top_species, function(sp) {
    y   <- Y[, sp]
    fit <- nnls::nnls(X, y)
    log_info(
      "  {sp}: residual = {round(sqrt(sum(fit$residuals^2)), 2)},",
      " non-zero cells = {sum(fit$x > 0)}"
    )
    tibble::tibble(h3_cell = h3_cells, cpue = fit$x, species = sp)
  }) |>
    dplyr::bind_rows()
  
  cpue_df <- .finalise_cpue(cpue_long, trips, min_trips)
  list(cpue = cpue_df, species = top_species)
}


# ── 6. Maps ───────────────────────────────────────────────────────────────────

#' Build H3 polygon sf from a vector of H3 cell IDs
#' @keywords internal
h3_cells_to_sf <- function(h3_cells) {
  h3jsr::cell_to_polygon(h3_cells) |>
    sf::st_sf(h3_cell = h3_cells, geometry = _)
}


#' Fishing effort map (ALL trips) per year on dark basemap with year toggle
#' @export
plot_effort_map <- function(effort, title = "Fishing Effort") {
  years <- sort(unique(effort$year))
  
  effort_hex <- effort |>
    dplyr::group_by(h3_cell, year) |>
    dplyr::summarise(
      fishing_hours = sum(fishing_hours, na.rm = TRUE),
      n_trips       = dplyr::n_distinct(trip),
      .groups       = "drop"
    )
  
  polys     <- h3_cells_to_sf(unique(effort_hex$h3_cell))
  effort_sf <- dplyr::left_join(polys, effort_hex, by = "h3_cell")
  
  pal <- leaflet::colorNumeric(
    palette  = "YlOrRd",
    domain   = log1p(effort_sf$fishing_hours),
    na.color = "transparent"
  )
  
  m <- leaflet::leaflet() |>
    leaflet::addProviderTiles("CartoDB.DarkMatter") |>
    leaflet::addControl(htmltools::HTML(glue("<b>{title}</b>")), position = "topright")
  
  for (yr in years) {
    ld <- effort_sf |> dplyr::filter(year == yr)
    m  <- m |>
      leaflet::addPolygons(
        data        = ld,
        fillColor   = ~pal(log1p(fishing_hours)),
        fillOpacity = 0.75,
        color       = "transparent",
        weight      = 0,
        label       = ~purrr::map(glue(
          "<b>Year:</b> {year}<br>",
          "<b>Fishing hours:</b> {round(fishing_hours, 1)}<br>",
          "<b>Trips:</b> {n_trips}"
        ), htmltools::HTML),
        group = as.character(yr)
      )
  }
  
  m |>
    leaflet::addLayersControl(
      baseGroups = as.character(years),
      options    = leaflet::layersControlOptions(collapsed = FALSE)
    ) |>
    leaflet::addLegend(
      pal       = pal,
      values    = log1p(effort_sf$fishing_hours),
      title     = "log(Fishing hours)",
      position  = "bottomright",
      labFormat = leaflet::labelFormat(transform = function(x) round(expm1(x), 1))
    )
}


#' CPUE map per species on dark basemap with species toggle
#' @export
plot_cpue_map <- function(cpue_df, species, title = "CPUE (kg / fishing hour)") {
  polys <- h3_cells_to_sf(unique(cpue_df$h3_cell))
  vmax  <- quantile(cpue_df$cpue, 0.95, na.rm = TRUE)
  
  pal <- leaflet::colorNumeric(
    palette  = "YlOrRd",
    domain   = c(0, vmax),
    na.color = "transparent"
  )
  
  m <- leaflet::leaflet() |>
    leaflet::addProviderTiles("CartoDB.DarkMatter") |>
    leaflet::addControl(htmltools::HTML(glue("<b>{title}</b>")), position = "topright")
  
  for (sp in species) {
    sp_data  <- cpue_df |>
      dplyr::filter(species == sp) |>
      dplyr::mutate(cpue_capped = pmin(cpue, vmax))
    layer_sf <- dplyr::left_join(polys, sp_data, by = "h3_cell")
    
    m <- m |>
      leaflet::addPolygons(
        data        = layer_sf,
        fillColor   = ~pal(cpue_capped),
        fillOpacity = 0.75,
        color       = "transparent",
        weight      = 0,
        label       = ~purrr::map(glue(
          "<b>{sp}</b><br>",
          "<b>CPUE:</b> {round(cpue, 3)} kg/hr<br>",
          "<b>Trips:</b> {n_trips}"
        ), htmltools::HTML),
        group = sp
      )
  }
  
  m |>
    leaflet::addLayersControl(
      baseGroups = species,
      options    = leaflet::layersControlOptions(collapsed = FALSE)
    ) |>
    leaflet::addLegend(
      pal      = pal,
      values   = c(0, vmax),
      title    = "CPUE (kg/hr)",
      position = "bottomright"
    )
}


# ── 7. Main pipeline ──────────────────────────────────────────────────────────

#' Run the full spatial CPUE pipeline for a country
#'
#' @param country        One of "zanzibar", "kenya", "mozambique"
#' @param h3_res         H3 resolution. Default 7. Use 5-6 for sparse data.
#' @param top_n          Top N species to model. Default 5.
#' @param min_trips      Min trips per H3 cell to retain in CPUE output. Default 3.
#' @param method         CPUE estimation method:
#'   "weighted" (default) — catch/effort ratio per cell. Robust with sparse data.
#'   "nnls" — spatially deconvolved NNLS. Better with dense data (trips >> cells).
#' @param out_dir        Directory to save HTML maps. NULL = don't save.
#' @param max_files_all  Limit ALL track files for testing. NULL = all.
#' @param max_files_matched Limit matched track files for testing. NULL = all.
#' @return Invisible list: effort_all, effort_matched, cpue, trips,
#'                         map_effort, map_cpue
#' @export
model_cpue <- function(
    country,
    h3_res             = 7L,
    top_n              = 5L,
    min_trips          = 3L,
    method             = c("weighted", "nnls"),
    out_dir            = NULL,
    max_files_all      = NULL,
    max_files_matched  = NULL
) {
  method <- match.arg(method)
  log_info(
    "=== CPUE pipeline | country = {country}",
    " | H3 res = {h3_res} | method = {method} ==="
  )
  
  conf <- read_config()
  
  # ── Matched trips (catch + pds_trip ID) ──────────────────────────────────────
  matched <- download_matched_trips(country, conf)
  
  # ── ALL predicted tracks → effort map ────────────────────────────────────────
  log_info("--- Downloading ALL tracks for effort map ---")
  tracks_all <- download_all_tracks(conf, max_files = max_files_all)
  tracks_all <- prepare_tracks(tracks_all, h3_res)
  effort_all <- aggregate_effort(tracks_all, label = "Full effort")
  rm(tracks_all)
  
  log_info(
    "Full effort: {n_distinct(effort_all$trip)} trips |",
    " {n_distinct(effort_all$h3_cell)} H3 cells |",
    " {format(round(sum(effort_all$fishing_hours)), big.mark = ',')} total fishing hours"
  )
  
  # ── Matched predicted tracks → CPUE model ────────────────────────────────────
  log_info("--- Downloading MATCHED tracks for CPUE model ---")
  tracks_m  <- download_matched_tracks(conf, matched,
                                       max_files = max_files_matched)
  tracks_m  <- prepare_tracks(tracks_m, h3_res)
  effort_m  <- aggregate_effort(tracks_m, label = "Matched effort")
  rm(tracks_m)
  
  # ── Catch matrix + join ───────────────────────────────────────────────────────
  catch_wide <- build_catch_wide(matched)
  rm(matched)
  
  trips <- join_effort_catch(effort_m, catch_wide)
  
  if (nrow(trips) == 0) {
    log_warn("Pipeline stopped: no matched trips.")
    return(invisible(NULL))
  }
  
  # ── CPUE estimation ───────────────────────────────────────────────────────────
  result <- if (method == "weighted") {
    run_weighted_cpue(trips, top_n, min_trips)
  } else {
    run_nnls_cpue(trips, top_n, min_trips)
  }
  
  cpue_df <- result$cpue
  species <- result$species
  
  # ── Maps ──────────────────────────────────────────────────────────────────────
  map_effort <- plot_effort_map(
    effort_all,
    title = glue("{stringr::str_to_title(country)} - Fishing Effort by Year (all trips)")
  )
  
  map_cpue <- plot_cpue_map(
    cpue_df, species,
    title = glue(
      "{stringr::str_to_title(country)} - CPUE by Species",
      " ({method}, H3 res {h3_res}, {n_distinct(trips$trip)} trips)"
    )
  )
  
  # ── Save ──────────────────────────────────────────────────────────────────────
  if (!is.null(out_dir)) {
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
    htmlwidgets::saveWidget(
      map_effort,
      file.path(out_dir, glue("{country}_effort_h3r{h3_res}.html")),
      selfcontained = TRUE
    )
    htmlwidgets::saveWidget(
      map_cpue,
      file.path(out_dir, glue("{country}_cpue_{method}_h3r{h3_res}.html")),
      selfcontained = TRUE
    )
    log_info("Maps saved to {out_dir}")
  }
  
  log_info("=== Pipeline complete ===")
  
  invisible(list(
    effort_all     = effort_all,
    effort_matched = effort_m,
    cpue           = cpue_df,
    trips          = trips,
    map_effort     = map_effort,
    map_cpue       = map_cpue
  ))
}


# ── 8. Run ────────────────────────────────────────────────────────────────────
#
out <- model_cpue(
  country           = "zanzibar",
  h3_res            = 7L,
  top_n             = 5L,
  min_trips         = 1L,
  method            = "weighted",  # or "nnls" for denser data
  out_dir           = "outputs/cpue",
  max_files_all     = NULL,         # remove for full run
  max_files_matched = NULL         # always download all matched (~160 files)
)
# out$map_effort   # full effort map (all trips)
# out$map_cpue     # CPUE map (matched trips only)


