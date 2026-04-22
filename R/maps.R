#' Derive Fishing Ground Polygons from an H3 Effort Grid
#'
#' @description
#' Converts an H3 hexagonal effort grid (as produced by [aggregate_pds_effort()])
#' into contiguous fishing ground polygons by filtering cells that meet
#' activity thresholds and dissolving their geometries. The result is ready for
#' further spatial analysis.
#'
#' @details
#' The derivation proceeds in four steps:
#'
#' 1. **Optional rollup**: if `target_h3_res` is provided, all effort metrics
#'    are summed into parent H3 cells at the coarser resolution before applying
#'    thresholds. This lets you extract broad fishing grounds without
#'    reprocessing raw tracks.
#' 2. **Threshold filtering**: cells are kept only when `unique_trips >=
#'    min_trips` and `fishing_hours >= min_hours`. When `min_hours` is `NULL`
#'    the median fishing-hours across all cells is used as the threshold.
#' 3. **Per-cell normalisation**: after collapsing years, the study period
#'    length (`n_total_days`) is inferred from the stored date range
#'    (`first_active_date`, `last_active_date`). Per-day rates and constancy
#'    are computed at the cell level before polygon dissolution so that ground-
#'    level values are means of cell values (ecologically correct) rather than
#'    re-derived from ground totals.
#' 4. **Polygon extraction**: filtered hexagons are converted to polygons via
#'    [create_spatial_grid()], dissolved into contiguous areas with
#'    `sf::st_union()`, split into individual polygons with
#'    `sf::st_cast("POLYGON")`, and annotated with a stable `ground_id` and
#'    `area_km2`.
#'
#' If the input grid contains a `year` column (as produced by the default
#' [aggregate_pds_effort()] schema), effort is collapsed across all years before
#' thresholding.
#'
#' @param h3_grid_df Data frame with columns `h3_index`, `fishing_hours`,
#'   `unique_trips`, `n_active_days`, `first_active_date`, `last_active_date`,
#'   `avg_fidelity_sum`, `n_trips_for_fidelity` (as returned by
#'   [aggregate_pds_effort()]). A `year` column is accepted and collapsed by
#'   summation / min / max as appropriate.
#' @param min_trips Integer. Minimum number of unique trips per H3 cell required
#'   to retain the cell. Default is `3L`.
#' @param min_hours Numeric or `NULL`. Minimum accumulated fishing hours per
#'   cell. `NULL` (default) uses the median of `fishing_hours` across all cells.
#' @param min_pings Deprecated. Use `min_hours` instead. If supplied, a warning
#'   is emitted and the argument is ignored.
#' @param target_h3_res Integer (0-15) or `NULL`. If provided, effort is first
#'   rolled up to this coarser H3 resolution before thresholding. Must be
#'   lower than the resolution stored in `h3_grid_df`. `NULL` uses the grid
#'   as-is.
#' @param n_days Integer or `NULL`. Deprecated fallback for `n_total_days`.
#'   When `first_active_date` / `last_active_date` columns are present in
#'   `h3_grid_df` (grids produced by the current pipeline), `n_total_days` is
#'   inferred automatically and this argument is ignored.
#'
#' @return An `sf` POLYGON object in WGS84 (EPSG 4326) with columns:
#'   \describe{
#'     \item{`ground_id`}{Stable identifier, e.g. `"FG_1"`, ordered by
#'       descending area.}
#'     \item{`area_km2`}{Area of each polygon in square kilometres.}
#'     \item{`fishing_hours`}{Total accumulated fishing hours within the
#'       ground.}
#'     \item{`unique_trips`}{Number of unique trips that fished within the
#'       ground.}
#'     \item{`n_active_days`}{Sum of cell-level active days across all
#'       constituent H3 cells.}
#'     \item{`n_cells`}{Number of H3 cells that make up the ground.}
#'     \item{`avg_fidelity`}{Mean across cells of the average fraction of
#'       visiting trips' fishing time spent in each cell. Bounded [0, 1].
#'       Higher values indicate stronger habitat preference (fidelity).}
#'     \item{`constancy`}{Mean across cells of the fraction of study days each
#'       cell was fished. Bounded [0, 1]. Near 0 = sporadic; near 1 = daily.}
#'     \item{`avg_hours_per_day`}{Mean across cells of fishing hours per
#'       calendar day (`fishing_hours / n_total_days`).}
#'     \item{`avg_visits_per_day`}{Mean across cells of unique trips per
#'       calendar day (`unique_trips / n_total_days`).}
#'     \item{`hours_per_trip`}{Mean across cells of fishing hours per trip
#'       (`fishing_hours / unique_trips`).}
#'     \item{`fishing_hours_per_km2`}{Total fishing hours divided by ground
#'       area (effort density).}
#'     \item{`unique_trips_per_km2`}{Total unique trips divided by ground area
#'       (visit density).}
#'     \item{`hours_per_day_per_km2`}{`avg_hours_per_day` divided by ground
#'       area (effort-rate density).}
#'   }
#'   Returns `NULL` with a warning if no cells survive the filters.
#'
#' @seealso [aggregate_pds_effort()], [rollup_h3_resolution()],
#'   [create_spatial_grid()], [plot_effort_map()]
#'
#' @keywords modeling
#' @export
derive_fishing_grounds <- function(
  h3_grid_df,
  min_trips = 3L,
  min_hours = NULL,
  min_pings = NULL,
  target_h3_res = NULL,
  n_days = NULL
) {
  if (!is.null(min_pings)) {
    warning(
      "`min_pings` is deprecated; use `min_hours` instead. The argument is ignored.",
      call. = FALSE
    )
  }

  # Collapse year dimension if present (sum totals; min/max dates)
  grid <- if ("year" %in% names(h3_grid_df)) {
    h3_grid_df |>
      dplyr::group_by(.data$h3_index) |>
      dplyr::summarise(
        fishing_hours        = sum(.data$fishing_hours),
        unique_trips         = sum(.data$unique_trips),
        n_active_days        = sum(.data$n_active_days, na.rm = TRUE),
        avg_fidelity_sum     = sum(.data$avg_fidelity_sum, na.rm = TRUE),
        n_trips_for_fidelity = sum(.data$n_trips_for_fidelity, na.rm = TRUE),
        first_active_date    = min(.data$first_active_date, na.rm = TRUE),
        last_active_date     = max(.data$last_active_date, na.rm = TRUE),
        fishing_pings        = if ("fishing_pings" %in% names(h3_grid_df))
          sum(.data$fishing_pings) else NA_integer_,
        .groups = "drop"
      )
  } else {
    h3_grid_df
  }

  # Infer n_total_days from stored date range (preferred) or legacy n_days arg
  n_total_days <- if (
    all(c("first_active_date", "last_active_date") %in% names(grid)) &&
      !all(is.na(grid$first_active_date))
  ) {
    as.numeric(
      max(grid$last_active_date, na.rm = TRUE) -
        min(grid$first_active_date, na.rm = TRUE)
    ) + 1
  } else if (!is.null(n_days)) {
    n_days
  } else if ("year" %in% names(h3_grid_df)) {
    length(unique(h3_grid_df$year)) * 365L
  } else {
    logger::log_warn("Cannot infer n_total_days -- per-day metrics will be NA")
    NA_real_
  }

  logger::log_info("Study period: {round(n_total_days)} days")

  # Optional rollup to coarser H3 resolution
  if (!is.null(target_h3_res)) {
    logger::log_info(
      "Rolling up H3 grid to resolution {target_h3_res} before extracting grounds"
    )
    grid <- grid |>
      dplyr::mutate(
        h3_index = h3jsr::get_parent(.data$h3_index, res = target_h3_res)
      ) |>
      dplyr::group_by(.data$h3_index) |>
      dplyr::summarise(
        fishing_hours        = sum(.data$fishing_hours),
        unique_trips         = sum(.data$unique_trips),
        n_active_days        = sum(.data$n_active_days, na.rm = TRUE),
        avg_fidelity_sum     = sum(.data$avg_fidelity_sum, na.rm = TRUE),
        n_trips_for_fidelity = sum(.data$n_trips_for_fidelity, na.rm = TRUE),
        first_active_date    = min(.data$first_active_date, na.rm = TRUE),
        last_active_date     = max(.data$last_active_date, na.rm = TRUE),
        fishing_pings        = if ("fishing_pings" %in% names(grid))
          sum(.data$fishing_pings) else NA_integer_,
        .groups = "drop"
      )
  }

  hours_threshold <- min_hours %||% stats::median(grid$fishing_hours)

  logger::log_info(
    "Filtering {nrow(grid)} H3 cells: min_trips = {min_trips},",
    " min_hours = {round(hours_threshold, 1)}"
  )

  filtered <- grid |>
    dplyr::filter(
      .data$unique_trips >= min_trips,
      .data$fishing_hours >= hours_threshold
    )

  if (nrow(filtered) == 0) {
    logger::log_warn("No H3 cells passed the filters -- returning NULL")
    return(NULL)
  }

  logger::log_info(
    "{nrow(filtered)} cells retained, deriving contiguous fishing ground polygons"
  )

  # Compute per-cell normalised metrics before polygon dissolve so that
  # ground-level values are means of cell values (not re-derived from totals)
  filtered <- filtered |>
    dplyr::mutate(
      avg_fidelity = dplyr::if_else(
        .data$n_trips_for_fidelity > 0,
        .data$avg_fidelity_sum / .data$n_trips_for_fidelity,
        NA_real_
      ),
      constancy = .data$n_active_days / n_total_days,
      avg_hours_per_day = .data$fishing_hours / n_total_days,
      avg_visits_per_day = .data$unique_trips / n_total_days,
      hours_per_trip = dplyr::if_else(
        .data$unique_trips > 0,
        .data$fishing_hours / .data$unique_trips,
        NA_real_
      )
    )

  hex_sf <- create_spatial_grid(filtered)

  geoms <- hex_sf |>
    sf::st_union() |>
    sf::st_cast("POLYGON")

  grounds <- sf::st_sf(
    ground_id = paste0("FG_", seq_along(geoms)),
    area_km2 = as.numeric(sf::st_area(geoms)) / 1e6,
    geometry = geoms,
    crs = 4326
  )

  # Aggregate effort from constituent H3 cells into each ground polygon.
  # Raw totals are summed; normalised metrics are averaged across cells.
  effort_by_ground <- sf::st_join(hex_sf, grounds, join = sf::st_within) |>
    sf::st_drop_geometry() |>
    dplyr::filter(!is.na(.data$ground_id)) |>
    dplyr::group_by(.data$ground_id) |>
    dplyr::summarise(
      fishing_hours      = sum(.data$fishing_hours, na.rm = TRUE),
      unique_trips       = sum(.data$unique_trips, na.rm = TRUE),
      n_active_days      = sum(.data$n_active_days, na.rm = TRUE),
      n_cells            = dplyr::n(),
      avg_fidelity       = mean(.data$avg_fidelity, na.rm = TRUE),
      constancy          = mean(.data$constancy, na.rm = TRUE),
      avg_hours_per_day  = mean(.data$avg_hours_per_day, na.rm = TRUE),
      avg_visits_per_day = mean(.data$avg_visits_per_day, na.rm = TRUE),
      hours_per_trip     = mean(.data$hours_per_trip, na.rm = TRUE),
      .groups = "drop"
    )

  grounds <- grounds |>
    dplyr::left_join(effort_by_ground, by = "ground_id") |>
    dplyr::mutate(dplyr::across(
      c("fishing_hours", "unique_trips", "n_active_days", "n_cells"),
      ~ tidyr::replace_na(., 0L)
    )) |>
    dplyr::mutate(
      fishing_hours_per_km2  = dplyr::if_else(
        .data$area_km2 > 0, .data$fishing_hours / .data$area_km2, NA_real_
      ),
      unique_trips_per_km2   = dplyr::if_else(
        .data$area_km2 > 0, .data$unique_trips / .data$area_km2, NA_real_
      ),
      hours_per_day_per_km2  = dplyr::if_else(
        .data$area_km2 > 0, .data$avg_hours_per_day / .data$area_km2, NA_real_
      )
    ) |>
    dplyr::arrange(dplyr::desc(.data$area_km2))

  logger::log_success(
    "Derived {nrow(grounds)} fishing grounds",
    " (total area: {round(sum(grounds$area_km2), 1)} km\u00b2,",
    " {round(sum(grounds$fishing_hours, na.rm = TRUE))} total fishing hours)"
  )

  grounds
}


#' Create an Interactive Leaflet Map of Fishing Effort by Year
#'
#' @description
#' Builds a Leaflet map of H3 fishing effort with one layer per year, toggled
#' via a radio-button year control. Hexagon colour reflects a log-scaled effort
#' metric. A basemap switcher (Dark / Light / Satellite) is provided via a
#' custom control in the top-right corner.
#'
#' @details
#' The function accepts the output of [aggregate_pds_effort()] (which includes
#' a `year` column) and aggregates effort to the H3 cell level within each year
#' before building the map layers. An all-time view can be approximated by
#' first collapsing the data across years:
#' ```r
#' all_time <- h3_grid |>
#'   dplyr::group_by(h3_index) |>
#'   dplyr::summarise(
#'     fishing_hours = sum(fishing_hours),
#'     n_trips       = sum(unique_trips),
#'     .groups = "drop"
#'   ) |>
#'   dplyr::mutate(year = "All years")
#' plot_effort_map(all_time)
#' ```
#'
#' @param effort A data frame with columns `h3_index`, `year`, `fishing_hours`,
#'   `unique_trips` (as returned by [aggregate_pds_effort()]).
#' @param metric Column to colour hexagons by: `"fishing_hours"` (default) or
#'   `"n_trips"`.
#'
#' @return A `leaflet` htmlwidget.
#'
#' @seealso [aggregate_pds_effort()], [derive_fishing_grounds()]
#'
#' @keywords modeling
#' @export
plot_effort_map <- function(effort, metric = c("fishing_hours", "n_trips")) {
  metric <- match.arg(metric)
  legend_title <- switch(
    metric,
    fishing_hours = "log(Fishing hours)",
    n_trips = "log(Trips)"
  )

  years <- sort(unique(effort$year))

  effort_hex <- effort |>
    dplyr::group_by(.data$h3_index, .data$year) |>
    dplyr::summarise(
      fishing_hours = sum(.data$fishing_hours, na.rm = TRUE),
      n_trips = sum(.data$unique_trips, na.rm = TRUE),
      .groups = "drop"
    )

  unique_cells <- unique(effort_hex$h3_index)
  polys <- sf::st_sf(
    h3_index = unique_cells,
    geometry = h3jsr::cell_to_polygon(unique_cells, simple = TRUE),
    crs = 4326
  )
  effort_sf <- dplyr::left_join(polys, effort_hex, by = "h3_index")

  active_vals <- log1p(effort_sf[[metric]])
  pal <- leaflet::colorNumeric(
    palette = "YlOrRd",
    domain = active_vals,
    na.color = "transparent"
  )

  m <- leaflet::leaflet() |>
    leaflet::addProviderTiles(
      leaflet::providers$CartoDB.DarkMatter,
      group = "bm_dark"
    ) |>
    leaflet::addProviderTiles(
      leaflet::providers$CartoDB.Positron,
      group = "bm_light"
    ) |>
    leaflet::addProviderTiles(
      leaflet::providers$Esri.WorldImagery,
      group = "bm_satellite"
    ) |>
    leaflet::addControl(
      htmltools::HTML(
        "
        <div id='bm-ctrl' style='background:rgba(0,0,0,0.7);padding:8px 10px;
             border-radius:6px;color:white;font-family:system-ui,sans-serif;
             font-size:12px;line-height:1.8;'>
          <div style='font-weight:600;margin-bottom:4px;color:#aaa;'>Basemap</div>
          <label><input type='radio' name='bm' value='bm_dark' checked> Dark</label><br>
          <label><input type='radio' name='bm' value='bm_light'      > Light</label><br>
          <label><input type='radio' name='bm' value='bm_satellite'  > Satellite</label>
        </div>
      "
      ),
      position = "topright"
    )

  for (yr in years) {
    ld <- effort_sf |> dplyr::filter(.data$year == yr)
    fill_vec <- pal(log1p(ld[[metric]]))
    popup_vec <- glue::glue_data(
      ld,
      "<div style='font-family:system-ui,sans-serif;font-size:13px;line-height:1.8;'>",
      "<b>Year:</b> {year}<br>",
      "<b>Fishing hours:</b> {round(fishing_hours, 1)} h<br>",
      "<b>Trips:</b> {n_trips}",
      "</div>"
    )
    m <- m |>
      leaflet::addPolygons(
        data = ld,
        fillColor = fill_vec,
        fillOpacity = 0.75,
        color = "transparent",
        weight = 0,
        popup = popup_vec,
        group = as.character(yr)
      )
  }

  m |>
    leaflet::addLayersControl(
      baseGroups = as.character(years),
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) |>
    leaflet::addLegend(
      pal = pal,
      values = active_vals,
      title = legend_title,
      position = "bottomright",
      labFormat = leaflet::labelFormat(
        transform = function(x) round(expm1(x), 1)
      )
    ) |>
    leaflet::addScaleBar(position = "bottomleft") |>
    htmlwidgets::onRender(
      "
      function(el, x) {
        var map = this;
        var basemaps = ['bm_dark', 'bm_light', 'bm_satellite'];

        document.querySelectorAll('#bm-ctrl input[type=radio]').forEach(function(inp) {
          inp.addEventListener('change', function() {
            var chosen = this.value;
            map.eachLayer(function(lyr) {
              if (lyr.options && basemaps.indexOf(lyr.options.group) !== -1) {
                lyr.setOpacity(lyr.options.group === chosen ? 1 : 0);
              }
            });
          });
        });
      }
    "
    )
}


#' Create an Interactive Leaflet Map of Spatial CPUE by Species
#'
#' Renders H3 hexagonal cells coloured by CPUE (kg / fishing hour) with a
#' species toggle control. Cells are capped at the 95th percentile to
#' prevent outliers from compressing the colour scale.
#'
#' @param cpue_df A tibble as returned by [model_cpue()] element `$cpue`,
#'   with columns `h3_index`, `cpue`, `species`, `n_trips`.
#' @param species Character vector of species names to include as map layers.
#' @param title Character. Map title shown in the top-right control.
#'   Default is `"CPUE (kg / fishing hour)"`.
#'
#' @return A `leaflet` htmlwidget.
#'
#' @seealso [model_cpue()], [plot_effort_map()]
#'
#' @keywords modeling
#' @export
plot_cpue_map <- function(
  cpue_df,
  species,
  title = "CPUE (kg / fishing hour)"
) {
  polys <- create_spatial_grid(
    cpue_df |>
      dplyr::distinct(.data$h3_index) |>
      dplyr::rename(h3_index = "h3_index")
  )
  vmax <- stats::quantile(cpue_df$cpue, 0.95, na.rm = TRUE)

  pal <- leaflet::colorNumeric(
    palette = "YlOrRd",
    domain = c(0, vmax),
    na.color = "transparent"
  )

  m <- leaflet::leaflet() |>
    leaflet::addProviderTiles("CartoDB.DarkMatter") |>
    leaflet::addControl(
      htmltools::HTML(glue::glue("<b>{title}</b>")),
      position = "topright"
    )

  for (sp in species) {
    sp_data <- cpue_df |>
      dplyr::filter(.data$species == sp) |>
      dplyr::mutate(cpue_capped = pmin(.data$cpue, vmax))
    layer_sf <- dplyr::left_join(polys, sp_data, by = "h3_index")

    m <- m |>
      leaflet::addPolygons(
        data = layer_sf,
        fillColor = ~ pal(cpue_capped),
        fillOpacity = 0.75,
        color = "transparent",
        weight = 0,
        label = ~ purrr::map(
          glue::glue(
            "<b>{sp}</b><br>",
            "<b>CPUE:</b> {round(cpue, 3)} kg/hr<br>",
            "<b>Trips:</b> {n_trips}"
          ),
          htmltools::HTML
        ),
        group = sp
      )
  }

  m |>
    leaflet::addLayersControl(
      baseGroups = species,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) |>
    leaflet::addLegend(
      pal = pal,
      values = c(0, vmax),
      title = "CPUE (kg/hr)",
      position = "bottomright"
    )
}


#' Interactive Leaflet Map of Fishing Effort by Gear Type
#'
#' Same as [plot_effort_map()] but the layer toggle uses gear type instead
#' of year. Uses matched trips only (gear is recorded in Kobo surveys, not
#' in raw PDS tracks).
#'
#' @param trips Combined effort + catch tibble from [join_effort_catch()],
#'   must contain columns `h3_index`, `fishing_hours`, `trip`, `gear`.
#' @param metric Column to colour hexagons by: `"fishing_hours"` (default)
#'   or `"n_trips"`.
#'
#' @return A `leaflet` htmlwidget.
#'
#' @seealso [plot_effort_map()], [model_cpue()]
#'
#' @keywords modeling
#' @export
plot_effort_map_gear <- function(
    trips,
    metric = c("fishing_hours", "n_trips")
) {
  metric <- match.arg(metric)
  legend_title <- switch(
    metric,
    fishing_hours = "log(Fishing hours)",
    n_trips       = "log(Trips)"
  )
  
  gears <- sort(unique(na.omit(trips$gear)))
  
  effort_hex <- trips |>
    dplyr::filter(!is.na(.data$gear)) |>
    dplyr::group_by(.data$h3_index, .data$gear) |>
    dplyr::summarise(
      fishing_hours = sum(.data$fishing_hours, na.rm = TRUE),
      n_trips       = dplyr::n_distinct(.data$trip),
      .groups       = "drop"
    )
  
  unique_cells <- unique(effort_hex$h3_index)
  polys <- sf::st_sf(
    h3_index = unique_cells,
    geometry = h3jsr::cell_to_polygon(unique_cells, simple = TRUE),
    crs = 4326
  )
  effort_sf <- dplyr::left_join(polys, effort_hex, by = "h3_index")
  
  active_vals <- log1p(effort_sf[[metric]])
  pal <- leaflet::colorNumeric(
    palette  = "YlOrRd",
    domain   = active_vals,
    na.color = "transparent"
  )
  
  m <- leaflet::leaflet() |>
    leaflet::addProviderTiles(
      leaflet::providers$CartoDB.DarkMatter,
      group = "bm_dark"
    ) |>
    leaflet::addProviderTiles(
      leaflet::providers$CartoDB.Positron,
      group = "bm_light"
    ) |>
    leaflet::addProviderTiles(
      leaflet::providers$Esri.WorldImagery,
      group = "bm_satellite"
    ) |>
    leaflet::addControl(
      htmltools::HTML(
        "
        <div id='bm-ctrl' style='background:rgba(0,0,0,0.7);padding:8px 10px;
             border-radius:6px;color:white;font-family:system-ui,sans-serif;
             font-size:12px;line-height:1.8;'>
          <div style='font-weight:600;margin-bottom:4px;color:#aaa;'>Basemap</div>
          <label><input type='radio' name='bm' value='bm_dark' checked> Dark</label><br>
          <label><input type='radio' name='bm' value='bm_light'      > Light</label><br>
          <label><input type='radio' name='bm' value='bm_satellite'  > Satellite</label>
        </div>
        "
      ),
      position = "topright"
    )
  
  for (gr in gears) {
    ld <- effort_sf |> dplyr::filter(.data$gear == gr)
    fill_vec  <- pal(log1p(ld[[metric]]))
    popup_vec <- glue::glue_data(
      ld,
      "<div style='font-family:system-ui,sans-serif;font-size:13px;line-height:1.8;'>",
      "<b>Gear:</b> {gear}<br>",
      "<b>Fishing hours:</b> {round(fishing_hours, 1)} h<br>",
      "<b>Trips:</b> {n_trips}",
      "</div>"
    )
    m <- m |>
      leaflet::addPolygons(
        data        = ld,
        fillColor   = fill_vec,
        fillOpacity = 0.75,
        color       = "transparent",
        weight      = 0,
        popup       = popup_vec,
        group       = gr
      )
  }
  
  m |>
    leaflet::addLayersControl(
      baseGroups = gears,
      options    = leaflet::layersControlOptions(collapsed = FALSE)
    ) |>
    leaflet::addLegend(
      pal       = pal,
      values    = active_vals,
      title     = legend_title,
      position  = "bottomright",
      labFormat = leaflet::labelFormat(
        transform = function(x) round(expm1(x), 1)
      )
    ) |>
    leaflet::addScaleBar(position = "bottomleft") |>
    htmlwidgets::onRender(
      "
      function(el, x) {
        var map = this;
        var basemaps = ['bm_dark', 'bm_light', 'bm_satellite'];
        document.querySelectorAll('#bm-ctrl input[type=radio]').forEach(function(inp) {
          inp.addEventListener('change', function() {
            var chosen = this.value;
            map.eachLayer(function(lyr) {
              if (lyr.options && basemaps.indexOf(lyr.options.group) !== -1) {
                lyr.setOpacity(lyr.options.group === chosen ? 1 : 0);
              }
            });
          });
        });
      }
      "
    )
}