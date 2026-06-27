# Standalone visualization script — reads the pre-computed dashboard parquets
# from GCS (produced by export_dashboard()) and renders the seven S/A-tier
# indicator charts for the Peskas analytical dashboard.
#
# Usage:
#   source("inst/peskas-dashboard-shiny/dashboard-viz.R")
#   # or call individual plot functions:
#   plot_weekly(weekly, country = "kenya")
#
# All functions return a ggplot2 object. Use ggsave() to export.

library(coasts)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(scales)
library(lubridate)
library(stringr)
library(tidyr)
library(purrr)

# ── Config & data download ────────────────────────────────────────────────────

conf <- coasts::read_config()
coasts_opts <- coasts::resolve_storage_opts(conf, "coasts")

.dl <- function(prefix) {
  coasts::download_parquet_from_cloud(
    prefix = prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

message("Downloading dashboard datasets from GCS ...")
weekly <- .dl("dashboard_weekly")
top_taxa <- .dl("dashboard_top_taxa")
rarity <- .dl("dashboard_rarity")
seasonal <- .dl("dashboard_seasonal")
cpue <- .dl("dashboard_cpue")
mtl <- .dl("dashboard_mtl")
vuln <- .dl("dashboard_vuln")
message("All datasets loaded.")

# ── Theme & palettes ──────────────────────────────────────────────────────────

pal_geo_base <- c(
  "#29B6F6",
  "#26C6DA",
  "#66BB6A",
  "#FDD835",
  "#FB8C00",
  "#EF5350",
  "#AB47BC",
  "#7986CB",
  "#26A69A",
  "#EC407A",
  "#8BC34A",
  "#FF7043"
)
pal_ocean <- c(
  "#29B6F6",
  "#26C6DA",
  "#66BB6A",
  "#FDD835",
  "#FB8C00",
  "#EF5350",
  "#AB47BC",
  "#7986CB",
  "#26A69A",
  "#EC407A"
)

theme_peskas <- function(base_size = 11) {
  ggplot2::theme_minimal(base_size = base_size) %+replace%
    ggplot2::theme(
      text = element_text(colour = "#c0c8d8"),
      plot.title = element_text(
        size = rel(1.15),
        face = "bold",
        hjust = 0,
        colour = "#e0e6f0",
        margin = margin(b = 5)
      ),
      plot.subtitle = element_text(
        size = rel(0.83),
        hjust = 0,
        colour = "#8a9bb8",
        margin = margin(b = 8)
      ),
      plot.caption = element_text(
        size = rel(0.68),
        hjust = 1,
        colour = "#5a6880",
        margin = margin(t = 8)
      ),
      axis.title = element_text(
        size = rel(0.83),
        face = "bold",
        colour = "#8a9bb8"
      ),
      axis.text = element_text(size = rel(0.77), colour = "#8a9bb8"),
      axis.line = element_line(colour = "#3a4060", linewidth = 0.35),
      panel.grid.major = element_line(colour = "#2e3448", linewidth = 0.25),
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      legend.background = element_rect(fill = "#242938", colour = NA),
      legend.title = element_text(
        face = "bold",
        size = rel(0.80),
        colour = "#e0e6f0"
      ),
      legend.text = element_text(size = rel(0.77), colour = "#c0c8d8"),
      strip.text = element_text(
        face = "bold",
        size = rel(0.85),
        colour = "#e0e6f0"
      ),
      strip.background = element_rect(fill = "#2e3448", colour = NA),
      plot.background = element_rect(fill = "#242938", colour = NA),
      panel.background = element_rect(fill = "#242938", colour = NA),
      plot.margin = margin(12, 12, 8, 12)
    )
}
ggplot2::theme_set(theme_peskas())

.cap <- function(country) {
  d <- weekly |> dplyr::filter(.data$country == !!country)
  rng <- range(d$week_start, na.rm = TRUE)
  paste0(
    "Source: Peskas · ",
    str_to_title(country),
    " · ",
    rng[1],
    " – ",
    rng[2]
  )
}

.geo_pal <- function(country_data) {
  units <- sort(unique(country_data$province))
  setNames(rep_len(pal_geo_base, length(units)), units)
}

# ── 1. Weekly landing dynamics ────────────────────────────────────────────────

#' Weekly catch and trips time series
#'
#' @param data    `dashboard_weekly` tibble.
#' @param country One of "kenya", "mozambique", "zanzibar".
#' @export
plot_weekly <- function(data, country) {
  d <- data |> dplyr::filter(.data$country == !!country)
  sf <- max(d$total_catch_kg, na.rm = TRUE) / max(d$n_trips, na.rm = TRUE)

  ggplot(d, aes(x = week_start)) +
    geom_col(
      aes(y = total_catch_kg),
      fill = "#29B6F6",
      alpha = 0.55,
      width = 6
    ) +
    geom_smooth(
      aes(y = total_catch_kg),
      method = "loess",
      span = 0.3,
      colour = "#00bcd4",
      linewidth = 1.1,
      se = TRUE,
      fill = "#00bcd4",
      alpha = 0.15
    ) +
    geom_line(
      aes(y = n_trips * sf),
      colour = "#FB8C00",
      linewidth = 0.8,
      linetype = "dashed"
    ) +
    scale_y_continuous(
      labels = comma,
      name = "Total Catch (kg)",
      sec.axis = sec_axis(~ . / sf, name = "Number of Trips")
    ) +
    labs(
      x = NULL,
      caption = .cap(country),
      subtitle = "Bars = catch · Dashed = trips · Teal = LOESS trend"
    ) +
    theme(axis.title.y.right = element_text(colour = "#FB8C00"))
}

# ── 2. Top 10 taxa by biomass ─────────────────────────────────────────────────

#' Top-10 taxa lollipop chart
#'
#' @param data    `dashboard_top_taxa` tibble.
#' @param country One of "kenya", "mozambique", "zanzibar".
#' @export
plot_top_taxa <- function(data, country) {
  d <- data |>
    dplyr::filter(.data$country == !!country) |>
    dplyr::slice_max(.data$total_kg, n = 10)

  ggplot(d, aes(x = reorder(catch_taxon, total_kg), y = total_kg)) +
    geom_segment(
      aes(xend = catch_taxon, y = 0, yend = total_kg),
      colour = "#29B6F6",
      linewidth = 1.2
    ) +
    geom_point(size = 5, colour = "#e0e6f0") +
    geom_text(
      aes(label = paste0(round(pct, 1), "%")),
      hjust = -0.3,
      size = 3.2,
      fontface = "bold",
      colour = "#FB8C00"
    ) +
    coord_flip() +
    scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.18))) +
    labs(x = NULL, y = "Total Catch (kg)", caption = .cap(country))
}

# ── 3. Frequency–biomass rarity quadrant ─────────────────────────────────────

#' Rarity–biomass quadrant coloured by category
#'
#' @param data    `dashboard_rarity` tibble.
#' @param country One of "kenya", "mozambique", "zanzibar".
#' @export
plot_rarity <- function(data, country) {
  d <- data |> dplyr::filter(.data$country == !!country)

  ggplot(
    d,
    aes(x = freq_pct, y = biomass_pct, colour = category, size = n_sites)
  ) +
    geom_hline(
      yintercept = median(d$biomass_pct),
      linetype = "dashed",
      colour = "#5a6880"
    ) +
    geom_vline(
      xintercept = median(d$freq_pct),
      linetype = "dashed",
      colour = "#5a6880"
    ) +
    geom_point(alpha = 0.85) +
    geom_text_repel(
      data = d |>
        dplyr::filter(
          freq_pct > quantile(freq_pct, 0.80, na.rm = TRUE) |
            biomass_pct > quantile(biomass_pct, 0.80, na.rm = TRUE) |
            category == "Rare High Yield"
        ),
      aes(label = catch_taxon),
      size = 2.8,
      max.overlaps = 14,
      seed = 42,
      show.legend = FALSE
    ) +
    scale_colour_manual(
      values = c(
        "Dominant" = "#29B6F6",
        "Frequent Low Yield" = "#66BB6A",
        "Rare High Yield" = "#EF5350",
        "Rare Low Yield" = "#5a6880"
      ),
      name = "Category"
    ) +
    scale_size_continuous(range = c(2, 8), name = "Districts") +
    scale_x_log10() +
    scale_y_log10() +
    labs(
      x = "Frequency (% trips) — log",
      y = "Biomass (% total kg) — log",
      caption = .cap(country)
    ) +
    guides(
      colour = guide_legend(override.aes = list(size = 4, shape = 16)),
      size = "none"
    )
}

# ── 4. Seasonal catch calendar ────────────────────────────────────────────────

#' Seasonal catch heatmap for top taxa
#'
#' @param data    `dashboard_seasonal` tibble.
#' @param country One of "kenya", "mozambique", "zanzibar".
#' @export
plot_seasonal <- function(data, country) {
  d <- data |>
    dplyr::filter(.data$country == !!country) |>
    dplyr::mutate(
      month = factor(
        as.character(month),
        levels = month.abb[sort(unique(month_num))]
      )
    )

  ggplot(
    d,
    aes(x = month, y = reorder(catch_taxon, total_kg), fill = scaled_kg)
  ) +
    geom_tile(colour = "#242938", linewidth = 0.7) +
    geom_text(
      aes(label = ifelse(total_kg > 0, round(total_kg, 0), "")),
      size = 2.6,
      colour = "white",
      fontface = "bold"
    ) +
    scale_fill_viridis_c(
      option = "inferno",
      direction = -1,
      name = "Relative Abundance",
      labels = percent_format()
    ) +
    labs(
      x = "Month",
      y = NULL,
      caption = .cap(country),
      subtitle = "Colour intensity = within-taxon peak | Values = kg"
    )
}

# ── 5. Monthly CPUE trend ─────────────────────────────────────────────────────

#' Monthly median CPUE with IQR ribbon
#'
#' @param data    `dashboard_cpue` tibble.
#' @param country One of "kenya", "mozambique", "zanzibar".
#' @export
plot_cpue <- function(data, country) {
  d <- data |>
    dplyr::filter(.data$country == !!country) |>
    dplyr::mutate(
      month = factor(
        as.character(month),
        levels = month.abb[sort(unique(month_num))]
      )
    )

  ggplot(d, aes(x = month)) +
    geom_ribbon(
      aes(ymin = q25, ymax = q75, group = 1),
      fill = "#29B6F6",
      alpha = 0.25
    ) +
    geom_line(
      aes(y = median_cpue, group = 1),
      colour = "#00bcd4",
      linewidth = 1.2
    ) +
    geom_point(
      aes(y = median_cpue, size = n_trips),
      colour = "#e0e6f0",
      alpha = 0.9
    ) +
    scale_size_continuous(range = c(2, 6), name = "Trips") +
    labs(
      x = "Month",
      y = "Median CPUE (kg/hr)",
      caption = .cap(country),
      subtitle = "IQR ribbon shows seasonal variability"
    )
}

# ── 6. Mean trophic level by gear and region ──────────────────────────────────

#' Mean trophic level lollipop — gear or region dimension
#'
#' @param data      `dashboard_mtl` tibble.
#' @param country   One of "kenya", "mozambique", "zanzibar".
#' @param dimension One of "gear", "region", "month".
#' @export
plot_mtl <- function(data, country, dimension = "gear") {
  d <- data |>
    dplyr::filter(.data$country == !!country, .data$dimension == !!dimension)

  col_map <- dplyr::case_when(
    d$mTL >= 4.0 ~ "#EF5350",
    d$mTL >= 3.5 ~ "#FB8C00",
    d$mTL >= 3.0 ~ "#FDD835",
    TRUE ~ "#66BB6A"
  )

  p <- ggplot(d, aes(x = reorder(gear, mTL), y = mTL)) +
    geom_segment(
      aes(xend = gear, y = 2.5, yend = mTL),
      colour = col_map,
      linewidth = 1.2
    ) +
    geom_point(colour = col_map, aes(size = n_trips)) +
    geom_hline(yintercept = 3.25, linetype = "dashed", colour = "#5a6880") +
    annotate(
      "text",
      x = 1,
      y = 3.28,
      label = "FAO 'fishing down' threshold",
      hjust = 0,
      size = 2.8,
      colour = "#5a6880"
    ) +
    coord_flip() +
    scale_size_continuous(range = c(2, 6), name = "Trips") +
    scale_y_continuous(limits = c(2.5, 4.6)) +
    labs(
      x = NULL,
      y = "Mean Trophic Level (mTL)",
      caption = .cap(country),
      subtitle = paste0(
        str_to_title(dimension),
        " · Weighted by catch (kg) · Green→Red = low→high TL"
      )
    ) +
    theme(legend.position = "none")

  p
}

# ── 7. Vulnerability overlay ──────────────────────────────────────────────────

#' Frequency–biomass quadrant coloured by FishBase vulnerability
#'
#' @param data      `dashboard_rarity` tibble (includes vulnerability columns).
#' @param country   One of "kenya", "mozambique", "zanzibar".
#' @param vuln_type One of "fishing" (default) or "climate".
#' @export
plot_vulnerability <- function(data, country, vuln_type = "fishing") {
  vuln_col <- if (vuln_type == "fishing") {
    "vulnerability_fishing"
  } else {
    "vulnerability_climate"
  }
  vuln_label <- if (vuln_type == "fishing") {
    "Fishing Vulnerability (0–100)"
  } else {
    "Climate Vulnerability (0–100)"
  }

  d <- data |>
    dplyr::filter(.data$country == !!country) |>
    dplyr::mutate(vuln = .data[[vuln_col]])

  ggplot(d, aes(x = freq_pct, y = biomass_pct)) +
    geom_hline(
      yintercept = median(d$biomass_pct),
      linetype = "dashed",
      colour = "#5a6880"
    ) +
    geom_vline(
      xintercept = median(d$freq_pct),
      linetype = "dashed",
      colour = "#5a6880"
    ) +
    geom_point(aes(size = n_sites, colour = vuln), alpha = 0.85) +
    geom_text_repel(
      data = d |>
        dplyr::filter(
          freq_pct > quantile(freq_pct, 0.75, na.rm = TRUE) |
            biomass_pct > quantile(biomass_pct, 0.75, na.rm = TRUE)
        ),
      aes(label = catch_taxon),
      size = 2.8,
      max.overlaps = 12,
      seed = 42,
      show.legend = FALSE,
      colour = "#c0c8d8"
    ) +
    scale_colour_gradientn(
      colours = c("#66BB6A", "#FDD835", "#FB8C00", "#EF5350"),
      na.value = "#5a6880",
      name = vuln_label,
      limits = c(0, 100)
    ) +
    scale_size_continuous(range = c(2, 8), name = "Districts") +
    scale_x_log10() +
    scale_y_log10() +
    labs(
      x = "Frequency (% trips) — log",
      y = "Biomass (% total kg) — log",
      subtitle = "Grey = no FishBase match · Green→Red = low→high vulnerability",
      caption = .cap(country)
    ) +
    guides(
      colour = guide_colorbar(
        barwidth = 8,
        barheight = 0.5,
        title.position = "top"
      ),
      size = "none"
    )
}

# ── Example: render all 7 charts for Kenya ────────────────────────────────────

# Uncomment to generate and save all charts for a given country:
#
# country <- "kenya"
# plots <- list(
#   weekly        = plot_weekly(weekly, country),
#   top_taxa      = plot_top_taxa(top_taxa, country),
#   rarity        = plot_rarity(rarity, country),
#   seasonal      = plot_seasonal(seasonal, country),
#   cpue          = plot_cpue(cpue, country),
#   mtl_gear      = plot_mtl(mtl, country, dimension = "gear"),
#   mtl_region    = plot_mtl(mtl, country, dimension = "region"),
#   vulnerability = plot_vulnerability(rarity, country, vuln_type = "fishing")
# )
#
# dir.create("outputs", showWarnings = FALSE)
# purrr::iwalk(plots, function(p, name) {
#   ggsave(
#     filename = file.path("outputs", paste0(country, "_", name, ".png")),
#     plot     = p,
#     width    = 10, height = 6, dpi = 150,
#     bg       = "#242938"
#   )
# })
