
<!-- README.md is generated from README.Rmd. Please edit that file -->

# Peskas Coasts: Coastal Fisheries Data Pipeline

[![R-CMD-check](https://github.com/WorldFishCenter/peskas.coasts/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.coasts/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/WorldFishCenter/peskas.coasts/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.coasts/actions/workflows/pkgdown.yaml)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![License: GPL
v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**Peskas Coasts** is the automated data pipeline powering the coastal
fisheries analytics at [Peskas.org](https://peskas.org/).

This project processes raw ocean tracking data and generates accessible
web dashboards for the Western Indian Ocean (WIO) region, including
Kenya, Mozambique, and Zanzibar.

## What Does This Project Do?

**Peskas Coasts** is an automated data pipeline. Every two days, a
scheduled GitHub Actions workflow performs a series of data gathering,
classification, and spatial modeling tasks:

1.  **Data Ingestion**: Fetches the latest boat GPS tracks from Pelagic
    Data Systems (PDS) and combines them with human-reported survey data
    (KoboToolbox).
2.  **Fishing Activity Prediction**: Uses a statistical model
    (`ssfaitk`) to classify parts of the boat’s journey as fishing
    activity.
3.  **Spatial Modeling**: Translates GPS pings into standardized
    hexagonal grids (H3) and calculates fisheries metrics, like **Catch
    Per Unit Effort (CPUE)**.
4.  **Dashboard Delivery**: Exports the final results into web-ready
    formats (JSON/GeoJSON) and pushes them to MongoDB to update the maps
    on Peskas.org.

## Explore the Documentation

We’ve designed this documentation to be accessible to stakeholders,
researchers, and developers alike:

- 📖 **How the Pipeline Works**: A plain-English walkthrough of our
  automated GitHub Actions workflow. Learn how data travels from a
  boat’s GPS tracker to our web portal.
- 🗺️ **Understanding the Models**: Discover how we calculate Catch Per
  Unit Effort (CPUE) and why we use Hexagonal (H3) gridding to protect
  fisher privacy while highlighting ocean hotspots.
- 🛠️ **Reference**: For developers looking to interact with the
  underlying R functions and APIs.

## The Impact

By automating data cleaning, model prediction, and spatial aggregation,
Peskas Coasts provides updated spatial datasets for coastal monitoring.

------------------------------------------------------------------------

*Peskas Coasts is proudly developed as part of the WorldFish Center’s
Peskas initiative.*
