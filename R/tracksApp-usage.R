# app_usage_report.R
# Functions for generating app usage analytics report

#' Pull catch events data from MongoDB
#'
#' @param conf Configuration object with MongoDB connection details
#' @return tibble of catch events
#' @export
pull_catch_events <- function(conf) {
  mdb_collection_pull(
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = conf$storage$mongodb$tracks_app$collection$catch_events,
    db_name = conf$storage$mongodb$tracks_app$database_name
  ) |>
    dplyr::as_tibble() |>
    dplyr::filter(is.na(.data$isAdminSubmission)) |>
    dplyr::select(-c("photos", "gps_photo", "community", "reportedAt")) |>
    dplyr::as_tibble()
}

#' Pull users data from MongoDB
#'
#' @param conf Configuration object with MongoDB connection details
#' @return tibble of users
#' @export
pull_users_data <- function(conf) {
  mdb_collection_pull(
    connection_string = conf$storage$mongodb$tracks_app$connection_string,
    collection_name = "users",
    db_name = conf$storage$mongodb$tracks_app$database_name
  ) |>
    dplyr::select(c(.data$Country, .data$IMEI, .data$Boat)) |>
    dplyr::distinct() |>
    janitor::clean_names() |>
    dplyr::rename(boat_name = "boat") |>
    dplyr::as_tibble()
}

#' Calculate catch record usage by week
#'
#' @param trips tibble of trip data
#' @param users tibble of user data
#' @param countries character vector of countries to include
#' @return tibble of catch record usage by week
#' @export
calculate_catch_record_usage <- function(
  trips,
  users,
  countries = c("Kenya", "Zanzibar", "Mozambique")
) {
  trips |>
    janitor::clean_names() |>
    dplyr::select(c(.data$created_at, .data$imei, .data$boat_name, .data$trip_id)) |>
    dplyr::rename(date = "created_at") |>
    dplyr::right_join(users, by = c("imei", "boat_name")) |>
    dplyr::filter(.data$country %in% countries) |>
    dplyr::distinct() |>
    dplyr::mutate(
      date_week = lubridate::floor_date(.data$date, unit = "week")
    ) |>
    dplyr::group_by(.data$country, .data$date_week, .data$boat_name, .data$imei) |>
    dplyr::summarise(
      catch_records = dplyr::n_distinct(.data$trip_id),
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$catch_records)) |>
    dplyr::distinct() |>
    dplyr::mutate(
      catch_records = dplyr::if_else(is.na(.data$date_week), 0, .data$catch_records)
    )
}

#' Create usage heatmap plot
#'
#' @param catch_record_usage tibble of catch record usage data
#' @param top_n integer, number of top boats to display (default 50)
#' @return ggplot object
#' @export
create_usage_heatmap <- function(catch_record_usage, top_n = 50) {
  # Rank boats by total catch records
  boat_order <- catch_record_usage |>
    dplyr::group_by(.data$imei) |>
    dplyr::summarise(total_records = sum(.data$catch_records, na.rm = TRUE)) |>
    dplyr::arrange(dplyr::desc(.data$total_records)) |>
    dplyr::pull(.data$imei)

  # Filter to top boats with records > 0
  catch_record_usage |>
    dplyr::filter(.data$catch_records > 0) |>
    dplyr::mutate(imei = factor(.data$imei, levels = rev(boat_order))) |>
    ggplot2::ggplot(ggplot2::aes(
      x = .data$date_week,
      y = .data$imei,
      fill = .data$catch_records
    )) +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::geom_tile(
      ggplot2::aes(alpha = log(.data$catch_records)),
      color = "transparent",
      linewidth = 0.2
    ) +
    ggplot2::scale_fill_viridis_c(
      name = "Recorded\ncatches",
      begin = 0,
      end = 1,
      direction = 1
    ) +
    ggplot2::labs(
      title = "App Usage Heatmap by Boat",
      subtitle = "Boats ranked by total catch records (highest at top)",
      x = "Date",
      y = ""
    ) +
    ggplot2::theme(
      axis.text.y = ggplot2::element_text(size = 11),
      panel.grid.major.x = ggplot2::element_blank(),
      panel.grid.minor.x = ggplot2::element_blank(),
      plot.title = ggplot2::element_text(face = "bold"),
      legend.position = "right"
    ) +
    ggplot2::guides(alpha = "none")
}

#' Prepare usage summary table
#'
#' @param catch_record_usage tibble of catch record usage data
#' @return tibble of usage summary statistics
#' @export
prepare_usage_summary <- function(catch_record_usage) {
  catch_record_usage |>
    dplyr::group_by(.data$country, .data$boat_name, .data$imei) |>
    dplyr::summarise(
      total_records = sum(.data$catch_records, na.rm = TRUE),
      weeks_active = dplyr::n(),
      avg_records_per_week = round(mean(.data$catch_records, na.rm = TRUE), 1),
      max_records_week = max(.data$catch_records, na.rm = TRUE),
      first_use = min(.data$date_week, na.rm = TRUE),
      last_use = max(.data$date_week, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$total_records))
}

#' Create interactive reactable
#'
#' @param usage_summary tibble of usage summary data
#' @return reactable object
#' @export
create_usage_table <- function(usage_summary) {
  reactable::reactable(
    usage_summary,
    defaultPageSize = 20,
    searchable = TRUE,
    filterable = TRUE,
    defaultSorted = "total_records",
    defaultSortOrder = "desc",

    columns = list(
      country = reactable::colDef(
        name = "Country",
        width = 100,
        style = function(value) {
          if (value == "Kenya") {
            list(background = "#e3f2fd", fontWeight = 600)
          } else if (value == "Zanzibar") {
            list(background = "#f3e5f5", fontWeight = 600)
          } else {
            list(background = "#fff3e0", fontWeight = 600)
          }
        }
      ),

      boat_name = reactable::colDef(
        name = "Boat Name",
        minWidth = 200,
        style = list(fontWeight = 500)
      ),

      imei = reactable::colDef(
        name = "Device ID",
        width = 120,
        style = list(fontFamily = "monospace", fontSize = "0.85em")
      ),

      total_records = reactable::colDef(
        name = "Total Catches",
        width = 120,
        align = "center",
        style = function(value) {
          normalized <- (value - min(usage_summary$total_records)) /
            (max(usage_summary$total_records) -
              min(usage_summary$total_records))
          color <- grDevices::rgb(
            grDevices::colorRamp(c("#f7fbff", "#08519c"))(normalized) / 255
          )
          list(
            background = color,
            fontWeight = 700,
            color = if (normalized > 0.5) "white" else "black"
          )
        }
      ),

      weeks_active = reactable::colDef(
        name = "Weeks Active",
        width = 120,
        align = "center",
        cell = function(value) {
          paste0(value, " ", if (value == 1) "week" else "weeks")
        }
      ),

      avg_records_per_week = reactable::colDef(
        name = "Avg/Week",
        width = 100,
        align = "center",
        format = reactable::colFormat(digits = 1)
      ),

      max_records_week = reactable::colDef(
        name = "Peak Week",
        width = 100,
        align = "center",
        style = function(value) {
          if (value >= 15) {
            list(
              background = "#4caf50",
              color = "white",
              fontWeight = 700,
              borderRadius = "4px"
            )
          } else if (value >= 10) {
            list(
              background = "#ff9800",
              color = "white",
              fontWeight = 700,
              borderRadius = "4px"
            )
          } else {
            list(fontWeight = 500)
          }
        }
      ),

      first_use = reactable::colDef(
        name = "First Use",
        width = 110,
        format = reactable::colFormat(date = TRUE, locales = "en-GB")
      ),

      last_use = reactable::colDef(
        name = "Last Use",
        width = 110,
        format = reactable::colFormat(date = TRUE, locales = "en-GB")
      )
    ),

    defaultColDef = reactable::colDef(
      headerStyle = list(background = "#f5f5f5", fontWeight = 700)
    ),

    highlight = TRUE,
    bordered = TRUE,
    striped = TRUE,

    theme = reactable::reactableTheme(
      borderColor = "#dfe2e5",
      stripedColor = "#f6f8fa",
      highlightColor = "#fff9c4"
    )
  )
}

#' Generate complete app usage report
#'
#' @param conf Configuration object with MongoDB connection details
#' @param output_file character, path to output HTML file
#' @param countries character vector of countries to include
#' @param top_n integer, number of top boats to display in heatmap
#' @return invisibly returns the output file path
#' @export
generate_app_usage_report <- function(
  conf,
  output_file = "app_usage_report.html",
  countries = c("Kenya", "Zanzibar", "Mozambique"),
  top_n = 50
) {
  # Pull data
  message("Pulling catch events data...")
  trips <- pull_catch_events(conf)

  message("Pulling users data...")
  users <- pull_users_data(conf)

  # Calculate usage
  message("Calculating catch record usage...")
  catch_record_usage <- calculate_catch_record_usage(trips, users, countries)

  # Prepare summary
  message("Preparing usage summary...")
  usage_summary <- prepare_usage_summary(catch_record_usage)

  # Render report
  message("Rendering report...")
  rmarkdown::render(
    input = system.file("reports", "app_usage_report.Rmd", package = "coasts"),
    output_file = output_file,
    params = list(
      catch_record_usage = catch_record_usage,
      usage_summary = usage_summary,
      top_n = top_n,
      report_date = Sys.Date()
    ),
    envir = new.env()
  )

  message(sprintf("Report generated: %s", output_file))
  invisible(output_file)
}
