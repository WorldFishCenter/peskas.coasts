# `exclude_dashboard_ids` is a country-dashboard concern only. The multi-country
# coasts portal must keep every survey form, so summarize_data() writes two
# monthly tables: `monthly_summaries` (filtered) and `all_monthly_summaries`
# (unfiltered), and `<country>_fishery_metrics` stays unfiltered too.
# The regression these guard: Kenya's exclusion list dropped 87% of validated
# rows from coasts.peskas.org, not just from its own dashboard.

# One catch item per row, two survey forms, two months, two districts.
fake_validated <- function() {
  tidyr::expand_grid(
    survey_id = c("keep", "drop"),
    gaul_2_name = c("Kilifi", "Kwale"),
    month = c(1L, 2L)
  ) |>
    dplyr::mutate(
      trip_id = paste(.data$survey_id, .data$gaul_2_name, .data$month),
      landing_date = lubridate::ymd(paste0("2024-0", .data$month, "-15")),
      gear = "gillnet",
      trip_duration_hrs = 5,
      n_fishers = 2,
      n_catch = 1L,
      length_cm = 30,
      catch_taxon = "TUN",
      scientific_name = "Thunnus albacares",
      catch_kg = 10,
      catch_price = 100,
      tot_catch_kg = 10,
      tot_catch_price = 100
    ) |>
    dplyr::select(-"month")
}

fake_conf <- function() {
  list(
    country = "testland",
    storage = list(google = list(key = "gcs", options = list(bucket = "b"))),
    api = list(
      trips = list(validated = list(cloud_path = "api", file_prefix = "trips"))
    ),
    pds = list(pds_tracks = list(file_prefix = "pds-tracks")),
    surveys = list(summaries = list(file_prefix = "testland-summaries"))
  )
}

# Runs summarize_data() with the cloud boundary mocked out, and returns every
# table it produced, keyed by name: the country-bucket parquet files plus the
# coasts-bucket uploads.
run_summarize <- function(exclude_dashboard_ids, validated = fake_validated()) {
  withr::local_dir(withr::local_tempdir())

  uploaded <- character(0)
  coasts_uploads <- list()

  testthat::local_mocked_bindings(
    read_config = function(...) fake_conf(),
    resolve_storage_opts = function(...) list(bucket = "b"),
    download_parquet_from_cloud = function(prefix, ...) {
      if (prefix == "asfis") {
        return(data.frame(
          alpha3_code = "TUN",
          scientific_name = "Thunnus albacares",
          english_name = "Yellowfin tuna"
        ))
      }
      if (grepl("grid_summaries", prefix)) {
        return(data.frame(lat = 1, lon = 1, time_spent_mins = 1))
      }
      validated
    },
    upload_cloud_file = function(file, ...) {
      uploaded <<- c(uploaded, file)
      invisible(NULL)
    },
    upload_parquet_to_cloud = function(data, prefix, ...) {
      coasts_uploads[[prefix]] <<- data
      invisible(NULL)
    },
    .package = "coasts"
  )

  summarize_data(
    exclude_dashboard_ids = exclude_dashboard_ids,
    log_threshold = logger::FATAL
  )

  c(
    stats::setNames(
      lapply(uploaded, arrow::read_parquet),
      sub("^testland-summaries_(.*)__.*$", "\\1", uploaded)
    ),
    coasts_uploads
  )
}

test_that("the exclusion reaches the dashboard tables but not the coasts ones", {
  out <- run_summarize(exclude_dashboard_ids = "drop")

  # Dashboard: one form left, so one trip per district-month.
  expect_equal(sum(out$districts_summaries$n_submissions), 4)

  # Coasts: both forms survive. Two trips of 10 kg each per district-month, so
  # the unfiltered mean catch per trip is over twice the dashboard's row count.
  trips_seen <- out$testland_fishery_metrics |>
    dplyr::filter(.data$metric_type == "pct_main_gear")
  expect_equal(nrow(trips_seen), 4) # 2 districts x 2 months, from all forms

  # Same district-months either way here, so the split shows up in the values,
  # not the shape: gillnet is 100% of trips whichever frame you count.
  expect_equal(nrow(out$all_monthly_summaries), 4)
})

test_that("a district only the excluded form covers still reaches coasts", {
  # The sharpest version of the bug: Kwale exists solely in the dropped form.
  only_dropped <- fake_validated() |>
    dplyr::filter(!(.data$survey_id == "keep" & .data$gaul_2_name == "Kwale"))

  out <- run_summarize(
    exclude_dashboard_ids = "drop",
    validated = only_dropped
  )

  expect_false("Kwale" %in% out$monthly_summaries$gaul_2_name)
  expect_true("Kwale" %in% out$all_monthly_summaries$gaul_2_name)
  expect_true("Kwale" %in% out$testland_fishery_metrics$gaul_2_name)
})

test_that("with no exclusions the two monthly tables are identical", {
  # Backward compatibility for pipelines that never set the option
  # (mozambique, zanzibar): their outputs must not move.
  out <- run_summarize(exclude_dashboard_ids = NULL)
  expect_equal(out$all_monthly_summaries, out$monthly_summaries)
})

test_that("all_monthly_summaries matches the dashboard table's schema", {
  # export_geos() binds three countries on a fixed key; a schema drift here
  # silently drops a country from coasts.peskas.org.
  out <- run_summarize(exclude_dashboard_ids = "drop")
  expect_equal(names(out$all_monthly_summaries), names(out$monthly_summaries))
})

test_that("the new table cannot be confused with the old one by prefix", {
  # cloud_object_name() resolves by string prefix then takes max(updated), so
  # `_monthly_summaries_all` would also match a `_monthly_summaries` read
  # (model_fishery_metrics() does exactly that) and could return the wrong file.
  expect_false(startsWith("all_monthly_summaries", "monthly_summaries"))
})
