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
    surveys = list(summaries = list(file_prefix = "testland-summaries")),
    metadata = list(
      fishbase = list(taxa_enriched = list(file_prefix = "taxa-enriched"))
    )
  )
}

# Two species rows for one taxon code, as enrich_taxa() writes them.
fake_enriched <- function() {
  data.frame(
    alpha3_code = "TUN",
    species_found = c("Thunnus albacares", "Thunnus obesus"),
    vulnerability_fishing = c(40, 60),
    food_troph = c(4.2, 4.4),
    diet_troph = NA_real_,
    demers_pelag = "pelagic-oceanic",
    class = "Teleostei",
    iucn_code = c("LC", "VU"),
    cites_code = NA_character_,
    length_maturity_tl = c(100, 110),
    length_optimum_tl = c(120, 130)
  )
}

# Runs summarize_data() with the cloud boundary mocked out, and returns every
# table it produced, keyed by name: the country-bucket parquet files plus the
# coasts-bucket uploads.
run_summarize <- function(exclude_dashboard_ids, validated = fake_validated(), conf = fake_conf()) {
  withr::local_dir(withr::local_tempdir())

  uploaded <- character(0)
  coasts_uploads <- list()

  testthat::local_mocked_bindings(
    read_config = function(...) conf,
    resolve_storage_opts = function(...) list(bucket = "b"),
    download_parquet_from_cloud = function(prefix, ...) {
      if (prefix == "asfis") {
        return(data.frame(
          alpha3_code = "TUN",
          scientific_name = "Thunnus albacares",
          english_name = "Yellowfin tuna"
        ))
      }
      if (prefix %in% c("taxa-enriched", "taxa-fishbase-enriched")) {
        return(fake_enriched())
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

# One trip landing a species in two length classes (Zanzibar and Mozambique
# store a class per row), plus a trip landing two species.
fake_length_classes <- function() {
  data.frame(
    survey_id = "keep",
    gaul_2_name = "Kilifi",
    trip_id = c("a", "a", "b", "b"),
    landing_date = lubridate::ymd("2024-01-15"),
    gear = "gillnet",
    trip_duration_hrs = 5,
    n_fishers = 2,
    n_catch = 2L,
    length_cm = c(12.5, 32.5, 22.5, 22.5),
    catch_taxon = c("TUN", "TUN", "TUN", "SKJ"),
    scientific_name = c(rep("Thunnus albacares", 3), "Katsuwonus pelamis"),
    catch_kg = c(1, 5, 2, 2),
    catch_price = NA_real_,
    tot_catch_kg = c(6, 6, 4, 4),
    tot_catch_price = c(600, 600, 1000, 1000)
  )
}

taxa_value <- function(out, taxon, metric) {
  out$taxa_summaries |>
    dplyr::filter(.data$catch_taxon == taxon, .data$metric == !!metric, !is.na(.data$value)) |>
    dplyr::pull("value")
}

test_that("a species' catch adds up every length class of a trip", {
  out <- run_summarize(NULL, validated = fake_length_classes())

  # Trip a: 1 + 5 kg; trip b: 2 kg. Summing only the first row gave 1 + 2.
  expect_equal(taxa_value(out, "Thunnus albacares", "catch_kg"), 8)
  # Weighted by catch: (12.5 * 1 + 32.5 * 5 + 22.5 * 2) / 8.
  expect_equal(taxa_value(out, "Thunnus albacares", "mean_length"), 27.5)
})

test_that("a trip price is a species price only when the trip landed one species", {
  out <- run_summarize(NULL, validated = fake_length_classes())

  # Only trip a counts: 600 for 6 kg. Trip b's 1000 covers two species.
  expect_equal(taxa_value(out, "Thunnus albacares", "price_kg"), 100)
  expect_length(taxa_value(out, "Katsuwonus pelamis", "price_kg"), 0)

  # A catch row with its own value (Kenya) is used as it is.
  priced <- fake_length_classes() |> dplyr::mutate(catch_price = .data$catch_kg * 50)
  out <- run_summarize(NULL, validated = priced)
  expect_equal(taxa_value(out, "Katsuwonus pelamis", "price_kg"), 50)
})

test_that("length summaries bin lengths and count the trips behind them", {
  out <- run_summarize(NULL, validated = fake_length_classes())
  tun <- dplyr::filter(out$length_summaries, .data$catch_taxon == "Thunnus albacares")

  expect_equal(tun$length_min, c(10, 20, 30))
  expect_equal(tun$length_max, c(15, 25, 40))
  expect_equal(tun$catch_kg, c(1, 2, 5))
  expect_equal(unique(tun$n_trips), 2)
})

test_that("taxa traits summarise a code's species and name every landed taxon", {
  out <- run_summarize(NULL, validated = fake_length_classes())
  traits <- out$taxa_traits

  expect_setequal(traits$catch_taxon, c("Thunnus albacares", "Katsuwonus pelamis"))
  tun <- dplyr::filter(traits, .data$alpha3_code == "TUN")
  expect_equal(tun$n_species, 2)
  expect_equal(tun$vulnerability, 50)
  expect_equal(c(tun$vulnerability_min, tun$vulnerability_max), c(40, 60))
  expect_equal(tun$n_threatened, 1)
  # Two species: no single IUCN category or length stands for the code.
  expect_true(is.na(tun$iucn_code))
  expect_true(is.na(tun$length_maturity_cm))
  expect_true(is.na(tun$length_optimum_cm))
  # SKJ has no FishBase row here: named, with no traits.
  expect_true(is.na(dplyr::filter(traits, .data$alpha3_code == "SKJ")$n_species))
})

test_that("a one-species code carries its IUCN category and lengths", {
  traits <- summarise_taxa_traits(dplyr::filter(fake_enriched(), .data$species_found == "Thunnus obesus"))
  expect_equal(traits$iucn_code, "VU")
  expect_equal(c(traits$length_maturity_cm, traits$length_optimum_cm), c(110, 130))

  # An optimum below maturity, or with no maturity to check it against, is dropped.
  below <- dplyr::filter(fake_enriched(), .data$species_found == "Thunnus obesus") |>
    dplyr::mutate(length_optimum_tl = 90)
  expect_true(is.na(summarise_taxa_traits(below)$length_optimum_cm))
  unchecked <- dplyr::filter(fake_enriched(), .data$species_found == "Thunnus obesus") |>
    dplyr::mutate(length_maturity_tl = NA_real_)
  expect_true(is.na(summarise_taxa_traits(unchecked)$length_optimum_cm))
})

test_that("gear taxa summaries add up each gear's catch and trips per taxon", {
  out <- run_summarize(NULL, validated = fake_length_classes())
  g <- out$gear_taxa_summaries |> dplyr::arrange(.data$catch_taxon)
  expect_equal(g$catch_taxon, c("Katsuwonus pelamis", "Thunnus albacares"))
  expect_equal(g$catch_kg, c(2, 8))
  expect_equal(g$n_trips, c(1, 2))
})

test_that("a pipeline config without the traits key still summarises", {
  conf <- fake_conf()
  conf$metadata <- NULL
  out <- run_summarize(NULL, validated = fake_length_classes(), conf = conf)
  expect_equal(dplyr::filter(out$taxa_traits, .data$alpha3_code == "TUN")$n_species, 2)
})

test_that("a taxa_enriched table from before 4.15.0 still summarises", {
  old <- dplyr::select(
    fake_enriched(),
    -"class", -"iucn_code", -"cites_code", -"length_maturity_tl", -"length_optimum_tl"
  )
  traits <- summarise_taxa_traits(old)
  expect_equal(traits$vulnerability, 50)
  expect_equal(traits$n_threatened, 0)
})
