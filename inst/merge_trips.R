pars <- read_config()

catch_events <-
  mdb_collection_pull(
    connection_string = pars$storage$mongodb$tracks_app$connection_string,
    collection_name = pars$storage$mongodb$tracks_app$collection$catch_events,
    db_name = pars$storage$mongodb$tracks_app$database_name
  ) |>
  dplyr::as_tibble() |>
  dplyr::mutate(date = as.Date(date), tripId = as.character(tripId)) |>
  dplyr::select(
    id = tripId,
    imei,
    landing_date = date,
    catch_outcome,
    fish_group = fishGroup,
    catch_kg = quantity
  )

# list of trips from tracks app imeis

boats_trips <-
  unique(catch_events$imei) |>
  purrr::set_names() |>
  purrr::map(
    get_trips,
    token = pars$pds$token,
    secret = pars$pds$secret,
    dateFrom = "2025-01-01",
    dateTo = Sys.Date()
  ) |>
  dplyr::bind_rows(.id = "imei") |>
  dplyr::mutate(
    landing_date = as.Date(Ended),
    duration_hrs = `Duration (Seconds)` / 3600,
    Trip = as.character(Trip)
  ) |>
  janitor::clean_names() |>
  dplyr::select(
    imei,
    trip,
    started,
    ended,
    landing_date,
    duration_hrs,
    range_meters,
    distance_meters
  ) |>
  dplyr::filter(landing_date %in% c(unique(catch_events$landing_date)))


we <-
  catch_events |>
  dplyr::left_join(boats_trips, by = c("imei", "landing_date")) |>
  dplyr::filter(!is.na(trip)) |>
  dplyr::distinct() |>
  View()

unique(we$trip)
