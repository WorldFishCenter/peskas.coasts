#' Prepare Predicted Tracks for Effort Aggregation
#'
#' @description
#' Enriches predicted fishing track points with time-based effort, H3 cell
#' assignment, and per-trip total hours (used downstream for the fidelity
#' metric). For each trip the inter-ping time interval (`dt_hours`) is computed
#' from consecutive timestamps, then limited by `max_gap_hours` so that a silent
#' device does not read as hours of fishing.
#'
#' @details
#' Each interval is credited in full to the cell holding the **later** ping, so
#' a long gap does not merely inflate the effort total — it puts unobserved time
#' in one particular hexagon, wherever the vessel happened to resurface.
#'
#' Devices report every few seconds (median interval across a 3,814-trip sample:
#' 7 seconds, with 95% of intervals under 1.5 minutes), so anything beyond a few
#' minutes means the device stopped reporting rather than the vessel slowing
#' down. Long gaps are rare and dominant at the same time: in that sample
#' intervals above one hour were 0.6% of the data and half of all fishing hours.
#' The default 15-minute limit is roughly 130 missed pings — ample room for a
#' genuine signal glitch — while holding any single dropout to a quarter of an
#' hour in one cell.
#'
#' Points whose coordinates are not usable positions ([valid_coordinates()]) are
#' dropped before any of this, and counted in a warning. A ping with no position
#' is a ping the grid has nowhere to put, so it is treated as one the device
#' never sent: the time it covers passes to the next ping that does have a
#' position, and `max_gap_hours` limits it there like any other silence.
#'
#' @param df Data frame of predicted fishing points with columns `trip`,
#'   `timestamp`, `latitude`, `longitude`.
#' @param h3_res Integer (0–15). H3 resolution for cell assignment.
#' @param max_gap_hours Numeric. Longest inter-ping interval treated as fishing
#'   time. Default is `0.25` (15 minutes).
#' @param gap_policy How intervals above `max_gap_hours` are handled.
#'   `"cap"` (default) counts `max_gap_hours` of them, assuming the vessel kept
#'   fishing where it was next seen; `"drop"` counts none of them, so effort is
#'   only observed time. Changing either setting changes every effort figure the
#'   pipeline produces.
#'
#' @return The input data frame with additional columns:
#'   \describe{
#'     \item{`year`}{Integer year extracted from `timestamp`.}
#'     \item{`dt_hours`}{Interval in hours to the previous ping within the
#'       same trip (0 for the first ping), limited per `gap_policy`.}
#'     \item{`h3_index`}{H3 cell identifier at resolution `h3_res`.}
#'   }
#'
#' @keywords internal
prepare_tracks_for_effort <- function(
  df,
  h3_res,
  max_gap_hours = 0.25,
  gap_policy = c("cap", "drop")
) {
  gap_policy <- match.arg(gap_policy)

  # Dropped here, before the intervals are measured, rather than after the H3
  # column is added. An unusable ping still carries a good timestamp, but there
  # is no cell to credit its interval to; removing it first hands that time to
  # the next ping that does have a position, where `max_gap_hours` limits it in
  # the ordinary way. Removing it afterwards would measure an interval into a
  # row about to be discarded, and silently lose that stretch of the trip.
  usable <- valid_coordinates(df$longitude, df$latitude)

  if (!all(usable)) {
    logger::log_warn(
      "Dropping {sum(!usable)} of {nrow(df)} point(s) whose coordinates are",
      " not usable positions; their time is credited to the next good ping"
    )
    df <- df[usable, , drop = FALSE]
  }

  df |>
    dplyr::mutate(
      trip = as.character(.data$trip),
      timestamp = lubridate::as_datetime(.data$timestamp),
      year = lubridate::year(.data$timestamp)
    ) |>
    dplyr::arrange(.data$trip, .data$timestamp) |>
    dplyr::group_by(.data$trip) |>
    dplyr::mutate(
      dt_hours = as.numeric(
        difftime(.data$timestamp, dplyr::lag(.data$timestamp), units = "hours")
      ),
      dt_hours = dplyr::coalesce(.data$dt_hours, 0),
      dt_hours = if (gap_policy == "cap") {
        pmin(.data$dt_hours, max_gap_hours)
      } else {
        dplyr::if_else(.data$dt_hours > max_gap_hours, 0, .data$dt_hours)
      }
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      h3_index = h3_index_chunked(
        cbind(.data$longitude, .data$latitude),
        res = h3_res
      )
    )
}


#' Flag Coordinates H3 Can Be Trusted With
#'
#' @description
#' Tests longitude/latitude pairs for the two ways a position can be unusable:
#' not a number at all, or outside the range it is meant to occupy.
#'
#' @details
#' [h3jsr::point_to_cell()] fails in opposite directions on the two, and the
#' quiet one is the more damaging.
#'
#' A non-finite coordinate (`NA`, `NaN`, `Inf`) raises *Latitude or longitude
#' arguments were outside of acceptable range*, which aborts the entire run --- a
#' single bad ping in a batch of millions, and nothing is written, because the
#' aggregation state is only uploaded at the end.
#'
#' An out-of-range *finite* coordinate raises nothing at all. Despite the
#' wording of that error, H3 does not reject these: it wraps them and returns a
#' perfectly ordinary-looking cell. Latitude is where this does damage, because
#' the wrap is not a wrap at all --- it reflects over the pole and swings
#' longitude by 180°. Measured at resolution 9, a point off the Tanzanian coast
#' with a corrupt latitude of 91 comes back as a cell at 89°N, 141°W, in the
#' Arctic Ocean some 10,000 km away; a latitude of 900 lands in the mid-Pacific.
#' Either would enter the grid as a real hexagon carrying real fishing hours,
#' with no error and no log line to explain it.
#'
#' Longitude out of range is the harmless case --- longitude is genuinely cyclic,
#' so 181° resolves to 179°W, which is the correct point. It is rejected anyway:
#' a device reporting outside ±180 is reporting something wrong, and the value
#' being recoverable is not a reason to trust the rest of that ping.
#'
#' @param longitude,latitude Numeric vectors of equal length.
#'
#' @return Logical vector, `TRUE` where the pair is a position H3 can be given.
#'
#' @seealso [h3_index_chunked()], [prepare_tracks_for_effort()]
#'
#' @keywords internal
valid_coordinates <- function(longitude, latitude) {
  is.finite(longitude) &
    is.finite(latitude) &
    latitude >= -90 &
    latitude <= 90 &
    longitude >= -180 &
    longitude <= 180
}


#' Assign H3 Cells in Bounded Blocks
#'
#' @description
#' Maps coordinates to H3 cell identifiers a block at a time, so that peak
#' memory is set by the block size rather than by the number of points.
#'
#' @details
#' [h3jsr::point_to_cell()] is a bridge to a JavaScript H3 build, not compiled
#' code: it pushes the whole coordinate set into a V8 heap as one object per
#' point, loops over them in JavaScript, and ships the strings back. The heap
#' that requires grows with the input and is not returned to the operating
#' system, so a single call over a full backfill is an out-of-memory kill rather
#' than a slow run.
#'
#' Splitting the call costs nothing. Measured at H3 resolution 9 on 4M points,
#' peak resident memory falls monotonically with block size --- 2.69 GB in one
#' call, 1.75 GB at 100,000, 1.68 GB at 50,000 --- while wall time stays flat
#' (21.7s against 20.5s), because the work per point is unchanged and only the
#' number of round trips differs. The default of 100,000 sits at the point where
#' the memory curve has flattened but the round trips are still few.
#'
#' Blocking does not change the result: each point's cell depends only on its
#' own coordinates, so the concatenated output is identical to the single call
#' (verified byte-for-byte up to 4M points). The one deliberate departure is the
#' empty input, which [h3jsr::point_to_cell()] errors on rather than answering;
#' here it returns `character(0)`.
#'
#' Coordinates are assumed to have been checked already --- see
#' [valid_coordinates()] for what happens if they have not been.
#'
#' @param xy Two-column numeric matrix of longitude and latitude, in that order.
#'   Every row must be a valid position; blocking does not make
#'   [h3jsr::point_to_cell()] any more tolerant of one that is not.
#' @param res Integer (0--15). H3 resolution.
#' @param chunk Integer. Maximum points per call. Default `100000`.
#'
#' @return Character vector of H3 cell identifiers, one per row of `xy`.
#'
#' @seealso [prepare_tracks_for_effort()]
#'
#' @keywords internal
h3_index_chunked <- function(xy, res, chunk = 100000L) {
  n <- nrow(xy)

  # `point_to_cell()` does not survive an empty input --- it fails building its
  # own working frame ("arguments imply differing number of rows: 2, 0") --- and
  # an empty input is reachable whenever a trip's positions are all unusable.
  if (n == 0) {
    return(character(0))
  }

  if (n <= chunk) {
    return(h3jsr::point_to_cell(xy, res = res))
  }

  blocks <- split(seq_len(n), ceiling(seq_len(n) / chunk))

  unlist(
    lapply(blocks, function(i) {
      h3jsr::point_to_cell(xy[i, , drop = FALSE], res = res)
    }),
    use.names = FALSE
  )
}


#' Index Trips Whose Track Duplicates Another Trip
#'
#' @description
#' Flags trips whose GPS track is identical to another trip's, so the same
#' fishing event is not counted once per trip identifier.
#'
#' @details
#' Pelagic Data Systems re-segments trips retroactively. A trip identifier can
#' be **retired** (its points reappear under a new identifier) or **reassigned**
#' (its points move to a new trip while the old identifier comes to describe a
#' different window). Since [predict_pds_tracks()] writes one file per trip,
#' the predicted-track store ends up holding several identifiers that describe
#' the very same track, and [aggregate_pds_effort()] counts each of them as a
#' separate vessel: fishing hours, unique trips and pings are multiplied by the
#' number of copies, while `n_active_days` (a set union of dates) is not, so
#' the per-day rates in [add_cell_effort_metrics()] explode.
#'
#' Each trip is reduced to a fingerprint of its `(timestamp, latitude,
#' longitude)` triples, order-independent so that two files listing the same
#' points always agree. Trips sharing a fingerprint -- with each other, or with
#' a trip aggregated in an earlier run (`seen`) -- collapse to one survivor: a
#' trip listed in `prefer` wins, then the trip with the most points, then the
#' lowest identifier, so the choice is stable across runs.
#'
#' A trip already in the effort store always outranks a new duplicate of it, so
#' that a run cannot move effort between trips that are known to be the same.
#' When a retired identifier is aggregated before the trip that supersedes it,
#' the retired one therefore survives and its cells keep `gear`/`country` =
#' `"unknown"`; the deletion pass in [predict_pds_tracks()] is what stops that
#' happening.
#'
#' @param df Prepared fishing points with one row per ping and columns `trip`,
#'   `timestamp`, `latitude`, `longitude`.
#' @param prefer Character vector of trip identifiers to keep in preference to
#'   others -- normally the trips PDS still lists, which are the ones carrying
#'   device metadata. Defaults to none.
#' @param seen Optional data frame of trips aggregated in earlier runs, with
#'   columns `trip` and `fingerprint`.
#'
#' @return A tibble with one row per trip in `df`: `trip`, `fingerprint`,
#'   `n_points`, `duplicate_of` (the surviving trip, `NA` when the trip is
#'   itself the survivor) and `keep`.
#'
#' @seealso [drop_overlapping_pings()], [aggregate_pds_effort()]
#'
#' @keywords internal
index_trip_duplicates <- function(df, prefer = character(0), seen = NULL) {
  trips <- df |>
    dplyr::summarise(
      .by = "trip",
      n_points = dplyr::n(),
      # Ordered on the raw columns rather than on the pasted strings: same
      # order-independent digest, but a numeric sort instead of a locale-aware
      # comparison of five million strings on a full rebuild.
      fingerprint = rlang::hash(paste(
        .data$timestamp,
        .data$latitude,
        .data$longitude
      )[order(.data$timestamp, .data$latitude, .data$longitude)])
    ) |>
    dplyr::mutate(preferred = .data$trip %in% prefer) |>
    dplyr::arrange(
      dplyr::desc(.data$preferred),
      dplyr::desc(.data$n_points),
      .data$trip
    ) |>
    dplyr::mutate(.by = "fingerprint", survivor = dplyr::first(.data$trip))

  # A trip aggregated in an earlier run always wins: its effort is already in
  # the store, and moving it to a different trip would gain nothing.
  if (!is.null(seen) && nrow(seen) > 0) {
    trips <- trips |>
      dplyr::left_join(
        seen |>
          dplyr::distinct(.data$fingerprint, .keep_all = TRUE) |>
          dplyr::select("fingerprint", seen_trip = "trip"),
        by = "fingerprint"
      ) |>
      dplyr::mutate(survivor = dplyr::coalesce(.data$seen_trip, .data$survivor))
  }

  trips |>
    dplyr::mutate(
      keep = .data$trip == .data$survivor,
      duplicate_of = dplyr::if_else(.data$keep, NA_character_, .data$survivor)
    ) |>
    dplyr::select("trip", "fingerprint", "n_points", "duplicate_of", "keep")
}


#' Drop GPS Pings Shared by More Than One Trip
#'
#' @description
#' Removes pings that appear under more than one trip identifier, keeping a
#' single copy.
#'
#' @details
#' Complements [index_trip_duplicates()], which only removes trips duplicated
#' in full: when PDS moves *part* of a track to another trip the two files
#' overlap without being identical, and the shared pings would otherwise
#' contribute their time twice.
#'
#' Pings are matched on the exact `(timestamp, latitude, longitude)` triple --
#' two vessels reporting the same position to the metre in the same second is
#' not a case that occurs in practice, and the triple is compared across trips
#' precisely because a retired trip carries no device metadata to group on. The
#' copy kept belongs to a trip listed in `prefer` where possible, then to the
#' lowest trip identifier, so the outcome does not depend on download order.
#'
#' `dt_hours` is left as computed within each trip: deduplicating before the
#' intervals were known would turn the removed pings into artificial gaps and
#' inflate the survivors.
#'
#' Only pings within the batch are compared, which is why
#' [aggregate_pds_effort()] recalls the trips already aggregated that could
#' share pings with it ([recall_overlapping_trips()]) rather than relying on
#' this function alone.
#'
#' @param df Prepared fishing points, as returned by
#'   [prepare_tracks_for_effort()].
#' @param prefer Character vector of trip identifiers whose copy of a shared
#'   ping should be kept. Defaults to none.
#'
#' @return `df` with shared pings removed. Row order is not preserved.
#'
#' @seealso [index_trip_duplicates()], [recall_overlapping_trips()],
#'   [aggregate_pds_effort()]
#'
#' @keywords internal
drop_overlapping_pings <- function(df, prefer = character(0)) {
  # Shared pings are a fraction of a percent of a batch, so the ordering that
  # decides which copy survives is established over just those rows: sorting
  # the whole frame would cost more than the deduplication it serves.
  #
  # `vec_duplicate_detect()` rather than `duplicated(ping) | duplicated(ping,
  # fromLast = TRUE)`: the two express the same thing, but `duplicated()` on a
  # *data frame* first rebuilds it as one list element per row
  # (`do.call(Map, ...)` inside `duplicated.data.frame`), and the base version
  # pays for that twice. Measured on this triple, that is 705 MB and 8.2s per
  # million rows against 50 MB and 0.04s -- survivable on a daily delta, ~18 GB
  # and an out-of-memory kill on a full backfill, after the tracks have already
  # been downloaded. The vctrs form is a single C-level pass and returns the
  # "duplicated anywhere, first occurrence included" flag directly.
  ping <- df[c("timestamp", "latitude", "longitude")]
  shared <- vctrs::vec_duplicate_detect(ping)

  if (!any(shared)) {
    return(df)
  }

  kept <- df[shared, ] |>
    dplyr::arrange(dplyr::desc(.data$trip %in% prefer), .data$trip) |>
    dplyr::distinct(
      .data$timestamp,
      .data$latitude,
      .data$longitude,
      .keep_all = TRUE
    )

  dplyr::bind_rows(df[!shared, ], kept)
}


#' Recall Aggregated Trips That May Share Pings With a Batch
#'
#' @description
#' Finds trips already in the effort store whose pings could also appear in the
#' files about to be read, so they can be withdrawn and re-read alongside them.
#'
#' @details
#' [drop_overlapping_pings()] can only compare pings it sees at once. When PDS
#' moves *part* of a track to a new trip, the two copies arrive in different
#' runs: the new trip is read while the old one sits in the store, the shared
#' pings are never compared, and their hours stay counted twice. Withdrawing the
#' stored trip and reading it again in the same batch puts both copies in front
#' of the deduplication.
#'
#' A candidate is a stored trip on the **same device** whose fishing overlaps
#' the incoming trip in time -- re-segmentation moves points within one vessel's
#' track, so nothing else can share a ping. Device and window come from the
#' stored trip listing, which means the recall costs no downloads to decide.
#' Trips PDS no longer lists carry no device metadata and so cannot be matched
#' this way; full copies of them are still caught by
#' [index_trip_duplicates()], and [predict_pds_tracks()] deletes their files
#' outright.
#'
#' @param files Character vector of object names about to be read. Trip
#'   identifiers are taken from the `trip_{id}_v{version}.parquet` names written
#'   by [predict_pds_tracks()].
#' @param trip_lookup Trip metadata with columns `trip`, `imei`, `started` and
#'   `ended`.
#' @param trips The registry of aggregated trips, with columns `trip`, `file`,
#'   `imei`, `first_timestamp` and `last_timestamp`.
#'
#' @return A tibble of `trip` and `file` for the trips to withdraw and re-read,
#'   empty when there is nothing to recall.
#'
#' @seealso [drop_overlapping_pings()], [aggregate_pds_effort()]
#'
#' @keywords internal
recall_overlapping_trips <- function(files, trip_lookup, trips) {
  empty <- tibble::tibble(trip = character(0), file = character(0))

  if (length(files) == 0 || nrow(trips) == 0) {
    return(empty)
  }

  incoming <- tibble::tibble(
    trip = predicted_track_trip_id(files)
  ) |>
    dplyr::filter(!is.na(.data$trip)) |>
    dplyr::inner_join(trip_lookup, by = "trip") |>
    dplyr::filter(
      !is.na(.data$imei),
      !is.na(.data$started),
      !is.na(.data$ended)
    ) |>
    dplyr::select(incoming_trip = "trip", "imei", "started", "ended")

  if (nrow(incoming) == 0) {
    return(empty)
  }

  # Narrowed to the batch's own span before joining on the device. Joining
  # first would build every pairing of a device's entire history with its new
  # trips — a cross product that grows for as long as the pipeline runs — only
  # to discard nearly all of it; a batch covers a day or two, so this leaves a
  # handful of candidates per device to check properly.
  trips |>
    dplyr::filter(
      !is.na(.data$imei),
      .data$last_timestamp >= min(incoming$started),
      .data$first_timestamp <= max(incoming$ended)
    ) |>
    dplyr::inner_join(
      incoming,
      by = "imei",
      relationship = "many-to-many"
    ) |>
    dplyr::filter(
      .data$trip != .data$incoming_trip,
      .data$first_timestamp <= .data$ended,
      .data$last_timestamp >= .data$started
    ) |>
    dplyr::distinct(.data$trip, .data$file)
}


#' Withdraw Trips From the Aggregation State
#'
#' @description
#' Takes a set of trips out of the effort store, together with the trips that
#' were dropped as duplicates of them, and forgets the objects they all came
#' from so those are read again.
#'
#' @details
#' A trip only survived because the trips duplicating it were discarded in its
#' favour. Withdrawing it alone would leave those copies unrepresented and
#' unread, so they come out too and the whole set is judged again together.
#'
#' The three parts of the state have to move in step -- effort rows, registry
#' entries and manifest lines all describe the same trips -- which is why they
#' are carried as one object and withdrawn in one call rather than filtered
#' three times at each site that needs it.
#'
#' @param state The aggregation state, as returned by
#'   [load_aggregation_state()] or [empty_aggregation_state()].
#' @param seed Character vector of trip identifiers to withdraw.
#' @param extra_files Object names to forget regardless of which trips they
#'   held -- used for files that have already been deleted or replaced upstream.
#'
#' @return A list with the updated `state`, the `trips` withdrawn and the
#'   `files` to read again.
#'
#' @seealso [aggregate_pds_effort()]
#'
#' @keywords internal
withdraw_trips <- function(state, seed, extra_files = character(0)) {
  affected <- union(
    seed,
    state$registry$trip[state$registry$duplicate_of %in% seed]
  )
  reread <- unique(c(
    extra_files,
    state$registry$file[state$registry$trip %in% affected]
  ))

  state$effort <- dplyr::filter(state$effort, !(.data$trip %in% affected))
  state$registry <- dplyr::filter(state$registry, !(.data$trip %in% affected))
  state$manifest <- dplyr::filter(state$manifest, !(.data$name %in% reread))

  list(state = state, trips = affected, files = reread)
}


#' Take the Next Batch of Predicted Track Files
#'
#' @description
#' Takes the first `max_files` objects of the queue in trip start order, so that
#' a backlog is worked through a batch at a time instead of being held in memory
#' at once.
#'
#' @details
#' The cap exists because peak memory is set by the number of points held at
#' once, and a store that has just gained a region's full history is not the
#' small daily delta the pipeline is otherwise sized for. [aggregate_pds_effort()]
#' calls this once per batch until its queue is empty.
#'
#' Ordering is by the trip's start time rather than by object name, and this is
#' what keeps the cap cheap rather than merely correct. Both orders are
#' deterministic, but [recall_overlapping_trips()] pre-filters the aggregated
#' registry on the batch's own minimum and maximum span before joining on the
#' device. A batch drawn in name order is scattered across every year the store
#' covers, so that span is the whole store, the pre-filter stops discriminating,
#' and the many-to-many join on device is left to pair each device's entire
#' history against its incoming trips. Ordering by time keeps a batch inside a
#' narrow window, which is the shape that pre-filter was written for.
#'
#' Correctness does not rest on the order. A capped batch is judged exactly as a
#' full one is: the cap is applied *before* the recall step, so trips already
#' aggregated that might share pings with it are still pulled back in, and the
#' grid is re-derived from the whole store every run --- which is what makes the
#' result independent of how trips were batched.
#'
#' @param files Character vector of candidate object names.
#' @param trip_lookup Trip metadata with columns `trip` and `started`.
#' @param max_files Integer, or `NULL` to take everything.
#'
#' @return A character vector of at most `max_files` object names.
#'
#' @seealso [recall_overlapping_trips()], [aggregate_pds_effort()]
#'
#' @keywords internal
select_effort_batch <- function(files, trip_lookup, max_files) {
  if (is.null(max_files) || length(files) <= max_files) {
    return(files)
  }

  ordered <- tibble::tibble(
    name = files,
    trip = predicted_track_trip_id(files)
  ) |>
    dplyr::left_join(
      dplyr::select(trip_lookup, "trip", "started"),
      by = "trip"
    ) |>
    # Trips carrying no metadata sort last (arrange puts NA last), and the name
    # breaks ties, so the same backlog is always consumed in the same order.
    dplyr::arrange(.data$started, .data$name)

  ordered$name[seq_len(max_files)]
}


#' Download a Batch of Predicted Track Files
#'
#' @description
#' Fetches predicted track objects in parallel and reports which of them could
#' not be read, so the caller does not record a file as aggregated when its
#' contents never arrived.
#'
#' @param names Character vector of cloud object names.
#' @param provider Cloud storage provider key.
#' @param options Named list of cloud storage options for the PDS bucket.
#'
#' @return A list with `tracks` (all points read, with the object each came from
#'   in `source_file`) and `failed` (names that could not be read).
#'
#' @keywords internal
download_predicted_files <- function(names, provider, options) {
  workers <- parallel::detectCores() - 1
  logger::log_info(
    "Downloading {length(names)} predicted track files with {workers} workers..."
  )
  future::plan(future::multisession, workers = workers)
  on.exit(future::plan(future::sequential), add = TRUE)

  results <- furrr::future_map(
    names,
    function(f) {
      local_file <- file.path(tempdir(), basename(f))
      tryCatch(
        {
          download_cloud_file(
            name = f,
            provider = provider,
            options = options,
            file = local_file
          )
          data <- arrow::read_parquet(local_file)
          unlink(local_file)
          # Carried so a trip can be traced back to the object it came from,
          # and withdrawn from the store when that object changes.
          data$source_file <- rep(f, nrow(data))
          list(name = f, data = data)
        },
        error = function(e) {
          logger::log_warn("Skipping {f}: {conditionMessage(e)}")
          list(name = f, data = NULL)
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  failed <- purrr::map_chr(
    purrr::keep(results, \(r) is.null(r$data)),
    "name"
  )

  if (length(failed) > 0) {
    logger::log_warn(
      "{length(failed)} file(s) could not be read; they stay out of the",
      " manifest so the next run tries them again"
    )
  }

  # The per-file frames and the bound result would otherwise be live at the
  # same time, doubling peak memory at the worst possible moment. Dropping the
  # wrapper list first leaves `bind_rows()` holding the only other reference,
  # so the parts are freed as it consumes them.
  parts <- purrr::map(results, "data")
  rm(results)

  list(
    tracks = dplyr::bind_rows(parts),
    failed = failed
  )
}


#' Summarise Prepared Tracks into Per-Trip Cell-Day Effort
#'
#' @description
#' Reduces prepared fishing points to the table [aggregate_pds_effort()] keeps
#' as its source of truth: one row per trip, cell, year, gear, country and
#' calendar day.
#'
#' @details
#' This is the smallest summary the H3 grid can still be derived from in full.
#' Keeping `trip` means `unique_trips` can be counted rather than added up, and
#' keeping `date` means active days can be counted rather than unioned from a
#' list-column; keeping both means a trip's contribution can be *removed* when
#' PDS revises it, which a grid of running totals cannot support. It is about
#' one row per trip and cell visited on a day -- a few hundred thousand rows
#' against five million pings -- so a rebuild reads it in seconds instead of
#' re-downloading every predicted track.
#'
#' @param df Prepared fishing points, as returned by
#'   [prepare_tracks_for_effort()], carrying `gear` and `country`.
#'
#' @return A tibble with columns `trip`, `h3_index`, `year`, `gear`, `country`,
#'   `date`, `cell_hours` and `cell_pings`.
#'
#' @seealso [derive_effort_grid()], [aggregate_pds_effort()]
#'
#' @keywords internal
build_trip_effort <- function(df) {
  df |>
    dplyr::mutate(date = as.Date(.data$timestamp)) |>
    dplyr::summarise(
      .by = c("trip", "h3_index", "year", "gear", "country", "date"),
      cell_hours = sum(.data$dt_hours, na.rm = TRUE),
      cell_pings = dplyr::n()
    )
}


#' Derive the H3 Effort Grid from Per-Trip Effort
#'
#' @description
#' Collapses the per-trip cell-day table built by [build_trip_effort()] into the
#' published H3 grid: one row per `(h3_index, year, gear, country)`.
#'
#' @details
#' Every column is computed from the whole store in one pass, so the result does
#' not depend on how the trips were batched. `unique_trips` counts distinct
#' trips instead of summing per-batch counts, and a trip whose fishing crosses
#' midnight on New Year contributes to both year rows without being counted
#' twice in either. Fidelity is a trip's share of its own total fishing hours
#' spent in a cell, with the total taken across every cell and year the trip
#' touched.
#'
#' @param trip_effort Per-trip cell-day effort, as returned by
#'   [build_trip_effort()].
#'
#' @return A tibble with one row per `(h3_index, year, gear, country)` and the
#'   grid columns documented in [aggregate_pds_effort()].
#'
#' @seealso [build_trip_effort()], [aggregate_pds_effort()]
#'
#' @keywords internal
derive_effort_grid <- function(trip_effort) {
  trip_totals <- trip_effort |>
    dplyr::summarise(
      .by = "trip",
      trip_total = sum(.data$cell_hours, na.rm = TRUE)
    )

  cell_fidelity <- trip_effort |>
    dplyr::summarise(
      .by = c("trip", "h3_index", "year", "gear", "country"),
      cell_hours = sum(.data$cell_hours, na.rm = TRUE)
    ) |>
    dplyr::left_join(trip_totals, by = "trip") |>
    dplyr::mutate(
      trip_share = dplyr::if_else(
        .data$trip_total > 0,
        .data$cell_hours / .data$trip_total,
        NA_real_
      )
    ) |>
    dplyr::summarise(
      .by = c("h3_index", "year", "gear", "country"),
      avg_fidelity_sum = sum(.data$trip_share, na.rm = TRUE),
      n_trips_for_fidelity = sum(!is.na(.data$trip_share))
    )

  trip_effort |>
    dplyr::summarise(
      .by = c("h3_index", "year", "gear", "country"),
      fishing_hours = sum(.data$cell_hours, na.rm = TRUE),
      unique_trips = dplyr::n_distinct(.data$trip),
      active_dates = list(sort(unique(.data$date))),
      fishing_pings = sum(.data$cell_pings),
      n_active_days = dplyr::n_distinct(.data$date),
      first_active_date = min(.data$date),
      last_active_date = max(.data$date)
    ) |>
    dplyr::left_join(
      cell_fidelity,
      by = c("h3_index", "year", "gear", "country")
    ) |>
    dplyr::select(
      "h3_index",
      "year",
      "gear",
      "country",
      "fishing_hours",
      "unique_trips",
      "active_dates",
      "fishing_pings",
      "avg_fidelity_sum",
      "n_trips_for_fidelity",
      "n_active_days",
      "first_active_date",
      "last_active_date"
    )
}


#' Name the Objects the Aggregation State Lives In
#'
#' @description
#' The four side files [aggregate_pds_effort()] keeps beside the grid, named
#' from the grid's own prefix so that grids at different H3 resolutions keep
#' separate state.
#'
#' @param grid_prefix Cloud prefix of the grid, including the `_r{res}` suffix.
#'
#' @return A named list of object names: `manifest`, `registry`, `effort` and
#'   `settings`.
#'
#' @keywords internal
aggregation_state_names <- function(grid_prefix) {
  list(
    manifest = paste0(grid_prefix, "/aggregated_manifest.rds"),
    registry = paste0(grid_prefix, "/aggregated_trips.rds"),
    effort = paste0(grid_prefix, "/aggregated_trip_effort.parquet"),
    settings = paste0(grid_prefix, "/aggregated_settings.rds")
  )
}


#' Normalise the Effort Settings for Comparison
#'
#' @description
#' Puts the gap-handling settings in a canonical form, so that a run passing
#' `1L` where `1` was stored is not mistaken for a change of policy and does not
#' set off a rebuild.
#'
#' @param settings Named list with `max_gap_hours` and `gap_policy`.
#'
#' @return The same two fields, as a double and a character.
#'
#' @keywords internal
normalise_effort_settings <- function(settings) {
  list(
    max_gap_hours = as.numeric(settings$max_gap_hours),
    gap_policy = as.character(settings$gap_policy)
  )
}


#' An Empty Aggregation State
#'
#' @description
#' The state a run starts from when there is nothing usable to carry forward:
#' typed but empty, so the first batch binds onto columns of the right kind.
#'
#' @param settings Named list of the effort settings this state will be built
#'   under.
#'
#' @return An aggregation state: `manifest`, `registry`, `effort`, `settings`.
#'
#' @seealso [load_aggregation_state()], [aggregation_state_ok()]
#'
#' @keywords internal
empty_aggregation_state <- function(settings) {
  list(
    manifest = tibble::tibble(
      name = character(0),
      updated = lubridate::as_datetime(character(0))
    ),
    registry = tibble::tibble(
      trip = character(0),
      file = character(0),
      imei = character(0),
      first_timestamp = lubridate::as_datetime(character(0)),
      last_timestamp = lubridate::as_datetime(character(0)),
      fingerprint = character(0),
      n_points = integer(0),
      duplicate_of = character(0)
    ),
    effort = tibble::tibble(
      trip = character(0),
      h3_index = character(0),
      year = numeric(0),
      gear = character(0),
      country = character(0),
      date = as.Date(character(0)),
      cell_hours = numeric(0),
      cell_pings = integer(0)
    ),
    settings = normalise_effort_settings(settings)
  )
}


#' Load the Incremental Aggregation State
#'
#' @description
#' Reads back the manifest, trip registry, per-trip effort and settings that
#' [upload_aggregation_state()] wrote.
#'
#' @details
#' The four parts describe each other and are only usable together: a manifest
#' whose effort store failed to download would have the pipeline skip files
#' whose effort is nowhere. Anything missing therefore yields `NULL`, and the
#' caller starts from [empty_aggregation_state()] — which is also how the state
#' comes into being on the very first run.
#'
#' @param names Object names, as returned by [aggregation_state_names()].
#' @param provider Cloud storage provider key.
#' @param options Named list of cloud storage options.
#'
#' @return An aggregation state, or `NULL` if any part could not be read.
#'
#' @seealso [upload_aggregation_state()], [aggregation_state_ok()]
#'
#' @keywords internal
load_aggregation_state <- function(names, provider, options) {
  read_part <- function(name, reader) {
    local_file <- file.path(tempdir(), basename(name))
    download_cloud_file(
      name = name,
      provider = provider,
      options = options,
      file = local_file
    )
    reader(local_file)
  }

  tryCatch(
    list(
      manifest = read_part(names$manifest, readr::read_rds),
      registry = read_part(names$registry, readr::read_rds),
      settings = read_part(names$settings, readr::read_rds),
      effort = read_part(names$effort, arrow::read_parquet)
    ),
    error = function(e) {
      logger::log_info("No usable aggregation state found")
      NULL
    }
  )
}


#' Is a Loaded Aggregation State Usable?
#'
#' @description
#' Checks that every part carries the columns the aggregation reads, so that a
#' state written by an older schema is rebuilt rather than failing partway
#' through the derivation.
#'
#' @param state An aggregation state, or `NULL`.
#'
#' @return `TRUE` when the state can be built on.
#'
#' @keywords internal
aggregation_state_ok <- function(state) {
  has_cols <- function(x, cols) is.data.frame(x) && all(cols %in% names(x))

  !is.null(state) &&
    has_cols(state$manifest, c("name", "updated")) &&
    has_cols(
      state$registry,
      c(
        "trip",
        "file",
        "imei",
        "first_timestamp",
        "last_timestamp",
        "fingerprint",
        "duplicate_of"
      )
    ) &&
    has_cols(
      state$effort,
      c(
        "trip",
        "h3_index",
        "year",
        "gear",
        "country",
        "date",
        "cell_hours",
        "cell_pings"
      )
    ) &&
    is.list(state$settings)
}


#' Upload the Incremental Aggregation State
#'
#' @description
#' Writes the four side files that let [aggregate_pds_effort()] rebuild the grid
#' without re-reading every predicted track: the manifest of aggregated objects
#' (with the modification time each was aggregated at, so replaced files can be
#' detected), the registry of trips seen (with the fingerprint used to spot
#' duplicates arriving in later runs), the per-trip cell-day effort the grid is
#' derived from, and the settings those hours were measured under.
#'
#' @param state The aggregation state to write.
#' @param names Object names, as returned by [aggregation_state_names()].
#' @param provider Cloud storage provider key.
#' @param options Named list of cloud storage options.
#'
#' @return Invisibly `NULL`.
#'
#' @seealso [load_aggregation_state()]
#'
#' @keywords internal
upload_aggregation_state <- function(state, names, provider, options) {
  local <- lapply(names, \(n) file.path(tempdir(), basename(n)))

  saveRDS(state$manifest, local$manifest)
  saveRDS(state$registry, local$registry)
  saveRDS(state$settings, local$settings)
  arrow::write_parquet(
    state$effort,
    sink = local$effort,
    compression = "lz4",
    compression_level = 12
  )

  # The manifest goes last, on purpose. It is the record of what has already
  # been read, so a run interrupted mid-upload must not leave it claiming files
  # whose effort never reached the store: those files would never be read
  # again. Written in this order, an interruption leaves the previous manifest
  # in place and the next run reads the batch a second time -- which changes
  # nothing, because [aggregate_pds_effort()] replaces a trip's rows rather
  # than adding to them. One call, not four: `upload_cloud_file()` uploads a
  # vector in order and re-authenticates once, where four calls would pay for
  # four fresh service-account tokens.
  order <- c("effort", "registry", "settings", "manifest")
  upload_cloud_file(
    file = unlist(local[order], use.names = FALSE),
    name = unlist(names[order], use.names = FALSE),
    provider = provider,
    options = options
  )

  unlink(unlist(local, use.names = FALSE))
  invisible(NULL)
}


#' Aggregate Predicted Fishing Tracks into an H3 Effort Grid
#'
#' @description
#' Downloads all per-trip predicted fishing track files produced by
#' [predict_pds_tracks()] and aggregates them into an H3 hexagonal grid of
#' cumulative fishing effort. The result is uploaded as a versioned parquet
#' file to the country-level cloud storage bucket.
#'
#' @details
#' Predicted track files contain fishing-only GPS points (columns: `trip`,
#' `timestamp`, `latitude`, `longitude`). This function:
#'
#' 1. Lists all files under `conf$pds$pds_tracks_predicted$file_prefix` in the
#'    PDS bucket.
#' 2. Downloads only **new** files (incremental via manifest) in parallel using
#'    `furrr`.
#' 3. Prepares each track with [prepare_tracks_for_effort()], which computes
#'    per-ping time intervals (`dt_hours`), assigns H3 cell indices, and
#'    records per-trip total hours for fidelity computation.
#' 4. Removes tracks PDS has already delivered under another trip identifier,
#'    whole trips first ([index_trip_duplicates()]) and then the pings two
#'    partially overlapping trips share ([drop_overlapping_pings()]).
#' 5. Adds what survives to the per-trip effort store ([build_trip_effort()]).
#' 6. Derives the whole grid from that store ([derive_effort_grid()]) and
#'    uploads it.
#'
#' ## Incremental state
#'
#' Four side files sit next to the grid. `aggregated_manifest.rds` lists the
#' objects already aggregated with the modification time they were aggregated
#' at; `aggregated_trips.rds` lists every trip seen, with its device, its
#' window, the fingerprint of its track and the trip it duplicates if it was
#' dropped; `aggregated_trip_effort.parquet` holds fishing hours and pings per
#' trip, cell, year, gear, country and day; `aggregated_settings.rds` records
#' the gap handling those hours were measured under.
#'
#' The store — not the published grid — is what carries between runs, and the
#' grid is rebuilt from it in full every time. That is what makes a trip
#' *removable*: PDS revises trips continuously (identifiers are retired, and
#' the same identifier can come to describe a different window), so
#' [predict_pds_tracks()] deletes and replaces files, and effort that was
#' already counted has to be taken back out. A grid of running totals cannot do
#' that; a per-trip store can, by dropping the affected trips' rows — together
#' with any trip that was dropped as a duplicate of them, and any trip that
#' might share pings with the incoming batch ([recall_overlapping_trips()]) —
#' and re-reading only the handful of files involved. Deriving from the whole
#' store each run also makes the result independent of how trips were batched,
#' so `unique_trips` is a distinct count rather than a sum of per-batch counts.
#'
#' A trip's rows are replaced rather than added to, so reading a file twice
#' changes nothing. Everything is re-read from the predicted tracks only when
#' the state is missing or written by an older schema, or when `max_gap_hours` /
#' `gap_policy` differ from the values the stored hours were measured under —
#' settings that leave no trace in the files themselves. A new model version
#' needs no trigger of its own: [predict_pds_tracks()] deletes every
#' old-version file and writes new ones, which the per-file comparison already
#' sees, and it sees precisely which files moved rather than assuming all of
#' them did.
#'
#' ## Batching large backlogs
#'
#' Peak memory is set by how many points are read at once, not by how many the
#' store holds, and the two are normally unrelated: a daily run reads the delta
#' since the last one --- of the order of 300 objects and 100,000 points. A
#' region registered for the first time breaks that assumption, because its
#' whole history is new at once. Read in one go, 25 million points need roughly
#' 25 GB and the run is killed by the operating system rather than failing in R.
#'
#' So the backlog is read in batches of `batch_size` objects, and the loop is
#' inside this function: there is one way to call it, and it does the whole
#' backlog whatever size that is. A normal delta is smaller than a single batch
#' and behaves exactly as it did before batching existed.
#'
#' Each batch checkpoints the aggregation state before the next begins, so an
#' interrupted backlog resumes from the last completed batch instead of
#' starting over --- which matters when the reading alone can run for hours. The
#' grid is derived once at the end rather than per batch, since it is rebuilt
#' from the whole store every time regardless.
#'
#' Batching does not change the result. Trips that might share pings are
#' recalled whatever batch they fall in ([recall_overlapping_trips()]), and the
#' grid is derived from the entire store, so how the trips were divided leaves
#' no trace. A store built over eleven batches and one built in a single pass
#' agree on cell count, `unique_trips` and per-cell effort to floating-point
#' summation order.
#'
#' As a guide to sizing, predicted-track objects hold a few hundred points each,
#' so the default 8,000 is roughly two million points, or about 2.7 GB. Lower it
#' if the machine is smaller than that; there is no reason to raise it.
#'
#' ## Grid schema
#'
#' The grid is partitioned by `year`, `gear`, and `country`, so there may be
#' several rows per `(h3_index, year)` — one per `(gear, country)` combination.
#' `gear` and `country` come from the per-trip device metadata
#' (trip → IMEI → Airtable `pds_devices`); trips with no device metadata are
#' kept under `gear = "unknown"` / `country = "unknown"` so totals are
#' preserved. Because each trip maps to exactly one gear and one country,
#' summing over `gear` and `country` (and `year`) recovers the all-time,
#' all-fleet totals — see [plot_effort_map()] for temporal maps. Primary
#' effort columns:
#'
#' - `fishing_hours`: accumulated fishing time (sum of capped inter-ping
#'   intervals). This is the primary effort metric.
#' - `unique_trips`: count of distinct trips contributing to the cell.
#' - `n_active_days`: count of distinct calendar days with fishing activity.
#' - `first_active_date` / `last_active_date`: date range for inferring the
#'   study period length (`n_total_days`) downstream.
#' - `avg_fidelity_sum`: sum of per-trip fidelity values (fraction of each
#'   trip's total fishing hours spent in this cell). Divide by
#'   `n_trips_for_fidelity` to get `avg_fidelity` ∈ [0, 1].
#' - `n_trips_for_fidelity`: number of trips contributing to `avg_fidelity_sum`.
#' - `fishing_pings`: raw GPS point count (retained for QA; not used as a
#'   primary metric because ping frequency is irregular).
#'
#' **Multi-resolution support:** passing different `h3_res` values writes to
#' separate cloud prefixes (e.g. `predicted-pds-h3_grid_r9`,
#' `predicted-pds-h3_grid_r7`), so grids at multiple resolutions can coexist.
#' Use [rollup_h3_resolution()] to derive coarser views from a stored fine grid,
#' or pass a coarser `h3_res` directly to recompute from raw tracks.
#' [derive_fishing_grounds()] can further roll up to any resolution before
#' extracting contiguous fishing ground polygons.
#'
#' @param log_threshold The logging threshold to use. Default is `logger::DEBUG`.
#' @param h3_res Integer (0–15). H3 resolution level for the output grid.
#'   Default is `9` (~174 m edge length). Different resolutions write to
#'   separate cloud prefixes.
#' @param max_gap_hours,gap_policy How silent stretches of a track are counted,
#'   passed to [prepare_tracks_for_effort()]. Both are recorded with the
#'   aggregation state: changing either makes every stored figure incomparable,
#'   so the grid is rebuilt from the predicted tracks.
#' @param batch_size Integer. Most predicted-track objects to hold in memory at
#'   once. The function reads the whole backlog either way; this only sets how
#'   much of it is in flight at a time. The default of `8000` is far above a
#'   normal delta, so ordinary runs are a single batch and are unaffected.
#'   Lower it on a smaller machine. See the batching section below.
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Pass your own package name when calling from a downstream
#'   package with a compatible configuration.
#'
#' @return Invisibly returns the merged H3 grid data frame (columns:
#'   `h3_index`, `year`, `gear`, `country`, `fishing_hours`, `unique_trips`,
#'   `n_active_days`, `first_active_date`, `last_active_date`,
#'   `avg_fidelity_sum`, `n_trips_for_fidelity`, `fishing_pings`), or `NULL` if
#'   there was nothing to process.
#'
#' @seealso [predict_pds_tracks()], [derive_fishing_grounds()],
#'   [rollup_h3_resolution()], [plot_effort_map()]
#'
#' @keywords workflow modeling
#' @export
aggregate_pds_effort <- function(
  log_threshold = logger::DEBUG,
  h3_res = 9L,
  max_gap_hours = 0.25,
  gap_policy = c("cap", "drop"),
  batch_size = 8000L,
  package = "coasts"
) {
  gap_policy <- match.arg(gap_policy)
  logger::log_threshold(log_threshold)
  conf <- read_config(package = package)

  effort_settings <- normalise_effort_settings(list(
    max_gap_hours = max_gap_hours,
    gap_policy = gap_policy
  ))

  pds_opts <- resolve_storage_opts(conf, "pds")
  country_opts <- resolve_storage_opts(conf, "country")

  file_prefix <- conf$pds$pds_tracks_predicted$file_prefix
  # Resolution-specific prefix so grids at different h3_res don't share manifests
  grid_prefix <- paste0(conf$pds$pds_tracks_h3_grid$file_prefix, "_r", h3_res)
  state_names <- aggregation_state_names(grid_prefix)

  logger::log_info("Model version: {ssfaitk::ssfaitk_version()[[1]]}")

  # --- Build the per-trip lookup ---
  # gear and country are trip-level attributes (trip -> imei -> device metadata)
  # used to partition the effort grid. Each trip maps to exactly one gear and one
  # country, so this is a loss-free refinement of the grid. The device and the
  # trip's window are kept as well, so that trips liable to share pings with an
  # incoming batch can be found without reading any tracks -- see
  # [recall_overlapping_trips()].
  logger::log_info("Building trip -> gear/country lookup...")

  pds_trips <- download_parquet_from_cloud(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    version = conf$pds$pds_trips$version,
    options = country_opts
  ) |>
    janitor::clean_names() |>
    dplyr::transmute(
      trip = as.character(.data$trip),
      imei = as.character(.data$imei),
      started = lubridate::as_datetime(.data$started),
      ended = lubridate::as_datetime(.data$ended)
    )

  devices <- conf$metadata$airtable$name |>
    cloud_object_name(
      provider = conf$storage$google$key,
      version = "latest",
      extension = "rds",
      options = conf$storage$google$options
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    readr::read_rds() |>
    purrr::pluck("devices") |>
    dplyr::transmute(
      imei = as.character(.data$imei),
      gear = .data$gear_class,
      country = .data$country_unlink
    )

  # One row per trip; distinct() guards against an imei mapping to multiple
  # device rows, which would otherwise fan out (and double-count) track rows.
  trip_lookup <- pds_trips |>
    dplyr::left_join(devices, by = "imei") |>
    dplyr::select("trip", "gear", "country", "imei", "started", "ended") |>
    dplyr::distinct(.data$trip, .keep_all = TRUE)

  logger::log_info("Listing predicted track files...")
  cloud_storage_authenticate(conf$pds_storage$google$key, pds_opts)

  predicted_files <- googleCloudStorageR::gcs_list_objects(
    bucket = pds_opts$bucket,
    prefix = file_prefix
  )

  # An empty listing comes back without columns at all, so the guard has to
  # come before anything that names one.
  if (
    is.null(predicted_files) ||
      !all(c("name", "updated") %in% names(predicted_files)) ||
      nrow(predicted_files) == 0
  ) {
    logger::log_info("No predicted tracks found in bucket")
    return(invisible(NULL))
  }

  predicted_files <- dplyr::select(predicted_files, "name", "updated")

  # --- Load the incremental state ---
  # The grid itself is not read back: it is derived from the per-trip effort
  # store, which is the only thing that has to survive between runs.
  state <- load_aggregation_state(
    state_names,
    provider = conf$storage$google$key,
    options = country_opts
  )

  if (!aggregation_state_ok(state)) {
    logger::log_info("Aggregation state is absent or outdated, starting over")
    state <- empty_aggregation_state(effort_settings)
  } else if (
    !identical(normalise_effort_settings(state$settings), effort_settings)
  ) {
    # Stored hours were measured under the old setting and cannot be compared
    # with, or added to, hours measured under the new one.
    logger::log_info(
      "Gap handling changed to {gap_policy} at {max_gap_hours * 60} minutes",
      " (was {state$settings$gap_policy} at",
      " {as.numeric(state$settings$max_gap_hours) * 60}), rebuilding from scratch"
    )
    state <- empty_aggregation_state(effort_settings)
  } else {
    logger::log_info(
      "Loaded per-trip effort store: {nrow(state$effort)} rows,",
      " {dplyr::n_distinct(state$effort$trip)} trips"
    )
  }

  # --- Detect changed inputs → drop just what they contributed ---
  # `predict_pds_tracks()` deletes the files of trips PDS has retired and
  # overwrites the files of trips it re-predicted. Every affected trip is
  # withdrawn from the store, along with the trips that were dropped as
  # duplicates of it, so they are all re-read and re-judged together.
  changed_files <- union(
    setdiff(state$manifest$name, predicted_files$name),
    state$manifest |>
      dplyr::inner_join(
        predicted_files,
        by = "name",
        suffix = c("_aggregated", "_current")
      ) |>
      dplyr::filter(.data$updated_current > .data$updated_aggregated) |>
      dplyr::pull(.data$name)
  )

  if (length(changed_files) > 0) {
    withdrawn <- withdraw_trips(
      state,
      seed = state$registry$trip[state$registry$file %in% changed_files],
      extra_files = changed_files
    )
    state <- withdrawn$state

    logger::log_info(
      "{length(changed_files)} aggregated file(s) deleted or replaced upstream:",
      " withdrawing {length(withdrawn$trips)} trip(s) from the effort store"
    )
  }

  # --- Work through the new files in bounded batches ---
  # Peak memory is set by how many points are held at once, not by the size of
  # the store, so the backlog is consumed a batch at a time. A normal delta is
  # smaller than one batch and runs exactly as it always has; only a backlog
  # large enough to exhaust memory is split, and it is split here rather than
  # by the caller, so there is one way to call this function whatever it finds.
  pending <- setdiff(predicted_files$name, state$manifest$name)

  if (length(pending) == 0 && length(changed_files) == 0) {
    logger::log_info("No new tracks to aggregate, grid is up to date")
    return(invisible(NULL))
  }

  n_batches <- ceiling(length(pending) / batch_size)

  if (n_batches > 1L) {
    logger::log_info(
      "{length(pending)} new files to aggregate in {n_batches} batches of up to",
      " {batch_size} (skipping {nrow(state$manifest)} already done)"
    )
  } else {
    logger::log_info(
      "{length(pending)} new files to aggregate (skipping {nrow(state$manifest)} already done)"
    )
  }

  batch_no <- 0L

  while (length(pending) > 0) {
    batch_no <- batch_no + 1L
    new_files <- select_effort_batch(pending, trip_lookup, batch_size)

    # The batch leaves the queue whether or not it could be read. A file that
    # fails stays out of the *manifest*, so a later run retries it, but it must
    # not be offered to this loop again: selection is deterministic, so a batch
    # that failed in full would otherwise be chosen over and over for ever.
    pending <- setdiff(pending, new_files)

    if (n_batches > 1L) {
      logger::log_info(
        "--- batch {batch_no}/{n_batches}: {length(new_files)} file(s),",
        " {length(pending)} still queued after it"
      )
    }

    # --- Recall aggregated trips that could share pings with this batch ---
    # Pings are only compared within a batch, so a track PDS has split across
    # two trips would otherwise be counted twice: one half read now, the other
    # already in the store and never set beside it.
    recalled <- recall_overlapping_trips(new_files, trip_lookup, state$registry)

    if (nrow(recalled) > 0) {
      withdrawn <- withdraw_trips(state, seed = recalled$trip)
      state <- withdrawn$state
      new_files <- union(new_files, withdrawn$files)

      logger::log_info(
        "Recalling {length(withdrawn$trips)} aggregated trip(s) that may share",
        " pings with this batch, to judge them together"
      )
    }

    # --- Download only new files ---
    new_tracks <- tibble::tibble()
    failed_files <- character(0)

    if (length(new_files) > 0) {
      fetched <- download_predicted_files(
        new_files,
        provider = conf$pds_storage$google$key,
        options = pds_opts
      )
      new_tracks <- fetched$tracks
      failed_files <- fetched$failed
    }

    # Attach gear/country; trips with no device metadata are kept as "unknown"
    # so that aggregate totals are preserved when collapsing the grid back over
    # them.
    if (nrow(new_tracks) > 0) {
      new_tracks <- new_tracks |>
        dplyr::mutate(trip = as.character(.data$trip)) |>
        dplyr::left_join(trip_lookup, by = "trip") |>
        dplyr::mutate(
          gear = dplyr::coalesce(.data$gear, "unknown"),
          country = dplyr::coalesce(.data$country, "unknown")
        )

      n_trips <- dplyr::n_distinct(new_tracks$trip)
      logger::log_info(
        "Aggregating {nrow(new_tracks)} new fishing points from {n_trips} trips to H3 res {h3_res}"
      )

      # --- Prepare tracks: compute dt_hours, year and h3_index ---
      prepared <- prepare_tracks_for_effort(
        new_tracks,
        h3_res,
        max_gap_hours = max_gap_hours,
        gap_policy = gap_policy
      )

      # Where each trip came from and when it fished, so a later batch can find
      # the trips it might share pings with.
      trip_meta <- prepared |>
        dplyr::summarise(
          .by = "trip",
          file = dplyr::first(.data$source_file),
          imei = dplyr::first(.data$imei),
          first_timestamp = min(.data$timestamp),
          last_timestamp = max(.data$timestamp)
        )

      # --- Drop tracks PDS has already delivered under another trip id ---
      # Trips PDS still lists are preferred: they are the ones carrying device
      # metadata, so the survivor keeps its gear/country instead of falling into
      # the "unknown" bucket. Only trips that survived earlier runs are offered
      # as prior art — a trip already dropped as a duplicate must never become
      # the survivor of a later one.
      trip_index <- index_trip_duplicates(
        prepared,
        prefer = trip_lookup$trip,
        seen = dplyr::filter(state$registry, is.na(.data$duplicate_of))
      )
      n_duplicated <- sum(!trip_index$keep)

      if (n_duplicated > 0) {
        logger::log_info(
          "Dropping {n_duplicated} of {nrow(trip_index)} trips whose track duplicates another trip"
        )
        prepared <- dplyr::filter(
          prepared,
          .data$trip %in% trip_index$trip[trip_index$keep]
        )
      }

      n_pings <- nrow(prepared)
      prepared <- drop_overlapping_pings(prepared, prefer = trip_lookup$trip)

      if (nrow(prepared) < n_pings) {
        logger::log_info(
          "Dropping {n_pings - nrow(prepared)} pings shared by more than one trip"
        )
      }

      # A trip's rows are replaced rather than added to, so reading the same
      # file twice — after an interrupted upload, say — changes nothing.
      state$registry <- dplyr::bind_rows(
        dplyr::filter(state$registry, !(.data$trip %in% trip_index$trip)),
        trip_index |>
          dplyr::left_join(trip_meta, by = "trip") |>
          dplyr::select(
            "trip",
            "file",
            "imei",
            "first_timestamp",
            "last_timestamp",
            "fingerprint",
            "n_points",
            "duplicate_of"
          )
      )

      state$effort <- dplyr::bind_rows(
        dplyr::filter(state$effort, !(.data$trip %in% trip_index$trip)),
        build_trip_effort(prepared)
      )
    }

    # Every file read in this batch counts as aggregated, including the empty
    # ones and the duplicates dropped above, so they are not fetched again.
    # Files that could not be read are left out: recording them would retire
    # their effort for good over a transient storage error.
    read_files <- dplyr::filter(
      predicted_files,
      .data$name %in% setdiff(new_files, failed_files)
    )
    state$manifest <- dplyr::bind_rows(
      dplyr::filter(state$manifest, !(.data$name %in% read_files$name)),
      read_files
    )

    # Checkpoint between batches so that an interrupted backlog resumes where
    # it stopped. The grid is left until the end: it is derived from the whole
    # store, so writing it once costs one derivation instead of one per batch,
    # and a run that dies half way leaves a stale grid beside a good store —
    # which the next run rebuilds from that store in full.
    if (length(pending) > 0) {
      logger::log_info(
        "Checkpointing state after batch {batch_no}/{n_batches}..."
      )
      upload_aggregation_state(
        state,
        state_names,
        provider = conf$storage$google$key,
        options = country_opts
      )

      # Nothing below needs the batch, and the next one is about to allocate
      # its own; holding these until then would double the peak for no reason.
      rm(new_tracks)
      suppressWarnings(rm(prepared, trip_index, trip_meta))
      invisible(gc(FALSE))
    }
  }

  if (nrow(state$effort) == 0) {
    logger::log_info("No effort left to aggregate, grid not written")
    upload_aggregation_state(
      state,
      state_names,
      provider = conf$storage$google$key,
      options = country_opts
    )
    return(invisible(NULL))
  }

  # --- Derive the grid from the whole store ---
  # Every column is recomputed from scratch, so the result never depends on how
  # the trips were batched and withdrawn trips leave no trace behind.
  h3_grid <- derive_effort_grid(state$effort)

  # --- Upload updated grid ---
  output_filename <- grid_prefix |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    h3_grid,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  n_cells <- dplyr::n_distinct(h3_grid$h3_index)
  logger::log_info(
    "Uploading H3 grid ({n_cells} cells, {nrow(h3_grid)} rows) to cloud storage..."
  )
  upload_cloud_file(
    file = output_filename,
    provider = conf$storage$google$key,
    options = country_opts
  )
  unlink(output_filename)

  # --- Upload updated manifest, trip registry and effort store ---
  upload_aggregation_state(
    state,
    state_names,
    provider = conf$storage$google$key,
    options = country_opts
  )

  logger::log_success(
    "H3 grid updated: {n_cells} cells ({nrow(state$manifest)} files aggregated",
    " over {batch_no} batch(es))"
  )

  invisible(h3_grid)
}


#' Project Fishing GPS Points to a Metric CRS
#'
#' @description
#' Converts a data frame of GPS fishing observations to a projected `sf` POINT
#' object. Rows with missing coordinates are dropped. The result is in a metric
#' CRS suitable for distance-based operations such as grid creation and spatial
#' joins.
#'
#' @param df A data frame containing GPS fishing point records.
#' @param lat_col Character. Name of the latitude column. Default is `"lat"`.
#' @param lon_col Character. Name of the longitude column. Default is `"lon"`.
#' @param crs_projected Integer. EPSG code of the target projected CRS.
#'   Default is `32632` (UTM zone 32N). Choose a zone that covers your study
#'   area for accurate metric distances.
#'
#' @return An `sf` POINT object in the requested projected CRS.
#'
#' @seealso [create_reference_grid()], [aggregate_daily_effort()]
#'
#' @keywords preprocessing
#' @export
prep_fishing_points <- function(
  df,
  lat_col = "lat",
  lon_col = "lon",
  crs_projected = 32632
) {
  if (!all(c(lat_col, lon_col) %in% names(df))) {
    stop(
      "Latitude or longitude columns not found: ",
      paste(c(lat_col, lon_col), collapse = ", ")
    )
  }

  df |>
    dplyr::filter(!is.na(.data[[lat_col]]) & !is.na(.data[[lon_col]])) |>
    sf::st_as_sf(coords = c(lon_col, lat_col), crs = 4326) |>
    sf::st_transform(crs = crs_projected)
}


#' Create a Deterministic Reference Grid
#'
#' @description
#' Generates a regular square or hexagonal spatial grid over a study area
#' bounding box and assigns each cell a stable unique identifier. Create the
#' grid once and reuse it across pipeline runs so that `cell_id` values remain
#' consistent over time.
#'
#' @param study_area_bbox An `sf` polygon or bounding box defining the spatial
#'   extent of the grid.
#' @param cell_size_meters Numeric. Grid cell size in metres. Default is `500`.
#' @param hex Logical. If `TRUE` (default), creates hexagonal cells; if
#'   `FALSE`, creates square cells.
#'
#' @return An `sf` polygon object with a `cell_id` column containing a unique
#'   identifier for each cell (format: `"GRID_<n>"`).
#'
#' @seealso [prep_fishing_points()], [aggregate_daily_effort()]
#'
#' @keywords preprocessing
#' @export
create_reference_grid <- function(
  study_area_bbox,
  cell_size_meters = 500,
  hex = TRUE
) {
  grid_sfc <- sf::st_make_grid(
    study_area_bbox,
    cellsize = cell_size_meters,
    square = !hex
  )

  sf::st_sf(
    cell_id = paste0("GRID_", seq_along(grid_sfc)),
    geometry = grid_sfc
  )
}


#' Aggregate GPS Points to a Reference Grid
#'
#' @description
#' Spatially joins projected GPS fishing points to a reference grid and counts
#' the number of fishing pings per cell. Points that fall outside the grid
#' extent are silently dropped.
#'
#' @param points_sf Projected `sf` POINT object, as returned by
#'   [prep_fishing_points()].
#' @param reference_grid_sf `sf` polygon grid, as returned by
#'   [create_reference_grid()].
#'
#' @return A data frame with columns `cell_id` and `fishing_pings`.
#'
#' @seealso [prep_fishing_points()], [create_reference_grid()]
#'
#' @keywords preprocessing
#' @export
aggregate_daily_effort <- function(points_sf, reference_grid_sf) {
  sf::st_join(points_sf, reference_grid_sf, join = sf::st_intersects) |>
    sf::st_drop_geometry() |>
    dplyr::filter(!is.na(.data$cell_id)) |>
    dplyr::group_by(.data$cell_id) |>
    dplyr::summarise(fishing_pings = dplyr::n(), .groups = "drop")
}


#' Assign H3 Hexagon Indices to GPS Points
#'
#' @description
#' Adds an `h3_index` column to a GPS data frame by mapping each coordinate to
#' its containing H3 hexagon at the specified resolution. Rows whose coordinates
#' are not usable positions are dropped --- missing, non-finite, or outside the
#' latitude/longitude range, all of which [valid_coordinates()] describes. The
#' data frame is returned in its original unprojected (WGS84) form with the
#' index appended.
#'
#' @param df A data frame with GPS coordinates.
#' @param lat_col Character. Name of the latitude column. Default is `"lat"`.
#' @param lon_col Character. Name of the longitude column. Default is `"lon"`.
#' @param h3_res Integer (0–15). H3 resolution level. Default is `9`
#'   (~174 m edge length). Higher values produce smaller, finer cells.
#'
#' @return The input data frame (minus rows with missing coordinates) with an
#'   additional `h3_index` character column.
#'
#' @seealso [aggregate_h3_effort()], [rollup_h3_resolution()]
#'
#' @keywords preprocessing
#' @export
assign_h3_indices <- function(
  df,
  lat_col = "lat",
  lon_col = "lon",
  h3_res = 9
) {
  # Filtering on `is.na()` alone left two holes: a non-finite coordinate that is
  # not NA still aborts the call, and an out-of-range one is wrapped into a
  # plausible cell on the far side of the world without complaint. See
  # [valid_coordinates()].
  df_clean <- df[
    valid_coordinates(df[[lon_col]], df[[lat_col]]), ,
    drop = FALSE
  ]

  if (nrow(df_clean) == 0) {
    df_clean$h3_index <- character(0)
    return(df_clean)
  }

  points_sf <- sf::st_as_sf(df_clean, coords = c(lon_col, lat_col), crs = 4326)
  df_clean$h3_index <- h3jsr::point_to_cell(points_sf, res = h3_res)
  df_clean
}


#' Aggregate Fishing Effort by H3 Hexagon
#'
#' @description
#' Summarises GPS fishing pings by H3 hexagon index, computing total ping
#' count and number of unique trips per cell.
#'
#' @param df_with_h3 A data frame with an `h3_index` column and a `Trip`
#'   column, as produced by [assign_h3_indices()].
#'
#' @return A data frame with columns `h3_index`, `fishing_pings`, and
#'   `unique_vessels`.
#'
#' @seealso [assign_h3_indices()], [rollup_h3_resolution()]
#'
#' @keywords preprocessing
#' @export
aggregate_h3_effort <- function(df_with_h3) {
  df_with_h3 |>
    dplyr::group_by(.data$h3_index) |>
    dplyr::summarise(
      fishing_pings = dplyr::n(),
      unique_vessels = dplyr::n_distinct(.data$Trip),
      .groups = "drop"
    )
}


#' Roll Up H3 Fishing Effort to a Coarser Resolution
#'
#' @description
#' Re-aggregates fishing effort from a fine H3 resolution to a coarser parent
#' resolution by mapping each cell to its containing parent hexagon and summing
#' ping counts. Useful for multi-scale analysis without rerunning the full
#' spatial join.
#'
#' @param aggregated_df A data frame with columns `h3_index` and
#'   `fishing_pings`, as returned by [aggregate_h3_effort()].
#' @param target_res Integer. The target H3 resolution. Must be lower (coarser)
#'   than the resolution used to create `aggregated_df`.
#'
#' @return A data frame with columns `parent_h3_index` and
#'   `total_fishing_pings`.
#'
#' @seealso [assign_h3_indices()], [aggregate_h3_effort()]
#'
#' @keywords preprocessing
#' @export
rollup_h3_resolution <- function(aggregated_df, target_res) {
  aggregated_df |>
    dplyr::mutate(
      parent_h3_index = h3jsr::get_parent(.data$h3_index, res = target_res)
    ) |>
    dplyr::group_by(.data$parent_h3_index) |>
    dplyr::summarise(
      total_fishing_pings = sum(.data$fishing_pings),
      .groups = "drop"
    )
}

#' Convert an H3 Effort Summary to a Spatial Grid
#'
#' @description
#' Attaches hexagonal polygon geometries to an H3 effort summary table,
#' returning an `sf` object ready for mapping or further spatial analysis.
#' Polygons are derived from the `h3_index` column using WGS84 (EPSG 4326).
#'
#' @param h3_summary_df A data frame with an `h3_index` column, as returned by
#'   [aggregate_h3_effort()] or [rollup_h3_resolution()]. All other columns are
#'   preserved in the output.
#'
#' @return An `sf` polygon object in WGS84 (EPSG 4326) with one row per H3
#'   cell and a `geometry` column containing the hexagon boundaries.
#'
#' @seealso [aggregate_h3_effort()], [rollup_h3_resolution()]
#'
#' @keywords preprocessing
#' @export
create_spatial_grid <- function(h3_summary_df = NULL) {
  hex_geoms <- h3jsr::cell_to_polygon(h3_summary_df$h3_index, simple = TRUE)
  sf_grid <- sf::st_sf(h3_summary_df, geometry = hex_geoms, crs = 4326)
  return(sf_grid)
}
