# KoBoToolbox validation status
#
# The shared validation UI writes an approval back to KoBoToolbox, and a
# pipeline that overwrote it on every run would undo the enumerators' work.
# These functions existed only in `peskas.mozambique.data.pipeline`; Timor-Leste
# had to port them, and fixed three things while doing so — the bulk read, the
# 404 handling and basic auth. This is the corrected implementation.

#' Build an authenticated KoBoToolbox request
#'
#' Accepts either a token or a username and password. Timor-Leste authenticates
#' with basic auth because its `KOBO_TOKEN` belongs to a user with no data
#' access to its assets (200 on `/assets/<id>/`, 404 on `/assets/<id>/data/`),
#' while the username and password ingestion already uses work on both. A
#' country should not need a second credential for this.
#'
#' The request never throws on an HTTP error status. KoBoToolbox answers **404**
#' for a submission that has never been validated, which is the normal case, so
#' the status code has to reach the caller rather than being raised.
#'
#' @param url Character. The full request URL.
#' @param token Character. A KoBoToolbox API token, with or without the
#'   `Token ` prefix. Ignored when `username` is supplied.
#' @param username,password Character. Basic-auth credentials.
#'
#' @return An `httr2` request object.
#' @keywords internal
kobo_request <- function(url, token = NULL, username = NULL, password = NULL) {
  req <- httr2::request(url) |>
    httr2::req_error(is_error = function(resp) FALSE)

  if (!is.null(username) && nzchar(username)) {
    return(httr2::req_auth_basic(req, username, password))
  }
  if (is.null(token) || !nzchar(token)) {
    stop("KoBoToolbox needs either a token or a username and password")
  }
  if (!grepl("^Token ", token)) {
    token <- paste("Token", token)
  }
  httr2::req_headers(req, Authorization = token)
}

#' Build the validation-status URL for one submission
#'
#' @param asset_id Character. The KoBoToolbox asset id.
#' @param submission_id Character or integer. The submission to address.
#' @param url Character. The KoBoToolbox host.
#'
#' @return A character URL.
#' @keywords internal
kobo_validation_url <- function(
  asset_id,
  submission_id,
  url = "eu.kobotoolbox.org"
) {
  paste0(
    "https://",
    url,
    "/api/v2/assets/",
    asset_id,
    "/data/",
    submission_id,
    "/validation_status/"
  )
}

#' Read Every Submission's Validation Status From a KoBoToolbox Asset
#'
#' @description
#' The bulk form of [get_validation_status()], and the one a pipeline should
#' use. The data endpoint returns `_validation_status` alongside `_id` for up to
#' 1,000 submissions per request, so one asset costs `ceiling(n / 1000)`
#' requests rather than one request per submission.
#'
#' Measured against Timor-Leste's v2 form (64,997 submissions): **65 requests
#' and roughly 70 seconds**, against more than twenty minutes for a
#' per-submission loop over 7,776 previously-flagged submissions spread across
#' ten parallel workers. It also covers *every* submission rather than only
#' those a previous run flagged, so an approval entered by hand on a submission
#' the pipeline never flagged is seen too.
#'
#' @param asset_id Character. The KoBoToolbox asset id.
#' @param url Character. The KoBoToolbox host. Defaults to
#'   `"eu.kobotoolbox.org"`, matching [get_kobo_data()].
#' @inheritParams kobo_request
#' @param page_size Integer. Submissions per request. KoBoToolbox caps this at
#'   1,000 and silently truncates anything larger.
#'
#' @return A tibble with one row per submission: `submission_id`,
#'   `validation_status` (`not_validated` where none is set), `validated_at`,
#'   `validated_by`, `fetch_error`.
#'
#' @seealso [get_validation_status()], [update_validation_status()],
#'   [get_kobo_data()]
#'
#' @keywords validation
#' @export
#'
#' @examples
#' \dontrun{
#' conf <- read_config()
#' list_validation_statuses(
#'   asset_id = conf$ingestion$landings$v3$asset_id,
#'   username = conf$ingestion$landings$v3$username,
#'   password = conf$ingestion$landings$v3$password
#' )
#' }
list_validation_statuses <- function(
  asset_id = NULL,
  token = NULL,
  username = NULL,
  password = NULL,
  url = "eu.kobotoolbox.org",
  page_size = 1000
) {
  base <- paste0(
    "https://",
    url,
    "/api/v2/assets/",
    asset_id,
    "/data/?fields=",
    utils::URLencode('["_id","_validation_status"]', reserved = TRUE),
    "&limit=",
    page_size,
    "&start="
  )

  rows <- list()
  start <- 0
  repeat {
    response <- httr2::req_perform(kobo_request(
      paste0(base, start),
      token = token,
      username = username,
      password = password
    ))
    if (httr2::resp_status(response) != 200) {
      stop(
        "KoBoToolbox returned ",
        httr2::resp_status(response),
        " listing validation statuses for ",
        asset_id
      )
    }
    body <- httr2::resp_body_json(response)
    if (length(body$results) == 0) {
      break
    }
    rows <- c(rows, body$results)
    start <- start + length(body$results)
    logger::log_debug("Read {start} of {body$count} validation statuses")
    if (start >= body$count) {
      break
    }
  }

  status <- purrr::map(rows, "_validation_status")
  tibble::tibble(
    submission_id = as.integer(purrr::map_dbl(rows, "_id")),
    validation_status = purrr::map_chr(
      status,
      ~ .x$uid %||% "not_validated"
    ),
    validated_at = lubridate::as_datetime(purrr::map_dbl(
      status,
      ~ as.numeric(.x$timestamp %||% NA)
    )),
    validated_by = purrr::map_chr(status, ~ .x$by_whom %||% NA_character_),
    fetch_error = FALSE
  )
}

#' Read One Submission's Validation Status From KoBoToolbox
#'
#' @description
#' [list_validation_statuses()] is the bulk form and the one a pipeline should
#' call; this single-submission accessor exists because
#' [update_validation_status()] re-reads what it wrote.
#'
#' @param submission_id Character or integer. The submission to query.
#' @param asset_id Character. The KoBoToolbox asset id.
#' @param url Character. The KoBoToolbox host.
#' @inheritParams kobo_request
#' @param debug Logical. Print the request before performing it.
#'
#' @return A one-row tibble: `submission_id`, `validation_status`,
#'   `validated_at`, `validated_by`, `fetch_error`. A submission that has never
#'   been validated reports `not_validated`; `fetch_error` marks a genuine
#'   transport failure, **not** an absent status. KoBoToolbox answers 404 for
#'   the former, which is why [kobo_request()] does not let `httr2` throw on a
#'   4xx — otherwise every unvalidated submission is recorded as a fetch
#'   failure.
#'
#' @seealso [list_validation_statuses()], [update_validation_status()]
#'
#' @keywords validation
#' @export
#'
#' @examples
#' \dontrun{
#' conf <- read_config()
#' get_validation_status(
#'   submission_id = "452176760",
#'   asset_id = conf$ingestion$landings$v3$asset_id,
#'   username = conf$ingestion$landings$v3$username,
#'   password = conf$ingestion$landings$v3$password
#' )
#' }
get_validation_status <- function(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  username = NULL,
  password = NULL,
  url = "eu.kobotoolbox.org",
  debug = FALSE
) {
  req <- kobo_request(
    kobo_validation_url(asset_id, submission_id, url),
    token = token,
    username = username,
    password = password
  )
  if (debug) {
    print(req)
  }

  no_status <- function(fetch_error) {
    tibble::tibble(
      submission_id = submission_id,
      validation_status = if (fetch_error) NA_character_ else "not_validated",
      validated_at = lubridate::as_datetime(NA),
      validated_by = NA_character_,
      fetch_error = fetch_error
    )
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)
      if (httr2::resp_status(response) != 200) {
        return(no_status(FALSE))
      }
      status <- httr2::resp_body_json(response)
      tibble::tibble(
        submission_id = submission_id,
        validation_status = status$uid %||% "not_validated",
        validated_at = if (is.null(status$timestamp)) {
          lubridate::as_datetime(NA)
        } else {
          lubridate::as_datetime(status$timestamp)
        },
        validated_by = status$by_whom %||% NA_character_,
        fetch_error = FALSE
      )
    },
    error = function(e) {
      if (debug) {
        message("Error: ", conditionMessage(e))
      }
      no_status(TRUE)
    }
  )
}

#' Write One Submission's Validation Status Back to KoBoToolbox
#'
#' @description
#' **This mutates the live form.** There is no development KoBoToolbox
#' instance, so the assets a pipeline reads are the only targets in either
#' environment — `R_CONFIG_ACTIVE` does not isolate this call.
#'
#' @inheritParams get_validation_status
#' @param status Character. One of `validation_status_approved`,
#'   `validation_status_not_approved` or `validation_status_on_hold`.
#'
#' @return A one-row tibble as [get_validation_status()] returns, with
#'   `fetch_error` replaced by `update_success`.
#'
#' @seealso [get_validation_status()], [list_validation_statuses()]
#'
#' @keywords validation
#' @export
#'
#' @examples
#' \dontrun{
#' conf <- read_config()
#' update_validation_status(
#'   submission_id = "452176760",
#'   asset_id = conf$ingestion$landings$v3$asset_id,
#'   username = conf$ingestion$landings$v3$username,
#'   password = conf$ingestion$landings$v3$password,
#'   status = "validation_status_approved"
#' )
#' }
update_validation_status <- function(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  username = NULL,
  password = NULL,
  status = "validation_status_approved",
  url = "eu.kobotoolbox.org",
  debug = FALSE
) {
  valid_statuses <- c(
    "validation_status_approved",
    "validation_status_not_approved",
    "validation_status_on_hold"
  )
  if (!status %in% valid_statuses) {
    stop("Status must be one of: ", paste(valid_statuses, collapse = ", "))
  }

  req <- kobo_request(
    kobo_validation_url(asset_id, submission_id, url),
    token = token,
    username = username,
    password = password
  ) |>
    httr2::req_headers("Content-Type" = "application/json") |>
    httr2::req_method("PATCH") |>
    httr2::req_body_json(list("validation_status.uid" = status))

  if (debug) {
    print(req)
  }

  failed <- tibble::tibble(
    submission_id = submission_id,
    validation_status = NA_character_,
    validated_at = lubridate::as_datetime(NA),
    validated_by = NA_character_,
    update_success = FALSE
  )

  tryCatch(
    {
      response <- httr2::req_perform(req)
      if (!httr2::resp_status(response) %in% c(200, 201, 204)) {
        return(failed)
      }
      get_validation_status(
        submission_id = submission_id,
        asset_id = asset_id,
        token = token,
        username = username,
        password = password,
        url = url,
        debug = debug
      ) |>
        dplyr::select(-"fetch_error") |>
        dplyr::mutate(update_success = TRUE)
    },
    error = function(e) {
      if (debug) {
        message("Error: ", conditionMessage(e))
      }
      failed
    }
  )
}
