#' Add timestamp and sha string to a file name
#'
#' An alternative to version data is to name it using the sha (unique
#' identifier) of the code using to generate or process the data and the time at
#' which the data was generated or processed. This function adds this
#' information, a version identifier, to a file name (character string)
#'
#' @param filename Path sans extension of the file to version
#' @param extension Extension of the file
#' @param sha_nchar Number of characters from the SHA to use as the version
#'   identifier
#' @param sep Characters separating the version identifier from the file name
#'
#' @return A character string with the file name and the version identifier
#' @export
#'
#' @details
#'
#' The SHA information is retrieved using [git2r::sha]. If the code is not
#' running in a context aware of a git repository (for example when code is
#' running inside a container) then this function attempts to get the sha from
#' the environment variable `GITHUB_SHA`. If both of these methods fail, no sha
#' versioning is added.
#' @keywords helper
#' @examples
#' if (git2r::in_repository()) {
#'   add_version("my_file", "csv")
#' }
add_version <- function(filename, extension = "", sha_nchar = 7, sep = "__") {
  # Git sha are 40 characters long
  stopifnot(sha_nchar <= 40)

  version <- format(Sys.time(), "%Y%m%d%H%M%S")

  if (git2r::in_repository()) {
    commit_sha <- substr(git2r::sha(git2r::last_commit()), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  } else if (Sys.getenv("GITHUB_SHA") != "") {
    # If not in a git repository (for example when code is running inside a
    # container) get the sha from an environment variable if available
    commit_sha <- substr(Sys.getenv("GITHUB_SHA"), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  }

  # If the extension comes without dot, add one
  if (nchar(extension) > 0 & substr(extension, 1, 1) != ".") {
    extension <- paste0(".", extension)
  }

  paste0(filename, sep, version, sep, extension)
}


#' Read configuration file
#'
#' Reads configuration file in `conf.yml` and adds some logging lines. Wrapped
#' for convenience
#'
#' @param package Name of the package whose `inst/conf.yml` to read. Defaults
#'   to `"coasts"`. Downstream packages that ship their own `conf.yml` should
#'   pass their own package name here so that their configuration is loaded
#'   instead of the `coasts` defaults.
#'
#' @return the environment parameters
#' @keywords helper
#' @export
#'
read_config <- function(package = "coasts") {
  logger::log_info("Loading configuration file...")

  # Load .env file if it exists (for local development)
  if (file.exists(".env")) {
    logger::log_info("Loading environment variables from .env file")
    dotenv::load_dot_env(".env")
  }

  # Accept both conf.yml (coasts convention) and config.yml (config package default)
  conf_file <- system.file("conf.yml", package = package)
  if (!nzchar(conf_file)) {
    conf_file <- system.file("config.yml", package = package)
  }

  if (!nzchar(conf_file)) {
    stop(
      "No 'inst/conf.yml' or 'inst/config.yml' found in package '",
      package,
      "'. ",
      "Downstream packages must ship their own configuration file to use coasts pipeline functions. ",
      "See the coasts CLAUDE.md for the required configuration structure."
    )
  }

  conf <- config::get(
    config = Sys.getenv("R_CONFIG_ACTIVE", "default"),
    file = conf_file
  )

  logger::log_info("Using configutation: {attr(conf, 'config')}")

  # Never log the resolved config: it carries the GCP service-account key,
  # MongoDB connection strings, Kobo passwords and API tokens in plaintext.
  # Log only the non-sensitive storage targets actually in use. `unlist()` drops
  # buckets a downstream package's conf.yml does not define — glue blanks the
  # whole line if any interpolated key is NULL.
  buckets <- unlist(list(
    main = conf$storage$google$options$bucket,
    pds = conf$pds_storage$google$options$bucket,
    api = conf$api$trips$bucket
  ))
  logger::log_debug(
    "Buckets -> {paste(names(buckets), buckets, sep = ': ', collapse = ', ')}"
  )

  conf
}


#' Resolve Storage Options for PDS Workflows
#'
#' @description
#' Resolves the correct Google Cloud Storage options from a configuration object
#' based on the target bucket. Handles both standalone (`coasts`) and downstream
#' package contexts (e.g. `peskas.mozambique.data.pipeline`) transparently,
#' so workflow functions do not need to hardcode config paths.
#'
#' @param conf A configuration list as returned by [read_config()].
#' @param type Character. The target bucket. One of:
#'   - `"coasts"`: the shared coasts bucket where device metadata lives.
#'     Uses `conf$storage$google$options_coasts` if present (downstream packages),
#'     falling back to `conf$storage$google$options` (coasts itself, where
#'     `options` already points to the coasts bucket).
#'   - `"country"`: the country-specific pipeline bucket where processed outputs
#'     are written. Always uses `conf$storage$google$options`. In the coasts
#'     package this is the same bucket as `"coasts"`.
#'   - `"pds"`: the PDS-specific bucket where raw GPS tracks are stored.
#'     Always uses `conf$pds_storage$google$options`, which resolves to the
#'     correct bucket per package (`pds-peskas-coasts` in coasts,
#'     `pds-mozambique-prod` in the Mozambique pipeline, etc.).
#'   - `"public"`: the publicly readable bucket serving portal JSON. Uses
#'     `conf$public_storage$google$options`, falling back to
#'     `conf$storage$google$options_public`. Optional — see `error_if_missing`.
#'   - `"api"`: the cross-country API bucket holding the harmonized trips
#'     parquet every country publishes. Uses `conf$storage$google$options_api`.
#'     Optional — coasts itself does not configure one — see `error_if_missing`.
#'
#' @param error_if_missing Logical. Controls what happens when the requested
#'   type is not configured. `FALSE` (the default) returns `NULL`, which is how
#'   this function has always behaved for an unconfigured key and what makes
#'   `"public"` safe to ask for from a package that has no public bucket. Pass
#'   `TRUE` to fail fast with a message naming the expected configuration key.
#'
#' @return A named list of Google Cloud Storage options suitable for passing
#'   to [cloud_object_name()], [download_cloud_file()], or [upload_cloud_file()];
#'   or `NULL` when the type is unconfigured and `error_if_missing = FALSE`.
#'
#' @details
#' The bucket types map to the following storage contexts:
#'
#' | `type`     | coasts package        | downstream package         |
#' |------------|-----------------------|----------------------------|
#' | `"coasts"` | `peskas-coasts-dev`   | `peskas-coasts-dev`        |
#' | `"country"`| `peskas-coasts-dev`   | `mozambique-dev`           |
#' | `"pds"`    | `pds-peskas-coasts`   | `pds-mozambique-dev`       |
#' | `"public"` | *(unconfigured)*      | `timor-public-dev`         |
#' | `"api"`    | *(unconfigured)*      | `peskas-api-dev`           |
#'
#' `"public"` is deliberately optional. `coasts` itself has no public bucket, so
#' asking for it here returns `NULL`; a caller can then decide whether to skip
#' the publishing step or fail. The Timor-Leste
#' pipeline, which serves live portal JSON from a public bucket, previously had
#' to reach into its config directly because this helper could not express the
#' bucket at all.
#'
#' @examples
#' \dontrun{
#' conf <- read_config(package = "peskas.mozambique.data.pipeline")
#'
#' # Read device metadata from shared coasts bucket
#' coasts_opts <- resolve_storage_opts(conf, "coasts")
#'
#' # Write processed output to country bucket
#' country_opts <- resolve_storage_opts(conf, "country")
#'
#' # Access raw GPS tracks from PDS bucket
#' pds_opts <- resolve_storage_opts(conf, "pds")
#'
#' # Public portal bucket, tolerating its absence
#' public_opts <- resolve_storage_opts(conf, "public")
#' if (is.null(public_opts)) message("no public bucket configured")
#'
#' # Fail fast instead
#' public_opts <- resolve_storage_opts(conf, "public", error_if_missing = TRUE)
#' }
#'
#' @seealso [read_config()], [download_cloud_file()], [upload_cloud_file()]
#'
#' @export
#'
#' @keywords internal
resolve_storage_opts <- function(
  conf,
  type = c("coasts", "country", "pds", "public", "api"),
  error_if_missing = FALSE
) {
  type <- match.arg(type)

  opts <- switch(
    type,
    coasts = conf$storage$google$options_coasts %||%
      conf$storage$google$options,
    country = conf$storage$google$options,
    pds = conf$pds_storage$google$options,
    public = conf$public_storage$google$options %||%
      conf$storage$google$options_public,
    api = conf$storage$google$options_api
  )

  if (is.null(opts) && isTRUE(error_if_missing)) {
    stop(
      "No storage options configured for type '",
      type,
      "'. Expected ",
      switch(
        type,
        coasts = "'storage.google.options_coasts' or 'storage.google.options'",
        country = "'storage.google.options'",
        pds = "'pds_storage.google.options'",
        public = paste(
          "'public_storage.google.options' or",
          "'storage.google.options_public'"
        ),
        api = "'storage.google.options_api'"
      ),
      " in the active configuration. Pass error_if_missing = FALSE to get NULL ",
      "instead."
    )
  }

  opts
}
