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
  logger::log_debug("Running with parameters {conf}")

  conf
}


#' Resolve Storage Options for PDS Workflows
#'
#' @description
#' Resolves the correct Google Cloud Storage options from a configuration object
#' based on the intended operation type. Handles both standalone (`coasts`) and
#' downstream package contexts (e.g. `peskas.mozambique.data.pipeline`) transparently.
#'
#' @param conf A configuration list as returned by [read_config()].
#' @param type Character. The storage operation type. One of:
#'   - `"read"`: reading shared assets from the coasts bucket. Uses
#'     `conf$storage$google$options_coasts` if present (downstream packages),
#'     falling back to `conf$storage$google$options` (coasts itself).
#'   - `"write"`: writing pipeline outputs to the local package bucket.
#'     Always uses `conf$storage$google$options`.
#'   - `"tracks"`: reading/writing raw PDS track files. Always uses
#'     `conf$pds_storage$google$options`.
#'
#' @return A named list of Google Cloud Storage options suitable for passing
#'   to [cloud_object_name()], [download_cloud_file()], or [upload_cloud_file()].
#'
#' @details
#' The distinction between `"read"` and `"write"` exists because downstream
#' packages maintain two storage contexts: a shared coasts bucket (where device
#' metadata lives) and a local bucket (where pipeline outputs are written).
#' In the coasts package itself, both resolve to the same bucket.
#'
#' @examples
#' \dontrun{
#' conf <- read_config(package = "peskas.mozambique.data.pipeline")
#'
#' read_opts  <- resolve_storage_opts(conf, "read")   # peskas-coasts bucket
#' write_opts <- resolve_storage_opts(conf, "write")  # country bucket
#' track_opts <- resolve_storage_opts(conf, "tracks") # pds bucket
#' }
#'
#' @seealso [read_config()], [download_cloud_file()], [upload_cloud_file()]
#'
#' @keywords internal
resolve_storage_opts <- function(conf, type = c("read", "write", "tracks")) {
  type <- match.arg(type)
  switch(
    type,
    read = conf$storage$google$options_coasts %||% conf$storage$google$options,
    write = conf$storage$google$options,
    tracks = conf$pds_storage$google$options
  )
}
