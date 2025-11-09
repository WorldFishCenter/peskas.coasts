# Helper functions for switching between configurations

use_prod <- function() {
  Sys.setenv(R_CONFIG_ACTIVE = "production")
  cat("\033[32m✓\033[0m Switched to \033[1mPRODUCTION\033[0m environment\n")
  invisible(NULL)
}

use_local <- function() {
  Sys.setenv(R_CONFIG_ACTIVE = "local")
  cat("\033[32m✓\033[0m Switched to \033[1mLOCAL\033[0m environment\n")
  invisible(NULL)
}

use_default <- function() {
  Sys.setenv(R_CONFIG_ACTIVE = "default")
  cat("\033[32m✓\033[0m Switched to \033[1mDEFAULT\033[0m environment\n")
  invisible(NULL)
}

show_env <- function() {
  current <- Sys.getenv("R_CONFIG_ACTIVE", "default")
  cat("Current environment: \033[1m", current, "\033[0m\n", sep = "")
  invisible(current)
}

# Show current environment on startup
if (interactive()) {
  current <- Sys.getenv("R_CONFIG_ACTIVE", "default")
  cat("\n\033[36mPeskas Kenya Data Pipeline\033[0m\n")
  cat("Current environment: \033[1m", current, "\033[0m\n", sep = "")
  cat("\nQuick commands:\n")
  cat("  • use_prod()    - Switch to production\n")
  cat("  • use_local()   - Switch to local\n")
  cat("  • use_default() - Switch to default\n")
  cat("  • show_env()    - Show current environment\n\n")
}
