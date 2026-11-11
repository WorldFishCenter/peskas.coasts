test_that("mdb_users_upsert keeps the tracks app's own users and passwords", {
  # Needs a throwaway MongoDB, e.g. COASTS_TEST_MONGO=mongodb://127.0.0.1:27099
  url <- Sys.getenv("COASTS_TEST_MONGO")
  skip_if(!nzchar(url), "COASTS_TEST_MONGO not set")

  users <- mongolite::mongo("users", "coasts-test", url)
  users$drop()
  users$insert(data.frame(username = "admin1", password = "a", role = "admin"))
  users$insert(data.frame(username = "self1", password = "s"))
  users$insert(data.frame(IMEI = "1", Boat = "old", captain = "x", password = "changed"))
  users$insert(data.frame(IMEI = "9", Boat = "retired", password = "r"))
  id_before <- users$find('{"IMEI": "1"}', fields = '{"_id": 1}')$`_id`

  devices <- data.frame(
    IMEI = c("1", "2"),
    Boat = c("new", "boat2"),
    captain = c(NA, "c2"),
    password = c("from-airtable", "p2")
  )
  result <- mdb_users_upsert(devices, url, "users", "coasts-test")
  after <- users$find('{}', fields = '{}')

  expect_equal(unlist(result), c(matched = 1, changed = 1, added = 1, removed = 1))
  expect_setequal(after$username[!is.na(after$username)], c("admin1", "self1"))
  expect_setequal(after$IMEI[!is.na(after$IMEI)], c("1", "2"))
  one <- after[after$IMEI %in% "1", ]
  expect_equal(one$password, "changed")
  expect_equal(one$Boat, "new")
  expect_true(is.na(one$captain))
  expect_equal(one$`_id`, id_before)
  expect_equal(after$password[after$IMEI %in% "2"], "p2")

  users$drop()
})
