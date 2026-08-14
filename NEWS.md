# coasts 4.8.0

## `exclude_dashboard_ids` was silently emptying the multi-country portal

The option exists to keep legacy forms off a *country's own* dashboard. It was applied once, to the frame every summary descends from, so it also stripped the two artifacts written to the coasts bucket — `<country>_fishery_metrics` and, via `export_portal()`, `<country>_monthly_summaries_map`. Those are what `export_geos()` binds across Kenya, Zanzibar and Mozambique for coasts.peskas.org, so a country hiding its legacy sources from its own dashboard was also deleting them from the regional portal.

Kenya paid for it: of 340,008 rows in the validated export, the exclusion dropped **295,678 (87%)** — every WCS legacy/v1/v2 row — leaving KEFS from 2024-01 onward. `kenya_monthly_summaries_map` had fallen from 765 rows across 17 districts to 512 across 16, and its history from 2022-12 to 2024-01.

* **CHANGED** `summarize_data()` now derives two frames from the same input. `all_data` keeps every form; `dash_data` is `all_data` minus `exclude_dashboard_ids`. `calculate_fishery_metrics()` reads the unfiltered one; the taxa, districts, gear and monthly dashboard summaries read the filtered one. The filter expression itself is unchanged, only moved.
* **NEW** `all_monthly_summaries`, a fifth uploaded table built by the same recipe as `monthly_summaries` but from `all_data`. Both come from one shared local function, so the two cannot drift apart in schema.
* **CHANGED** `export_portal()` reads `all_monthly_summaries` to build `<country>_monthly_summaries_map`, and keeps pushing the filtered `monthly_summaries` to the MongoDB `dashboard` database. The map artifact's columns are untouched — `country, gaul1_name, gaul_2_name, date, mean_cpue, mean_rpue, mean_price_kg` — because `export_geos()` groups three countries on that key and a different one drops a country from the portal without erroring.

**Named `all_monthly_summaries`, not `monthly_summaries_all`.** `cloud_object_name()` matches by string prefix and then takes `max(updated)`, so the suffixed form would also match the `<prefix>_monthly_summaries` read in `model_fishery_metrics()` and could hand it the wrong file.

**Nothing changes for a pipeline that does not set the option.** Neither `peskas.mozambique.data.pipeline` nor `peskas.zanzibar.data.pipeline` sets `exclude_dashboard_ids`, so `dash_data` and `all_data` are the same frame and every output they already produce is unchanged; the only difference is one additional table in their bucket. The four dashboard summaries and the MongoDB push are unchanged for *every* country, exclusions or not — they were already reading the filtered frame. Covered by `tests/testthat/test-summarize-data.R`, which runs `summarize_data()` with the cloud boundary mocked and asserts both the split and the no-exclusions equivalence.

**Ordering on upgrade.** `export_portal()` now requires `<prefix>_all_monthly_summaries` in the bucket, which `summarize_data()` writes. The country pipelines run them in that order, but the first `export_portal()` run against a bucket predating this release will fail to resolve the object. Nothing in this package's own pipeline runs either function — only `export_geos()` — so coasts.peskas.org keeps serving the truncated Kenya series until Kenya's pipeline rebuilds against 4.8.0 and rewrites `kenya_monthly_summaries_map`.

Commit-level detail is in the commit behind this section.

# coasts 4.7.0

Brings into the hub what Timor-Leste's pipeline had that every country needs — reading KoBoToolbox validation status, and telling the cross-country assets snapshot apart by country — and fixes three upstream defects found while getting there, one of which silently re-downloads a country's entire GPS history.

**Nothing in this release changes what an existing pipeline computes.** Every item is additive or verified byte-identical against live data, and each was measured before it was written. Two things to know before upgrading are at the bottom.

## KoBoToolbox validation status was in one country's repo, with two live bugs

Any pipeline writing validation flags to the shared database has to read KoBoToolbox's current status first, or it overwrites approvals enumerators entered by hand. The functions existed only in `peskas.mozambique.data.pipeline`, so Timor-Leste had to port them — and fixed three things doing so.

* **NEW** `list_validation_statuses()` — the bulk read. The data endpoint returns `_validation_status` alongside `_id` for 1,000 submissions per request, so an asset costs `ceiling(n / 1000)` requests instead of `n`. Measured on Timor's v2 form: **65 requests and ~70 seconds**, against more than twenty minutes for a per-submission loop spread over ten `furrr` workers. It also covers *every* submission rather than only those a previous run flagged, so an approval entered by hand on a never-flagged submission is seen.
* **NEW** `get_validation_status()` / `update_validation_status()` — the single-submission read and the write-back. The latter **mutates the live form**; there is no development KoBoToolbox instance, so `R_CONFIG_ACTIVE` does not isolate it.
* **404 is the normal case, and it must not throw.** KoBoToolbox answers 404 for a submission that has never been validated. With `httr2`'s default the `"not_validated"` branch is unreachable and every unvalidated submission is recorded as `fetch_error = TRUE`. `kobo_request()` sets `req_error(is_error = ~ FALSE)` so the branch works and `fetch_error` keeps meaning a real transport failure.
* **Either credential.** A token or a username and password. Timor's `KOBO_TOKEN` belongs to a user with no data access to its own assets — 200 on `/assets/<id>/`, 404 on `/assets/<id>/data/` — while the pair ingestion already uses works on both. A country should not need a second credential for this.

Verified live against Timor's v3 asset: 22,250 rows in 22.3 s over 23 requests, `all.equal()` TRUE against the implementation it was ported from, and the never-validated path returning `not_validated` with `fetch_error = FALSE`.

## The assets snapshot could not tell you which rows were yours

`ingest_assets()` writes a **cross-country** snapshot — 1,609 taxa rows, 96 gears, 49 vessels, 736 landing sites over four countries — and dropped the `country` column the frame carries. Alpha-3 codes cannot substitute for it: every one of Timor-Leste's 56 codes is also used by Kenya, Mozambique or Zanzibar, two of them (`MZZ`, `PWT`) against a *different* `scientific_name`, so filtering by code alone mixes another country's taxonomy into a coefficient fetch. The workaround was hardcoded Airtable record ids in a config file.

* **NEW** `country` on `taxa`, `gear` and `vessels`. Measured against the live frame, adding it changes **no** row counts — taxa 1,609 → 1,609, gear 96 → 96, vessels 49 → 49, sites 736 → 736 — so `distinct()` does not fan out.
* Values are **trimmed**. `country` is free text in the frame and the taxa table's Timor rows carry a trailing newline (`"Timor-Leste\n"`). Untrimmed, the whole point of the column fails silently: `country == "Timor-Leste"` matches nothing.
* **NEW** `latitude` / `longitude` on `sites`, populated for 343 of 736. `landing_sites.Country` is deliberately *not* added — it is a linked record, so it arrives as an Airtable record id (`rec8G5G9FZCFZBFyc`), useless as a filter key. Sites stay keyed by `form_id`.
* **FIXED** the snapshot is now uploaded through `resolve_storage_opts(conf, "coasts")`. 4.6.0 fixed `enrich_taxa()` to read and write the hub; `ingest_assets()` was missed and still wrote the country bucket, while both readers — `ingest_pds_trips()` and `enrich_taxa()` — resolve the hub. Invisible inside this package, where the two are the same bucket; from a downstream package that defines `storage.google.options_coasts` it meant the snapshot landed where nothing read it.

Verified end to end with `ingest_assets(package = "peskas.timor.data.pipeline")`: the row counts above, `country` values `Kenya | Mozambique | Timor-Leste | Zanzibar` with no stray whitespace, 60 taxa / 9 gears / 2 vessels selectable for Timor-Leste — identical to what the record-id workaround returns.

## "No tracks stored" was indistinguishable from a bucket in another layout

`extract_trip_ids_from_filenames()` was `gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)`, and `gsub()` returns a **non-matching name unchanged**. A tracks bucket written under any other convention therefore yielded full object names where trip ids were expected, the `setdiff()` above reported every trip as new, and `ingest_pds_tracks()` re-fetched the entire history from the PDS API. Measured against a real bucket: **0 of 99,219 stored tracks recognised**, and a first run that would have re-downloaded 98,472 of them. Silent, expensive, and it looks exactly like a fresh bucket in the log.

* The id is derived from the configured prefix, so an unexpected layout yields the whole name — visibly not an id — instead of passing for one. `prefix` is a required argument: a default would be a guess, and a wrong guess is the failure this removes.
* A non-empty bucket in which **no** object yields an id now stops, naming the prefix and the first object.
* `backup_tracks()` was calling the same helper on `unique(latest_df$Trip)` — trip ids from a bound parquet, not filenames, where the regex was a no-op. That call is dropped, so the helper has one meaning.
* `?ingest_pds_tracks` now records that track objects are the **one unversioned Peskas artefact**: re-ingesting overwrites in place, `cloud_object_name(version = "latest")` cannot be used on that bucket, and existence is therefore decided entirely by object name.

No behaviour change for anyone running today, checked rather than assumed: `pds-peskas-coasts`, `pds-peskas-coasts-dev`, `pds-mozambique-dev`, `pds-mozambique-prod`, `pds-kenya-dev`, `pds-kenya-prod`, `pds-zanzibar-dev` and `pds-timor-dev` all store `pds-tracks_<id>.parquet`, and the new derivation returns the ids the regex did for every one.

**Known limit:** the guard covers "objects exist but none parse", not "the prefix matches nothing". An empty listing is still treated as an empty bucket, because after the prefix filter it is one.

## `resolve_storage_opts()` knows the API bucket

* **NEW** `type = "api"`, resolving `storage.google.options_api`. Optional, like `"public"` — `coasts` itself configures no API bucket. `summarize_data()` was reaching into the config by hand, the one thing every downstream call site is told not to do.
* `summarize_data()` now resolves all five of its storage targets through the helper. **No bucket moves for anyone**, verified by resolving every type against `coasts`', Mozambique's and Timor's configuration in both environments. Its API read now fails with `No storage options configured for type 'api'` instead of an obscure authentication error inside the download, and its fishery-metrics upload gained the `options_coasts` → `options` fallback it lacked, which was `NULL` for `coasts` itself.
* The `asfis` and grid-summary reads deliberately **keep** the country bucket, and now say why. Worth checking rather than assuming: `asfis` is held per country — one object each in `mozambique-dev`, `mozambique-prod`, `kenya-dev` and `zanzibar-dev`, **none** in `peskas-coasts` or `peskas-coasts-dev` — and the grid summaries are written to the country bucket by `preprocess_pds_tracks()`. Both reads already agreed with their writers; moving either to the hub would have broken all three countries on their next run.

## Selenium

`rfishbase::estimate()` models seven nutrients and `enrich_taxa()` selected six. A country publishing nutrition figures against this table could not source all of them from it.

* **NEW** `selenium` in `taxa-fishbase-enriched`. Both servers carry it with data — 5,696 non-NA rows in FishBase's `estimate` table, 359 in SeaLifeBase's.

Verified by regenerating the table against the live production assets snapshot: **5,318 rows before and after, 21 → 22 columns, none lost, all 21 pre-existing columns `all.equal()` TRUE**, 2,540 rows carrying a selenium value.

Two things are deliberately **not** upstreamed, now recorded in `?enrich_taxa` with the units the table is actually in: unit normalisation (the seven nutrients arrive in mg, μg and g per 100 g) and a food-composition override for the invertebrates the models cannot estimate. Either applied here would silently rescale or substitute figures three countries already publish. Both belong to the country that wants them.

## Before you upgrade

* **The assets snapshot changes independently of this release.** Kenya, Mozambique and Zanzibar read `assets__*` at `version = "latest"` from the hub *at runtime*, so the three new columns reach them on their next scheduled run whether or not they have rebuilt against 4.7.0. Each feeds `taxa`, `gear`, `vessels`, `sites` and `geo` into a chained join, where a second mapping table carrying `country` collides with the first and dplyr suffixes it to `country.x` / `country.y`. The effect is cosmetic — no error, no row-count change, and an explicit positive `select()` keeps it out of the cross-country API parquet — but the clean fix is to drop the columns where the snapshot is read: `dplyr::select(-dplyr::any_of(c("country", "latitude", "longitude")))`. All three country pipelines landed this on 2026-08-13, before the hub wrote its first snapshot carrying the columns.
* **First tests in the package.** `tests/testthat/` arrives with three assertions over the track-id parser, including the retired `pds-track-<id>__*__.csv.gz` family as the negative case. Note that `R CMD check` could not have caught the one defect found during this work — `codetools` does not report a call that omits a required argument, so adding one is a source-wide change with no static verification.

Commit-level detail, with the measurement behind every claim above, is in the five commits of #17.

# coasts 4.6.0

Opens the taxa pipeline to countries outside the Indian Ocean, gives it the length-weight and length-length coefficients that country pipelines were each fetching themselves, and corrects the FAO area the Western Indian Ocean countries were being filtered against.

**This release changes the enriched taxa table for Kenya, Mozambique and Zanzibar.** It does so only by *adding*: the new default is a strict superset of 4.5.0's output — 1,265 rows gained, **zero lost** — because the wrong area was being used, not because anything was reinterpreted. Set `fao_areas = 57` to reproduce 4.5.0 byte-for-byte, which is verified `identical()`.

## The taxa pipeline was filtering against the wrong ocean

`enrich_taxa()` filtered species with `AreaCode %in% c(NA_integer_, 57)`, annotated "Western Indian Ocean". **FAO Area 57 is the Eastern Indian Ocean.** The Western Indian Ocean — the water Kenya, Mozambique and Zanzibar actually fish — is Area **51**. Separately, for any country outside the Indian Ocean altogether the hardcoded filter removes precisely the species the function exists to enrich: Timor-Leste is Area 71, so calling it there enriched almost nothing.

* **NEW** `metadata.fishbase.fao_areas` configuration key and a matching `fao_areas` argument on `enrich_taxa()`. Resolution order is argument, then config, then `c(51, 57)`.
* **CHANGED** the default is now `c(51, 57)`, not `57`. Measured over the production assets snapshot:

  | | 57 (4.5.0) | 51 alone | **51 + 57 (4.6.0)** |
  |---|---|---|---|
  | rows | 4,053 | 4,092 | **5,318** |
  | distinct species | 2,924 | 2,886 | **3,898** |
  | taxon codes covered | 695 | 754 | **773** |
  | rows lost vs 4.5.0 | — | 1,226 | **0** |

  Both areas are kept rather than 51 alone because FishBase's area assignments are incomplete at family level. Restricting to 51 by itself drops 19 taxon codes *entirely*, among them `CLP` — Clupeidae, "Herrings, sardines nei" — whose 15 backbone species all carry area assignments, 2 of which include 57 and none of which include 51. Losing a major Kenyan and Zanzibari fishery to a gap in reference metadata is the wrong trade. `c(51, 57)` keeps all 19.

  Column set and types are unchanged. Trait and nutrient column means move by under 4% (`omega3` +3.5%, `vulnerability_fishing` −3.7%, the rest smaller) — the arithmetic of averaging over 33% more species, not a change in any species' values.
* **NEW** `resolve_fao_areas()` / `filter_by_fao_area()` - the resolution and filtering steps, factored out so the new coefficient functions apply the same rule.

## There were no length-weight or length-length coefficients at all

`enrich_taxa()` emitted traits and nutrients only. Every pipeline that needed to turn a measured length into a weight fetched `a` and `b` itself, and length-length conversion existed in exactly one place, unshared.

* **NEW** `get_length_weight_coeffs()` - `a`, `b`, `aTL`, `Type`, `EsQ`, `LengthMin`/`LengthMax` and study metadata, keyed `alpha3_code` → species → coefficients, from **both** FishBase and SeaLifeBase. Dual-server matters more than it sounds: `rfishbase::species()` is FishBase-only, which is why Timor-Leste had no coefficients whatsoever for 11 of its 56 taxon codes. SeaLifeBase supplies 7 of them — cockles, sea cucumbers, cuttlefish, octopus, penaeid shrimps, spiny lobster and flyingfish now have coefficients where before they had none.
* **NEW** `get_length_length_coeffs()` - `aL`, `bL`, `Length1`, `Length2`, filtered by default to `TL`/`FL`. Required, not decorative: where a survey records fork length and the coefficients are expressed in total length, every catch record carrying a measured length otherwise yields no weight at all.
* **NEW** `get_taxa_morphometrics()` - one call, one expansion, both tables back, guaranteed consistently keyed.
* **No filtering on `Type` or `EsQ` is applied**, deliberately. Restricting to `Type == "TL"` — the obvious simplification, and what one existing pipeline does — was measured to discard more than half the matched species for four of Timor's taxon codes (`CJX` 10 → 3, `EMP` 25 → 12, `MOB` 9 → 4, `YDX` 11 → 4). Since a weight estimate aggregates coefficients across the species in a code, that silently shifts the estimate. The caller filters.

## Filtering coefficients by FAO area costs 40% of them

Restricting by area is right for distributional traits. For `a` and `b` it is a coverage decision, not a correctness one — body form does not stop applying at an FAO boundary, and FishBase's area assignments are incomplete. Over Timor's 57 taxa, area 71 yields 764 species with usable coefficients against 1,283 unrestricted.

* **NEW** `filter_by_area` argument on `get_taxa_morphometrics()` (default `TRUE`, i.e. filter). The measured trade-off is tabulated in `?get_taxa_morphometrics`.
* **NEW** `strip_parentheticals` argument on `expand_taxonomic_info()` (default `FALSE`). Some FAO names carry a bracketed synonym — `"Haemulidae (=Pomadasyidae)"`, `"Labridae (ex Scaridae)"`, `"Selachimorpha (Pleurotremata)"` — and match nothing as written. It is off by default because switching it on changes results for taxa existing pipelines already publish: `Haemulidae` gains 138 species and `Labridae` 569. Enable it per pipeline, and re-baseline when you do.
* Documented, not fixed: `expand_taxonomic_info()` matches the FishBase/SeaLifeBase backbone, so it resolves nothing for tribes (`Thunnini`), infraorders (`Reptantia`, `Brachyura`), informal groupings (`Osteichthyes`, `Algae`) or superseded binomials (`Leiognathus equulus`, now *L. equula*). These need a synonym or common-name route.

## The enriched snapshot was written where nothing reads it

`enrich_taxa()` wrote to the country bucket while every reader — `ingest_pds_trips()` among them — looks in the shared hub via `resolve_storage_opts(conf, "coasts")`. The hub holds 118 production copies of the object, so the hub was already the de-facto home and the writer was simply pointing elsewhere.

* **CHANGED** `enrich_taxa()` resolves both the assets snapshot it reads and the enriched table it writes through `resolve_storage_opts(conf, "coasts")`. Inside `coasts` the hub and country buckets are the same object, so this is a no-op; from a downstream package defining `storage.google.options_coasts` it stops scattering per-country copies.

## Nothing retried anything

There was no retry logic anywhere in the package — not in the API layer, not in storage. The largest request in the pipeline, the PDS trip fetch, streams the entire trip history in one response, and a `Recv failure: Connection reset by peer` on it failed a production run on 2026-07-31.

* **ENHANCED** `get_trips()` and `get_trip_points()` gain `max_tries` (default 5) and wrap the request in `httr2::req_retry(retry_on_failure = TRUE)`, so low-level transport failures — not just HTTP status codes — are retried with exponential backoff. HTTP 429 and 5xx are treated as transient.
* **FIX** `get_trips()` sends `imeis` as a comma-separated query parameter, so a long device list built a URL past the 8192-byte limit that nginx, Apache and most CDNs enforce, and the server answered **HTTP 400 Bad Request** — an error that gives no hint the URL was the problem. At ~16 characters per IMEI only about 500 devices fit. Registering Timor-Leste in this release took the device list from 408 to 850 and the URL from 6,639 to 13,711 characters, which broke `predict_pds_tracks()`, the one caller that filtered server-side rather than locally. Fixed at both ends:
  - `get_trips()` now splits an oversized `imeis` list across as many requests as needed and row-binds the results, so no caller can build an over-long URL. New `max_url_chars` argument, default `7000`.
  - **CHANGED** `predict_pds_tracks()` no longer passes `imeis` at all; it fetches the window once and filters locally, the same way `ingest_pds_trips()` always has. This is also the faster route, because the `imeis` parameter forces the server to re-scan the whole date window once per chunk: measured over 90 days with 850 devices, three chunked requests took 11.5s against 2.8s for one unfiltered request, for a 2% overfetch. The gap widens with the window — three scans of the 2018-onward history did not finish in ten minutes.

  Verified against the live API: the two routes return identical trip sets.
* **NEW** `with_storage_retry()`, `insistent_upload_cloud_file()`, `insistent_download_cloud_file()` - `purrr::insistently()` plus `purrr::rate_backoff(pause_cap = 300, max_times = 10)`, the policy country pipelines were each maintaining by hand. The existing `upload_cloud_file()` / `download_cloud_file()` are untouched, so nothing changes for current callers until they opt in.
* **FIX** `ingest_pds_tracks(batch_size = n)` sliced with `new_trip_ids[1:n]`, which pads with `NA` once `n` exceeds the number of trips remaining — so the final batch of a run sent trip id `NA` to the API once per slot in the overshoot. Now `utils::head()`, which returns what is left instead. Unreachable from the pipeline, which never passes `batch_size`, but it hit interactive backfills, and the retry logic above had just multiplied its cost: a malformed id answered with a 5xx would be retried five times with backoff rather than failing once.

## `aggregate_pds_effort()` could not absorb a region's history in one run

Registering Timor made the unaggregated delta the entire store at once — 25.2M points across 101,246 trips, against the 99,078 points and 266 trips of a normal daily run, **254x**. The run was killed by the operating system rather than failing in R, and because state is written only at the end, nothing was saved and every retry met the same wall. A GitHub-hosted runner has 7 GB, so this would have failed in production the same way.

Two independent causes, both fixed, and neither sufficient alone:

* **FIX** `prepare_tracks_for_effort()` passed every point to `h3jsr::point_to_cell()` in a single call. That function is a bridge to a JavaScript H3 build, not compiled code — it materialises one object per point in a V8 heap that grows with the input and is never returned to the OS. Now blocked through `h3_index_chunked()` at 100,000 points per call. Measured at resolution 9, peak resident memory scales at **0.22 GB per million points instead of 1.6**, while wall time is unchanged (at 4M points, 1.75 GB / 21.3s blocked against 2.69 GB / 21.7s in one call). Output is byte-identical, verified to 4M points and across block boundaries.
* **NEW** `batch_size` argument on `aggregate_pds_effort()`, default `8000`. Blocking the H3 call alone does not rescue the backfill — measured end to end, the post-download work costs **0.95 GB per million points**, so 25.2M needs roughly 25 GB — and no single machine in this pipeline has that. The backlog is therefore read a batch at a time.

  **The loop is inside the function.** `aggregate_pds_effort()` is still called exactly as before, with no arguments, and still does the whole backlog; `batch_size` only sets how much is in flight at once. A normal delta (~307 files) is well under one batch and runs precisely as it always has, so the GitHub Actions step is unchanged.

  Each batch checkpoints the aggregation state before the next begins, so an interrupted backlog resumes from the last completed batch rather than starting over — the failure that motivated this, where hours of reading were lost to a crash at the end. The grid is derived once after the last batch instead of per batch, since it is rebuilt from the whole store either way.

  Batching does not change the result: trips that might share pings are recalled whatever batch they fall in, and the grid is derived from the entire store. Verified — a store accumulated over 11 batches and one built in a single pass agree on cell count, row count, `unique_trips` and per-cell effort, differing only by floating-point summation order (max 5.6e-17 hours on 4 of 49,139 rows).

  Files are taken in trip **start-time** order rather than by object name. Both are deterministic, but `recall_overlapping_trips()` pre-filters the registry on the batch's own minimum and maximum span before its many-to-many join on device; a name-ordered batch is scattered across every year in the store, which makes that span the whole store and leaves the pre-filter doing nothing. A batch also leaves the queue whether or not it could be read — files that fail stay out of the *manifest* so a later run retries them, but selection is deterministic, so re-offering them to the same loop would never terminate.

* **FIX** `download_predicted_files()` held the per-file frames and the bound result live at the same time, roughly doubling peak memory at the worst moment. The wrapper list is now dropped before `bind_rows()`, so the parts are freed as they are consumed.
* **FIX** `drop_overlapping_pings()` tested for shared pings with `duplicated(ping) | duplicated(ping, fromLast = TRUE)`. `duplicated()` on a *data frame* first rebuilds it as one list element per row — `do.call(Map, ...)` inside `duplicated.data.frame` — and that version pays for it twice. Measured on the `(timestamp, latitude, longitude)` triple, the cost is **705 MB and 8.2s per million rows against 50 MB and 0.04s** for `vctrs::vec_duplicate_detect()`, which expresses the same "duplicated anywhere" flag in a single C-level pass. Unnoticeable on a daily delta; ~18 GB and an out-of-memory kill on a full backfill, *after* the tracks have been downloaded. Output is `identical()` to the previous implementation, verified including the `NA`/`NaN`/`-0` cases. `vctrs` added to `Imports` (already a transitive dependency via dplyr).

## A bad coordinate could abort a run, or quietly move fishing to the Arctic

Nothing between the PDS API and the effort grid checked that a position was a position. `h3jsr::point_to_cell()` fails in opposite directions on the two ways it can be wrong, and the quiet one was doing the real damage. Neither is a regression — both were reachable before — but a backfill reading 101,246 files across eight years is far likelier to meet them than a 300-file daily run.

* **FIX** a non-finite coordinate (`NA`, `NaN`, `Inf`) raised *Latitude or longitude arguments were outside of acceptable range* and **aborted the entire run**. One bad ping among millions was enough, and because the aggregation state is only uploaded at the end, nothing was saved and the retry met the same ping again.
* **FIX** an out-of-range *finite* coordinate raised nothing whatsoever. Despite that error's wording, H3 does not reject these — it wraps them and returns an ordinary-looking cell. Latitude is where it does harm, because the wrap reflects over the pole and swings longitude by 180°: measured at resolution 9, a point off the Tanzanian coast with a corrupt latitude of 91 returns a cell at **89°N, 141°W — the Arctic Ocean, some 10,000 km away**, and a latitude of 900 lands in the mid-Pacific. Either entered the grid as a real hexagon carrying real fishing hours, with no error and no log line.
* **NEW** `valid_coordinates()` — the shared test for both. `prepare_tracks_for_effort()` now drops unusable points and warns with the count; `assign_h3_indices()` used it to replace an `is.na()` filter that caught neither case.

  Points are dropped **before** intervals are measured, not after the H3 column is added. An unusable ping still carries a good timestamp but has no cell to credit its interval to, so removing it first hands that time to the next ping that does have a position, where `max_gap_hours` limits it like any other silence. Removing it afterwards would have measured an interval into a row about to be discarded and silently lost that stretch of the trip. Verified: a track with points poisoned three different ways yields exactly the same total hours as the same track cleaned beforehand.

  Out-of-range *longitude* is rejected too, though it is the harmless case — longitude is cyclic, so 181° resolves correctly to 179°W. A device reporting outside ±180 is reporting something wrong, and the value being recoverable is no reason to trust the rest of that ping.

* **FIX** `h3_index_chunked()` and `assign_h3_indices()` return an empty result for empty input instead of failing. `point_to_cell()` errors on zero rows (*arguments imply differing number of rows: 2, 0*), which the new filtering makes reachable whenever every position on a trip is unusable.

On clean data all of this is a no-op: output is `identical()` to the previous behaviour, so no existing effort figure moves.

## Smaller things

* **NEW** `cloud_object_names()` - enumerates every object matching a prefix. `cloud_object_name()` returns `selected_rows$name[1]`, a *single* name, despite grouping internally by base name and extension; pointed at a prefix covering many distinct base names — one object per GPS trip, say — it silently hands back an arbitrary one of tens of thousands rather than erroring. That behaviour is now documented prominently and left exactly as it is, since downstream code depends on it; use the plural form to enumerate.
* **ENHANCED** `resolve_storage_opts()` accepts `type = "public"`, resolving `public_storage.google.options` or `storage.google.options_public`, and a `error_if_missing` argument (default `FALSE`, preserving the previous return-`NULL` behaviour) for callers that would rather fail fast with a message naming the expected key.
* **NEW** `timor` block under `api.trips`, and `"MAF / WorldFish"` added to `pds.customers` — read off the live PDS API, not guessed: 844 devices, timezone `Asia/Dili`, spanning every Timorese municipality. The two other `Asia/Dili` customers, `"Traders"` (17 devices) and `"FSSP2: Traders"` (8), are deliberately excluded — they are trader rather than vessel devices, and `"Traders"` also carries `Asia/Kuala_Lumpur` devices, which would widen the shared device filter for every country.
* **FIX** `expand_taxonomic_info()`'s `alpha3_code` was documented as a "three-letter country/region code". It is the FAO 3-alpha code identifying the *taxon* group.

# coasts 4.5.0

## The same fishing trip was counted many times over

A trip in PDS is not a fixed record. PDS decides where one trip ends and the next begins as data arrives, and revisits that decision later: trip IDs are retired when their points are merged into another trip, and an ID we have already read can end up describing a different stretch of time than it did when we read it. Two examples from the store: 92 different trip IDs fetched on 2026-05-27 all returned the same 304 GPS points (one boat, one night off Vanga) and 91 of those IDs no longer exist today; and our file for trip 14576327 holds the track PDS now files under trip 15040695, while trip 14576327 itself now describes a different day.

`predict_pds_tracks()` read each trip once and never looked at it again, so the predicted-track store filled up with several IDs holding the same GPS points. `aggregate_pds_effort()` counts one file as one vessel, so one boat was counted as many.

Measured on the production grid: 570 of 29,021 predicted files (2.0%) belonged to trips PDS no longer lists, contributing 3,105 duplicated fishing hours (2.2% of the grid) across 892 cell-years. One cell off Vanga held 124 copies of a single 8-hour track and reported 1,001 fishing hours over 2 active days — an `avg_hours_per_day` of 501, meaning roughly 21 boats fishing a 0.12 km² hexagon around the clock.

Copies multiply `fishing_hours`, `unique_trips`, `fishing_pings` and `avg_fidelity_sum`, but not `n_active_days`, which is a union of dates and so stays put. That is why the per-day rates blew up while the per-trip ratios (`hours_per_trip`, `avg_fidelity`) stayed plausible and hid the problem.

* **NEW** `index_trip_duplicates()` - Fingerprints each trip's `(timestamp, latitude, longitude)` set and collapses trips sharing a fingerprint to one survivor, preferring the trip PDS still lists so the surviving cell keeps its `gear`/`country` instead of falling into the `"unknown"` bucket. Duplicates are recognised across runs too, via the new trip registry.
* **NEW** `drop_overlapping_pings()` - Removes pings carried by more than one trip, for the partial overlaps left when PDS moves only part of a track. Runs after `dt_hours` is computed within each trip, so removing a ping cannot turn into an artificial gap that inflates the survivors.
* **NEW** `recall_overlapping_trips()` - Exact copies are caught by fingerprint, but when PDS moves only *part* of a track the two copies overlap without being identical, and they can arrive in different runs — where nothing would ever set them side by side. Before reading a batch, the trips already stored that could share pings with it (same device, overlapping window) are withdrawn and read again alongside it, so the shared pings are deduplicated rather than counted twice for good.
* **NEW** `aggregated_trips.rds` - Registry of every trip seen: its device, window, fingerprint, source object and the trip it duplicates if it was dropped. This is what lets a later run recognise a duplicate, and what lets a withdrawal take the trips discarded in that trip's favour with it.
* **ENHANCED** `predict_pds_tracks()` - Re-predicts trips whose PDS `Updated` timestamp is newer than the file we wrote (`refresh_updated`, on by default, capped per run with `max_refresh`), and deletes files for trips the API no longer lists. 24% of live trips currently carry a PDS revision newer than our snapshot, so the first run works through a backlog. Three things bound the damage this can do:
  - a trip listed twice is judged on its **latest** revision, not on whichever row the API happened to return first, which would otherwise hide a revision behind an older timestamp and leave the trip permanently unrefreshed;
  - only files whose identifier falls inside the range the listing covered can be judged retired. Identifiers rise over time, so a file from before `date_from` is missing from the listing for a reason that has nothing to do with retirement, and running with a later `date_from` no longer deletes the earlier years;
  - a refresh whose fetch fails or comes back empty deletes the stale file rather than leaving it: PDS has said the trip changed, so the snapshot from before it did is the duplicate this pass exists to remove, and keeping it would queue the trip again on every future run. The trip is still listed, so a later run reads it as new. Trips skipped for length are left alone, since they can never be re-predicted.
  
  Both deletions are skipped with a warning above `max_delete_frac` (default 10%), so a truncated listing or an API outage cannot empty the store.
* **ENHANCED** `aggregate_pds_effort()` - The manifest now records the modification time each object was aggregated at, so files deleted or replaced by `predict_pds_tracks()` are detected. Manifest, registry and effort store are loaded as a unit: previously a manifest that downloaded while the grid did not left the pipeline skipping files whose effort was in no grid at all.

## The effort grid is now derived, not accumulated

The grid used to be a running total that each run added to, which meant effort could never be taken back out — the only way to remove anything was to re-read all 29,021 predicted tracks. With PDS revising trips every single day (revisions landed on 180 of the last 180 days, about 29 trips a day), keeping the store in step with the API would have meant a full rebuild every day, hours per run.

* **NEW** `build_trip_effort()` / `derive_effort_grid()` - `aggregate_pds_effort()` now keeps `aggregated_trip_effort.parquet`, one row per trip, cell, year, gear, country and day, and derives the published grid from it in full on every run. Revised trips are withdrawn from the store row by row — along with any trip dropped as a duplicate of them — and only the files that actually changed are re-read; a rebuild is seconds rather than hours. The published grid keeps its columns and values unchanged.
* **FIX** `unique_trips` is now a distinct count over the whole store rather than a sum of per-batch counts, so it no longer depends on how trips were spread across runs, and a trip fishing across midnight on New Year is no longer counted twice.

## Silent devices no longer read as hours of fishing

`dt_hours` is the time since the previous ping, credited in full to the cell holding the *later* one — so a gap does not just inflate the total, it puts unobserved time in one particular hexagon, wherever the vessel resurfaced. The 4-hour limit on that was far too generous. Devices report every few seconds (median interval 7 s across 3,814 trips, 95% of intervals under 1.5 min), yet intervals longer than an hour — 0.6% of the data — produced **half of all fishing hours**, and the 0.15% pinned exactly at the 4-hour limit produced a fifth of them on their own.

* **CHANGED** `prepare_tracks_for_effort()` gains `max_gap_hours` (default `0.25`, i.e. 15 minutes) and `gap_policy` (`"cap"`, the default, or `"drop"` to count only observed time). Fifteen minutes is around 130 missed pings, so normal reporting is untouched, while a single dropout can add at most a quarter of an hour to one cell instead of four. On a clean sample this takes fishing hours to 56% of their former value; `"drop"` takes them to 43%.
* `aggregate_pds_effort()`, `aggregate_trip_effort()` and `model_cpue()` all take and forward both settings, so the H3 grid and CPUE cannot end up measuring effort differently. **CPUE roughly doubles**: the same catch is now divided by hours that no longer include device silence.
* The settings are recorded in `aggregated_settings.rds` beside the effort store. Changing either rebuilds the grid from the predicted tracks rather than mixing hours measured two different ways.

## Incremental state, hardened

* The manifest is uploaded **last** and a trip's rows **replace** rather than add to what is stored, so a run interrupted mid-upload leaves the batch to be read again — harmlessly — instead of recording files whose effort never arrived.
* A file that cannot be read is left out of the manifest, so a transient storage error no longer retires its effort permanently.
* The state check covers every column the derivation reads, so a store written by an older schema rebuilds instead of failing partway through.
* An empty bucket listing returns cleanly again rather than erroring on a column that is not there.
* A model version bump no longer needs a rebuild trigger of its own. `predict_pds_tracks()` deletes every old-version file and writes new ones, which the per-file comparison already sees — and it sees exactly which files moved, where the old check assumed all of them had. Running the aggregation against an unchanged store after a version bump no longer re-reads 29,021 files to arrive at the same grid.
* Gap settings are normalised before comparison, so passing `1L` where `1` was stored no longer sets off an hours-long rebuild for a setting that did not change.
* `prepare_tracks_for_effort()` no longer computes `trip_total_hours`: the fidelity denominator is derived from the store, and the column had no readers left.

## `constancy` is now a fraction of the year, not of the series

* **FIX** `add_cell_effort_metrics()` divided per-year active days by the length of the *whole* study period (977 days), so every cell landed in the third decimal and years could not be compared: a 2023 cell had at most 39 days available but was divided by 977 all the same. The denominator is now the days of that row's year inside the study period. Measured on the production grid, the median stopped being 1/977 for every year alike, and the most consistently fished cell reads 0.72 — fished on 149 of the 207 available days of 2026 — instead of 0.153. `derive_fishing_grounds()` is unchanged: it collapses years before computing the metric, so its numerator and denominator already agreed.

# coasts 4.4.1

* **ENHANCED** `summarize_data()` - New `exclude_dashboard_ids` argument drops listed `survey_id` values from the dashboard summaries. Defaults to the package config at `surveys$summaries$exclude_dashboard_ids`, so each pipeline manages its own list without changing the call; unset keeps all surveys.

# coasts 4.4.0

* **NEW** Gear and country dimension for the fishing-effort grid. `aggregate_pds_effort()` now partitions the H3 grid by `gear` and `country` in addition to `year`, so a cell can hold several rows (one per gear/country combination). Both attributes are resolved per trip (trip → IMEI → Airtable `pds_devices` metadata); trips with no device match are retained under `gear = "unknown"` / `country = "unknown"` so fleet-wide totals never shift. Because each trip maps to exactly one gear and one country, summing over them (and `year`) exactly reproduces the previous all-fleet grid. A one-time full rebuild is triggered automatically when an older grid lacking these columns is detected.
* **NEW** `export_effort_gear_shapefiles()` - Builds, per country, an all-time H3 effort layer broken down by gear class and uploads it as a zipped ESRI Shapefile bundle (the shapefile set plus a `README.txt` data dictionary) to the coasts bucket. Collapses years using the `active_dates` union for exact active-day counts, reuses `create_spatial_grid()` for hexagon polygons, and shortens columns to the 10-character DBF limit (`h3_id`, `gear_class`, `fish_hrs`, `trips`, `n_days`, `hrs_day`).
* **ENHANCED** `export_pds_spatial()` - Now emits a per-gear/country effort JSON (`pds-h3-effort-gear-r{h3_res}`) alongside the existing all-fleet file. The all-fleet effort JSON is kept byte-for-byte identical by collapsing the partitioned grid back over gear/country before export, so the current DeckGL portal is unaffected until a gear/country filter is built. Fishing-ground features now also carry a `country` attribute, derived by a centroid-to-ground spatial join (geometry, metrics, and thresholds are left untouched, since WIO countries are spatially disjoint and each ground falls in exactly one). Shared per-cell metric formulas were extracted into the internal `add_cell_effort_metrics()` helper used by both effort exports.
* `inst/conf.yml` - Added `portal.effort_gear.file_prefix` (`pds-h3-effort-gear-r`) for the new per-gear/country effort JSON.

# coasts 4.3.0
* **FIX** `export_pds_spatial()` per-cell metrics — `avg_hours_per_day` and `avg_visits_per_day` were divided by the whole study period (`n_total_days`, ~850+ days), producing values near zero. They now divide by `n_active_days` (the number of days the cell was actually visited), so the metric matches its name: average fishing hours / trips on days the cell was active. `constancy` (fraction of study period the cell was active) still uses `n_total_days` and is unchanged. Same fix applied to `derive_fishing_grounds()` per-cell metrics.
* **FIX** `aggregate_pds_effort()` — `n_active_days` was double-counted on incremental merge whenever the same calendar day was visited by trips from different aggregation runs (common: many boats fishing the same cell daily produce one parquet per trip, batched separately). The grid now stores `active_dates` as a list-column of `Date` per cell-year; merges take the unique union, and `n_active_days` is recomputed from it. `derive_fishing_grounds()` applies the same union semantics when collapsing years, rolling up to coarser resolutions, and aggregating cells into ground polygons.
# coasts 4.2.1

* **IMPROVEMENT** Filter out NAs in countries taxa summary (in `export_portal()`) to save storage space and loading time

# coasts 4.2.0
* **FIX** Fix critical bug in downloading versioned files

# coasts 4.1.0

* **IMPROVEMENT** Kenya matched trips now combine surveys from all Kenyan sources, not just KEFS — giving a more complete picture of fishing activity in the country.
* **FIX** Restored the fishing-effort aggregation step of the automated pipeline, which had stopped running on the server due to a missing system component.

# coasts 4.0.0

## Spatial CPUE Model Pipeline

* **NEW** `model_cpue()` - Estimates spatial Catch Per Unit Effort (CPUE) by joining matched survey trips with predicted PDS tracks. Supports two estimation methods: `"weighted"` (direct catch-to-effort ratio, robust for sparse data) and `"nnls"` (non-negative least squares, for denser datasets). Uploads results as a versioned parquet to cloud storage.
* **NEW** `run_weighted_cpue()` - Computes CPUE as `sum(catch_kg) / sum(fishing_hours)` per H3 cell and country.
* **NEW** `run_nnls_cpue()` - Solves a non-negative least squares system `min ||Xq - y||² s.t. q ≥ 0` across all H3 cells simultaneously.
* **NEW** `join_effort_catch()` - Builds the effort-catch matrix linking per-trip H3 effort vectors with catch records.
* **NEW** `load_matched_trips()` - Downloads the `trips-matched` parquet and returns validated catch records for matched PDS trips.
* **NEW** `download_predicted_tracks()` - Downloads predicted track files for a set of matched trip IDs from the PDS bucket.
* **NEW** `prepare_tracks_for_effort()` - Projects predicted fishing points into an H3 effort matrix (fishing hours and pings per cell).
* **NEW** `get_combined_tbl()` - Combines effort and catch into a single analysis table for CPUE modelling.
* **NEW** `build_catch_wide()` - Pivots catch records to a wide matrix (trips × species) for the NNLS solver.
* **NEW** `.finalise_cpue()` - Post-processes raw CPUE estimates: adds centroid coordinates, filters cells below `min_trips`, and attaches country labels.
* **NEW** `.top_species()` - Selects the top-N species by total catch weight to focus CPUE estimation.

## Web-Ready Spatial Export

* **NEW** `export_pds_spatial()` - Reads H3 effort grid and CPUE parquet files from cloud storage, derives fishing grounds, and uploads three web-ready files for the DeckGL portal: H3 effort JSON, CPUE JSON, and fishing grounds GeoJSON.
* **NEW** `derive_fishing_grounds()` - Converts an H3 effort grid to a GeoJSON `FeatureCollection` of discrete fishing ground polygons, enriched with area, constancy, and activity metrics.
* **NEW** `aggregate_trip_effort()` - Aggregates per-trip H3 effort vectors into a cumulative effort grid across all trips.
* **NEW** `plot_effort_map()` / `plot_cpue_map()` - Interactive Leaflet maps for visualising effort and CPUE grids during exploratory analysis.
## Taxa Enrichment

* **NEW** `enrich_taxa()` - Augments catch records with FishBase and SeaLifeBase taxonomic backbone data (class, order, family, genus) for all species in the matched trips dataset.
* **NEW** `get_taxa_backbone()` - Queries the GBIF taxonomic backbone to resolve species names to canonical taxonomy.
* **NEW** `expand_taxonomic_info()` - Expands the taxa lookup table with full higher classification.

## Bug Fixes

* **FIX** `aggregate_pds_effort()` - Manifest was silently uploaded to a temp-dir GCS path instead of the correct `{grid_prefix}/aggregated_manifest.rds` key, causing incremental processing to always rebuild the entire grid from scratch. Fixed by passing `name = manifest_name` explicitly to `upload_cloud_file()` in both the main and early-return paths.
* **FIX** `model_cpue()` - Removed dead code left from an earlier refactor (`map_effort`, `map_cpue`, `out_dir` block) that caused an R error at runtime: *"object 'map_effort' not found"*.
* **FIX** `export_pds_spatial()` - No longer crashes with a cryptic 404 when the effort grid parquet does not yet exist in GCS (e.g. first run or after manual deletion). The function now logs a warning and returns early, matching the existing behaviour for the CPUE file.
* **FIX** HTTP/2 `PROTOCOL_ERROR` failures on GCS uploads in CI — `upload_cloud_file()` now calls `cloud_storage_authenticate(force = TRUE)` unconditionally before every upload. Service-account tokens expire after 1 hour; long upstream jobs (e.g. `predict_pds_tracks`) can exhaust this window, causing `gargle` (which uses `httr2`) to attempt a mid-flight token refresh over a stale HTTP/2 connection. Forcing fresh re-auth before the upload avoids this path entirely.

## CI / Workflow

* Merged `predict-pds-tracks` and `aggregate-pds-effort` pipeline jobs into a single job — they are always sequential and sharing a container saves startup overhead.
* Deleted superseded `model-tracks.yaml` workflow (its steps are fully covered by `data-pipeline.yaml`).
* Fixed `pkgdown.yaml` deploy step: added required `environment: name: github-pages` block (needed by `actions/deploy-pages@v4`); bumped `actions/upload-pages-artifact` to `@v4` (native Node 24 support); added Changelog to pkgdown navbar.

## Naming & Versioning Coherence

* CPUE parquet files are now stored under `pds-cpue_r{h3_res}` (e.g. `pds-cpue_r9`) to match the effort grid naming convention (`predicted-pds-h3_grid_r9`). This ensures that running the pipeline at different H3 resolutions never silently mixes effort and CPUE data from different resolutions.
* Portal CPUE JSON files follow the same pattern: `pds-cpue-r{h3_res}__timestamp__json`.
* `inst/conf.yml` `portal.cpue.file_prefix` updated from `pds-cpue` to `pds-cpue-r`.

## Documentation & Website

* Vignettes (`pipeline.Rmd`, `metrics-and-models.Rmd`) moved from project root to `vignettes/` so pkgdown can discover them correctly.
* pkgdown CI workflow (`pkgdown.yaml`) fixed: system dependencies (GDAL, GEOS, PROJ, udunits2) now installed before `r-lib/actions/setup-r-dependencies@v2`.
* `_pkgdown.yml` articles section re-enabled now that vignettes are in the correct location.

# coasts 3.0.1

Align export functions according to countries API schema

# coasts 3.0.0

## Fishing Activity Prediction Pipeline

A new end-to-end pipeline for classifying GPS boat tracks into fishing and non-fishing activity using the `ssfaitk` statistical model, and aggregating the results into spatial effort maps.

### New Workflow Functions

* **NEW** `predict_pds_tracks()` - Downloads GPS tracks for all active vessels, applies the `ssfaitk` fishing activity model to each trip, and uploads fishing-only point files to cloud storage. Implements version-aware incremental processing: trips already classified with the current model version are skipped, and files from outdated model versions are automatically replaced when the model is updated.

* **NEW** `aggregate_pds_effort()` - Consolidates all classified fishing tracks into a single H3 hexagonal grid representing cumulative fishing effort across the fleet. Counts fishing pings and unique trips per cell and uploads the grid as a versioned parquet file ready for portal consumption.

### New Spatial Analysis Utilities

* **NEW** `assign_h3_indices()` - Maps GPS coordinates to H3 hexagon cell IDs at any resolution
* **NEW** `aggregate_h3_effort()` - Summarises fishing pings and unique vessel counts per H3 cell
* **NEW** `rollup_h3_resolution()` - Re-aggregates effort from a fine H3 resolution to any coarser level for multi-scale analysis
* **NEW** `create_spatial_grid()` - Converts an H3 effort table to an `sf` polygon object for mapping
* **NEW** `prep_fishing_points()` - Projects raw GPS coordinates to a metric CRS for distance-based spatial operations
* **NEW** `create_reference_grid()` - Generates a deterministic square or hexagonal reference grid over a study area
* **NEW** `aggregate_daily_effort()` - Counts fishing pings per reference grid cell via spatial join

### Automated Pipeline

* **NEW** GitHub Actions workflow (`model-tracks.yaml`) - Runs the full fishing activity prediction and effort aggregation pipeline every two days. Always fetches the latest `ssfaitk` model version at runtime, so improvements to the underlying model are picked up automatically without rebuilding the Docker image.

### Infrastructure

* **ENHANCED** Docker image - Added Python environment support required by the `ssfaitk` model, including automatic Python path configuration for `reticulate`

# coasts 2.2.7

* **IMPROVEMENT** Use scientific names rather than FAO alpha3 codes for dashboard data

# coasts 2.2.6

* **FIX** Update PDS ingestion and preprocessing according to new config paths

# coasts 2.2.5

* **FIX** Clarify `resolve_storage_opts()` arguments

# coasts 2.2.4

* **FIX** Export `resolve_storage_opts()`

# coasts 2.2.3

* **FIX** Refine pds ingestion functions to improve comatibility with country pipelines

# coasts 2.2.2

* **NEW** Upgrade and optimize all functions related to pds ingestion and preprocessing in order to be compatible with current countries pipelines. This means countries data flows follow the same data processing enhancing processes mangeabiity and data consistency

# coasts 2.2.1

## Minor fix

Add "version" argument to `download_parquet_from_cloud()`

# coasts 2.2.0

## New Features

* **NEW** Upgrade and optimize all functions related to storage (google cloud and mogodb auth, download and upload). These will then
replace the exsisting related functions for all the countries pipeline for improved manageability and centralization of common processes
# coasts 2.1.0

## New Features

* **NEW** `get_kobo_data()` - upgraded function to pull data from kobotoolbox according to [Kobo API changes](https://community.kobotoolbox.org/t/important-changes-to-api-v2-assets-uid-asset-data-result-limits/74610). The new function will replace the exisitn g data pulling process in all the pipeline for improved manageability and centralization of common processes

* **BUG FIX** Fixed bug related to the automatic generation of credentials of Peskas Tracks App.


# coasts 2.0.0

## Refactoring

Optmize package to export and process data from countries pipelines

# coasts 1.5.0

## New Features

### Survey & Fleet Analysis Pipeline
* **NEW** `summarize_data()` - End-to-end summarization of WorldFish survey data into five output tables (monthly, taxa, district, gear, grid summaries) uploaded to cloud storage as versioned parquet files
* **NEW** `calculate_fishery_metrics()` - Transforms catch-level records into normalized fishery indicators (site-level CPUE/RPUE, predominant gear, species composition) in long format for portal consumption
* **NEW** `generate_fleet_analysis()` - Orchestrates full fleet activity estimation pipeline and uploads aggregated results to cloud storage
* **NEW** `prepare_boat_registry()` - Constructs a boat registry from asset metadata for scaling GPS-tracked data to fleet-wide estimates
* **NEW** `process_trip_data()` - Processes PDS API trip records by device IMEI into per-trip summaries
* **NEW** `calculate_monthly_trip_stats()` - Aggregates trip data to monthly statistics per district
* **NEW** `estimate_fleet_activity()` - Scales GPS-sampled trips to fleet-wide activity estimates using boat registry sampling rates
* **NEW** `calculate_district_totals()` - Joins fleet estimates with survey summaries to produce district-level catch and revenue totals

### Data Export
* **NEW** `export_portal()` - Downloads WorldFish summary datasets from cloud storage, joins modelled aggregate estimates, pivots to long format, and uploads all tables to MongoDB portal collections

## Enhancements

### Multi-Package Architecture
* **ENHANCED** `read_config()` - Added `package` argument (default `"coasts"`). Downstream packages that ship their own `inst/conf.yml` can now call `read_config(package = "mypackage")` to load their own configuration instead of the `coasts` defaults
* **ENHANCED** All 12 top-level pipeline functions now accept a `package` argument threaded through to `read_config()`: `ingest_pds_trips()`, `ingest_pds_tracks()`, `backup_tracks()`, `ingest_assets()`, `preprocess_pds_tracks()`, `merge_survey_trips()`, `get_metadata()`, `summarize_data()`, `export_geos()`, `export_fishers_stats()`, `export_portal()`, `generate_fleet_analysis()`

### Automated Workflows
* **ENHANCED** `app-usage-report.yaml`, `sync-devices-users.yaml`, `tracks-backup.yaml` - All jobs now carry an explicit `if: github.ref == 'refs/heads/main'` guard, ensuring workflows triggered via `workflow_dispatch` on non-main branches are safely skipped

## Package Infrastructure
* **ENHANCED** `DESCRIPTION` - Migrated `Author`/`Maintainer` fields to `Authors@R: person(...)` format (fixes R CMD check WARNING); removed spurious `LazyData: true` (no `data/` directory); added `URL` and `BugReports` fields pointing to the GitHub repository
* **ENHANCED** `_pkgdown.yml` - Added new "Survey & Fleet Analysis" reference section; added `export_portal()` to "Data Export & Storage"; removed two non-exported internal helpers that would have caused build errors

# coasts 1.4.0

## New Features

### GPS-Survey Trip Matching
* **NEW** `merge_survey_trips()` - Downloads matched GPS and survey trip data across regions, harmonizes columns, and combines into a single dataset

### Automated Workflows
* **ENHANCED** GitHub Actions data pipeline with new `match-trips` job


### Multi-Bucket Regional Storage
* **ENHANCED** `download_parquet_from_cloud()` and `upload_parquet_to_cloud()` - Added `bucket_name` parameter to download/upload from regional buckets (Kenya, Mozambique, Zanzibar)
* **ENHANCED** `inst/conf.yml` - Regional bucket configuration with environment-specific bucket names (dev vs prod)

## Code Organization
* Refactored PDS ingestion and API functions into dedicated files (`R/ingestion-pds.R`, `R/pds-api.R`)
* Updated pkgdown reference index with tracks app and preprocessing sections

# coasts 1.3.0


### Fisher Performance Analytics
* **NEW** `export_fishers_stats()` - Comprehensive fisher performance analysis and export
  - Integrates catch events from tracks-app with GPS tracking data from PDS API
  - Matches fisher-reported landings with automated trip tracking by date and device
  - Calculates fishing efficiency metrics: CPUE (kg/hour, kg/km), search efficiency ratios
  - Estimates fuel consumption and catch per liter efficiency
  - Categorizes trips by distance (nearshore, mid-range, offshore)
  - Exports aggregated fisher statistics and trip-level performance metrics to MongoDB

### Automated Workflows
* **ENHANCED** GitHub Actions data pipeline workflow
  - Added `export-fishers-stats` job to automated pipeline
  - Runs after track preprocessing to ensure data availability
  - Automatically exports fisher performance data on every pipeline run

### Development Experience
* **NEW** `.Rprofile` - Interactive environment switching for local development
  - Added helper functions: `use_prod()`, `use_local()`, `use_default()`
  - Visual environment indicator on R session startup
  - Quick commands reference displayed in interactive sessions
  - Simplified testing across different configuration profiles

## Configuration Updates

### MongoDB Collections
* **ENHANCED** tracks-app MongoDB configuration in `inst/conf.yml`
  - Added `fishers-stats` collection for aggregated fisher summaries
  - Added `fishers-performance` collection for trip-level efficiency metrics
  - Improved data organization for analytics and reporting

# coasts 1.2.0

## Breaking Changes

### Configuration System Migration
* **BREAKING CHANGE** - Migrated from auth folder to `.env`-based credentials management
  - Removed `local` configuration profile from `inst/conf.yml`
  - All environments now use environment variables loaded via `.env` file in local development
  - Added `dotenv` package dependency for automatic `.env` file loading
  - Created `.env.example` template with all required environment variables
  - Updated `.gitignore` to properly handle `.env` files while tracking `.env.example`
  - **Migration Guide**: Copy `.env.example` to `.env` and fill in credentials (see updated README)

## New Features

### Asset Management
* **ENHANCED** `ingest_assets()` - Comprehensive fisheries asset metadata ingestion
  - Added `log_threshold` parameter for configurable logging
  - Now includes PDS device metadata from Airtable (`pds_devices` table)
  - Retrieves 6 asset types: taxa, gear, vessels, landing sites, forms, and devices
  - Changed output format from parquet to RDS for better R object serialization
  - Added complete roxygen documentation following package standards

### Data Ingestion Improvements
* **ENHANCED** `ingest_pds_trips()` - Improved trip data ingestion workflow
  - Now downloads device metadata from cloud storage instead of Google Sheets
  - Filters devices by `last_seen` date (>= 2023-01-01) for active devices only
  - Enhanced PDS API calls with `deviceInfo` and `withLastSeen` parameters
  - Client-side IMEI filtering for reliable data retrieval
  - Updated documentation with detailed configuration examples and notes

## Documentation

### Package Documentation
* **ENHANCED** README with `.env`-based configuration instructions
  - Added step-by-step local development setup guide
  - Documented all required environment variables with descriptions
  - Separated local and production deployment instructions
* **UPDATED** CLAUDE.md with new configuration system details
  - Revised Configuration System section to explain `.env` approach
  - Updated Configuration Requirements with clear setup steps
  - Added `dotenv` to Key Dependencies section
* **NEW** `.env.example` - Template file for local development credentials
  - Includes all 12 required environment variables with helpful comments
  - Proper formatting examples for complex values (JSON keys, connection strings)

### Function Documentation
* **ENHANCED** `ingest_assets()` with comprehensive roxygen documentation
  - Detailed @description with step-by-step operations
  - Complete @details section with YAML configuration structure
  - Asset type descriptions (Taxa, Gear, Vessels, Landing Sites, Forms, Devices)
  - Added @examples, @seealso, and @keywords tags

## Technical Improvements

### Configuration Loading
* **ENHANCED** `read_config()` function in `R/utils.R`
  - Automatic detection and loading of `.env` file if present
  - Seamless integration with existing `config::get()` workflow
  - Informative logging when `.env` file is loaded

### Development Experience
* Simplified credentials management for local development
* Consistent approach with other peskas packages (e.g., peskas.kenya.data.pipeline)
* Improved security with proper `.gitignore` configuration
* Easier onboarding for new developers with template file

# coasts 1.1.0

* **NEW** - Integrate (Beta) Cabo Delgado (Mozambique) estimates
* **NEW** - Ddeveloping code to integrate catch events records from tracks-app

# coasts 1.0.0

## Major New Features

### Airtable Integration System
* **NEW** `airtable_to_df()` - Convert Airtable tables to R data frames with pagination support
* **NEW** `df_to_airtable()` - Create new records in Airtable tables with batch processing  
* **NEW** `bulk_update_airtable()` - Update multiple Airtable records efficiently (10 record batches)
* **NEW** `update_airtable_record()` - Update individual Airtable records
* **NEW** `get_writable_fields()` - Identify writable fields in Airtable tables (excludes computed fields)
* **NEW** `device_sync()` - Comprehensive sync function for device data (updates existing, creates new)
* **NEW** `ingest_pelagic_boats()` - Complete workflow for PDS boat data ingestion and Airtable sync
* **NEW** `sync_device_users()` - Sync device users to MongoDB with password generation and Airtable updates

### Enhanced PDS API Integration
* **NEW** `pelagic_auth()` - Authentication with Pelagic Analytics API
* **NEW** `pelagic_refresh_token()` - Token refresh functionality for sustained API access
* **NEW** `get_pelagic_boats()` - Retrieve boat information with server-side filtering and column selection
* **NEW** `get_pelagic_devices()` - Retrieve device information with advanced filtering capabilities
* Enhanced `ingest_pds_tracks()` with improved error handling and parallel processing

### Automated Workflows
* **NEW** GitHub Actions workflow: `ingest-pelagic-boats.yaml` (runs every 15 days)
* **NEW** GitHub Actions workflow: `sync-device-users.yaml` (runs every 10 days)
* Enhanced main data pipeline workflow with improved container management

### Configuration System Improvements
* **BREAKING CHANGE** Restructured MongoDB configuration to support dual databases:
  - `mongodb.coasts_portal` - For main coasts geospatial data
  - `mongodb.tracks_app` - For tracks application user data
* **BREAKING CHANGE** Enhanced Airtable configuration with separate base IDs:
  - `airtable.frame` - For device and country metadata
  - `airtable.tracks_app` - For user management
* Updated environment variable requirements for production deployments

### Documentation and Development
* **NEW** Professional pkgdown website with enhanced theming and navigation
* Enhanced README with status badges and improved structure
* Fixed pkgdown configuration issues with pipe operators and tidy evaluation functions
* Updated function documentation with detailed examples and use cases

## Bug Fixes and Improvements

### Data Processing
* Fixed KES to USD conversion units in `export_geos()`
* Improved MongoDB collection references to use new dual-database configuration
* Enhanced error handling in data ingestion functions
* Better logging and progress tracking across all functions

### API and Authentication
* Robust token refresh mechanisms for long-running processes
* Improved error messages for authentication failures
* Server-side filtering for PDS API calls to reduce data transfer

### Workflow and Deployment
* Streamlined Docker image build process with better caching
* Enhanced GitHub Actions workflows with proper credential management
* Improved container registry integration

## Technical Improvements
* Password generation system for new users with reproducible seeding
* Comprehensive data validation and duplicate handling
* Enhanced country mapping for global fisheries data (13 countries supported)
* Improved spatial data processing with WGS84 coordinate system standardization
* Advanced MongoDB operations with geospatial indexing (2dsphere)

## Geographic Coverage Expansion
* Enhanced support for multi-country deployments
* Improved regional data harmonization
* Currency conversion support for multiple regions (KES, TZS to USD)

# coasts 0.1.0

* Initial release of the coastal fisheries data pipeline for Western Indian Ocean region.

## New Features

### Data Ingestion
* `ingest_pds_trips()` - Automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS) API
* `ingest_pds_tracks()` - Parallel processing of detailed GPS track data with batch processing capabilities
* `get_metadata()` - Retrieval of fishery metadata from Google Sheets

### Data Preprocessing  
* `preprocess_pds_tracks()` - Spatial gridding and summarization of fishing activity patterns
* Multi-scale spatial analysis support (100m, 250m, 500m, 1000m grid cells)
* Parallel processing for efficient handling of large datasets
* `preprocess_track_data()` - Core function for converting GPS tracks to spatial grid summaries

### Data Export and Storage
* `export_geos()` - Comprehensive export of geospatial data and regional metrics to MongoDB
* MongoDB integration with 2dsphere geospatial indexing
* Currency conversion for Kenya (KES to USD) and Zanzibar (TZS to USD) economic indicators
* Support for regional boundary data and time series metrics

### Cloud Storage Integration
* `upload_cloud_file()` and `download_cloud_file()` - Google Cloud Storage integration
* `cloud_object_name()` - Versioned object naming and retrieval
* `upload_parquet_to_cloud()` and `download_parquet_from_cloud()` - Optimized parquet file handling
* Automatic file compression using LZ4 algorithm

### Database Operations
* `mdb_collection_push()` and `mdb_collection_pull()` - MongoDB collection management
* Geospatial indexing support for spatial queries
* Bulk data operations with error handling

### API Integration
* `get_trips()` - PDS API integration for trip data retrieval
* `get_trip_points()` - Detailed GPS point data from PDS API
* Authentication and token management for external APIs

### Automation and Workflow
* GitHub Actions workflow for automated data pipeline execution
* Runs every 2 days with complete data processing pipeline
* Docker containerization for reproducible execution environment
* Configuration management through `conf.yml` files

## Geographic Coverage
* Kenya coastal fisheries data processing
* Zanzibar fisheries data integration
* Regional harmonization and standardization

## Technical Features
* Parallel processing using `future` and `furrr` packages
* Efficient data formats using Apache Arrow/Parquet
* Comprehensive logging with configurable thresholds
* Error handling and recovery mechanisms
* Versioned data management system
