# Stock assessment framework — technical reference

Branch `stock-assessment` of `peskas.coasts` (package `coasts`), state at commit `e0228f2`.
Each function's roxygen help page (`?function_name`) has the full argument and return documentation; this file is the map.

## 1. Entry point

```r
run_stock_assessment(
  n_species     = 5L,                     # priority species per country
  countries     = c("kenya", "mozambique", "zanzibar", "timor"),
  methods       = c("lbi", "lbb"),        # "lbspr" opt-in (fails on current data)
  start_date    = "2024-01-01",
  lbb_engine    = NULL,                   # NULL -> lbb_engine_jags
  robustness    = FALSE,                  # LBB prior grid (~12 JAGS fits / species)
  production    = TRUE,                   # Kolding production layer
  by_gear       = FALSE,                  # per-gear reads + reference gear
  min_gear_fish = 100L,
  group_layer   = TRUE,                   # genus / family taxa
  n_groups      = 15L,
  fao_areas     = NULL,                   # NULL -> kenya/zanzibar/mozambique 51+57, timor 71
  upload        = TRUE,                   # FALSE for local runs
  log_threshold = logger::DEBUG,
  package       = "coasts"
)
```

Recommended production call: `run_stock_assessment(robustness = TRUE, by_gear = TRUE)`, about 40 min for four countries. CI (`.github/workflows/data-pipeline.yaml`, job `stock-assessment`) currently calls `coasts::run_stock_assessment()` with defaults (only the log level set to INFO). Without `robustness = TRUE`, `robust_flag` is `NA` and `reliability` cannot become `borderline` on prior grounds.

**Returns** (invisibly) the long results table. `attr(res, "summary")` holds the one-row-per-stock table.
**Writes**, when `upload = TRUE`, two versioned parquets through `upload_parquet_to_cloud()`. Their names are `stock-assessment-results__<timestamp>_<sha>__.parquet` (long table) and `stock-assessment-summary__<timestamp>_<sha>__.parquet` (summary), set by `stock_assessment$results` and `stock_assessment$summary` in `inst/conf.yml`. The FAO areas for the group priors are `stock_assessment$fao_areas` there too.
**Stops** with a condition of class `fishbase_unavailable`, uploading nothing, if FishBase cannot be reached.

## 2. Pipeline

| # | Stage | Function(s) | File |
|---|---|---|---|
| 1 | Download validated landings per country. Keep rows with `landing_date >= start_date` and `gaul_2_name` present. Harmonise column types (`catch_outcome` → character, `n_catch` → integer) | `download_parquet_from_cloud()` | `model-stock-assessment.R` |
| 2 | Rank taxa by revenue per country, then keep species-level names with ≥ 30 records (`n_species * 5` candidates) | `.species_economic_rank()`, `select_priority_species()`, `.is_species_level()` | `model-stock-assessment.R` |
| 3 | Drop non-teleosts (sharks, rays, chimaeras, all SeaLifeBase taxa) | `.length_method_eligible()`, `.NON_TELEOST_CLASSES` | `model-stock-assessment.R` |
| 4 | Fetch life-history priors: L∞, K, M/K, Lm, a, b. Keep the top `n_species` with an L∞ | `get_life_history_priors()`, `.m_then2015()` | `model-stock-assessment.R` |
| 5 | Fetch trophic levels | `get_trophic_levels()` | `model-production.R` |
| 6 | Species track, one call per species: pooled read, plus per-gear reads when `by_gear` | `assess_species_stock()` | `model-stock-assessment.R` |
| 7 | Group track (genus/family): ranking, screening, regional priors, reads, optional per-gear | `assess_group_layer()` → `get_group_priors()`, `assess_group_stock()` | `model-group-layer.R` |
| 8 | Share of sampled catch weight per taxon | `.catch_share()` | `model-production.R` |
| 9 | Mark the reference gear (when `by_gear`) | `.mark_reference_gear()` | `model-gear.R` |
| 10 | Build the one-row-per-stock summary | `summarise_stock_status()` | `model-summary.R` |
| 11 | Upload both tables | `upload_parquet_to_cloud()` | — |

Every FishBase call in stages 3–5 and 7 goes through `.fb_retry()` (section 4.10).

### 2.1 `assess_species_stock()` — one species, one landings subset

```
.length_coverage()         lengths repeated n_catch times; n_records, n_measured (fish),
                           frac_measured (records), l_max, l_99, n_length_classes
.clean_lengths()           drop lengths > 1.2 * Linf
  -- if no priors or < 20 lengths: return rows with quality = "unknown" + advice --
assess_length_indicators() method "lbi"   (Froese indicators)
assess_lbspr()             method "lbspr" (only if requested)
assess_stock_lbb()         method "lbb"   -> lbb_engine_jags()
.flag_assessment_quality() quality
flag_length_mixture()      misID flag
lbb_robustness()           if robustness = TRUE
assess_production()        method "production"
.ensemble_consensus()      3 legs
.gear_profile(), .quality_reasons(), .stock_advice()
```

The function returns one row per method (`lbi`, `lbb`, `production`). The shared columns are repeated on every row.

### 2.2 `assess_group_stock()` — one coarse taxon

1. Pool the lengths (weighted by `n_catch`, no 1.2×L∞ cull). Return `unknown` if there are fewer than `min_measured` (100) fish.
2. Anchor the prior to the catch: `linf = max(member_median, L99/0.95)`. If it gets floored, Lm is recomputed with `.lm_from_linf()`.
3. Compute the Froese indicators and `.group_status()`, then the quality flag (a `pass` is demoted to `warn` unless confidence is `high`).
4. Run the production layer (Beverton–Holt leg only), then the 2-leg consensus (Froese + production), gear profile, reasons and advice.
5. Size reach is measured against the FishBase member median L∞, not the anchored one.

The function returns one row with `method = "group-lbi"` and `scale = "group"`.

## 3. Functions by file

`E` marks exported functions; all others are internal.

### `R/model-stock-assessment.R`
| Function | Purpose |
|---|---|
| `run_stock_assessment()` E | Orchestrator (section 1) |
| `select_priority_species()` E | Top species per country by revenue; logs the share of revenue held by coarse taxa |
| `.species_economic_rank()` | Revenue per taxon. Basis is `catch_price` if ≥ 50% populated, else trip revenue apportioned by weight, else weight |
| `.is_species_level()` | Is this a true binomial (not `sp`/`spp`)? |
| `.length_method_eligible()` | Bony-fish screen: the share of member species in FishBase classes other than `.NON_TELEOST_CLASSES` must be ≥ 0.5. Sets `attr(, "fishbase_ok")` |
| `.fishbase_reachable()` | Can the taxonomic backbone be read? |
| `.fb_retry()` | Retry wrapper / outage stop (section 4.10) |
| `.length_coverage()` | Length vector and coverage stats (section 2.1) |
| `.clean_lengths()` | Drop lengths > `max_factor` × L∞ (default 1.2) |
| `get_life_history_priors()` E | `rfishbase::estimate()` → `linf` (Loo, else MaxLengthTL), `k`, `mk`, `mk_source`, `lm`, `a`, `b` |
| `.m_then2015()` | M = 4.118 K^0.73 L∞^−0.33 |
| `assess_length_indicators()` E | Froese 2004 indicators |
| `assess_lbspr()` E | LBSPR wrapper (opt-in) |
| `assess_stock_lbb()` E | Calls the LBB engine; returns `bb0`, `bbmsy`, `f_m`, `lc_lopt`, `ci_width` |
| `.flag_assessment_quality()` | Quality score (section 4.8) |
| `assess_species_stock()` E | Species read (section 2.1) |

### `R/model-lbb-engine.R`
| Function | Purpose |
|---|---|
| `lbb_engine_jags()` E | Length-Based Bayesian model (Froese et al. 2018) in JAGS. Defaults: `bin_width = NULL`, `n_iter = 6000`, `n_burn = 3000`, `n_chains = 3`, `linf_sd_frac = 0.10`, `bmsy_b0 = 0.5`, `seed = 1L`. Returns `bb0` (posterior median), `bb0_lo`/`bb0_hi` (95% CrI), `bbmsy`, `fm`, `lc_lopt`, `ci_width`; `NULL` if fewer than 20 lengths, fewer than 6 occupied bins, or the fit fails |
| `.length_resolution()` | Most common gap between distinct recorded lengths (≈ 5 for 5-cm classes) |
| `.lbb_numbers()`, `.lbb_bb0()`, `.LBB_JAGS` | Numbers-at-length, B/B0 from posterior draws, model string |

### `R/model-group-layer.R`
| Function | Purpose |
|---|---|
| `assess_group_layer()` E | Group track. Defaults: `n_groups = 15`, `min_records = 30`, `min_measured = 100`, `fao_areas`, `version`, `production`, `by_gear`, `min_gear_fish` |
| `get_group_priors()` E | Expand the name to member species (`expand_taxonomic_info(strip_parentheticals = TRUE)`), filter by FAO area (`filter_by_fao_area()`), take member-median `linf`/`mk`/`lm`. Also returns `n_species`, `linf_cv`, `source` and `confidence` |
| `assess_group_stock()` E | Group read (section 2.2) |
| `.group_rank()` | Genus / family / other |
| `.group_fallback_species()` | Hard-coded members for families with none under FishBase 26.06/26.07 (Caesionidae, Scaridae) |
| `.group_status()` | `healthy` if p_mature ≥ 0.7 and Lmean/Lopt ≥ 0.9; `overfishing signal` if p_mature < 0.4 or Lmean/Lopt < 0.7; otherwise `borderline` |
| `.lm_from_linf()` | Lm = 10^(0.8979 log10 L∞ − 0.0782) |

### `R/model-production.R`
| Function | Purpose |
|---|---|
| `estimate_exploitation()` E | E = F/Z from Beverton–Holt and LBB, with a band |
| `assess_production()` E | Modified Cadima position: `e_x`, `sy_ratio`, status, band |
| `get_trophic_levels()` E | FishBase `FoodTroph`, as the member median for coarse taxa |
| `scale_sustainable_yield()` E | Converts `catch_share` × `sy_ratio` to kg, given country totals (`country`, `total_catch_kg`) |
| `.bh_zk()` | Z/K = (L∞ − Lmean)/(Lmean − Lc). Lc is the lower edge of the modal class |
| `.e_from_zk()` | E = 1 − (M/K)/(Z/K), floored at 0 |
| `.harvest_fraction()` | x = 0.5 if trophic level ≥ 4, otherwise 0.25 (also 0.25 when TL is unknown) |
| `.catch_share()` | Share of sampled `catch_kg` per country × taxon |

### `R/model-robustness.R`
| Function | Purpose |
|---|---|
| `lbb_robustness()` E | Prior grid (section 4.5) |
| `.stock_class()` | B/B0 < 0.3 `depleted`, < 0.5 `moderate`, otherwise `healthy` |
| `.ensemble_consensus()` | Up to 3 legs (section 4.6) |

### `R/model-misid-flag.R`
| Function | Purpose |
|---|---|
| `flag_length_mixture()` E | Misidentification / two-species flag (species only, n ≥ 200) |
| `.fit_2gauss()` | Two-component Gaussian EM in base R |

### `R/model-gear.R`
| Function | Purpose |
|---|---|
| `.gear_profile()` | `main_gear`, `main_gear_share`, `selective_gear_share`, `selectivity_flag` |
| `.gears_with_fish()` | Gears with ≥ `min_fish` measured fish; returned only if ≥ 2 gears qualify |
| `.mark_reference_gear()` | Adds `reference_gear` (section 4.9) |
| `.n_fish()`, `.measured_rows()` | `n_catch` weights (NA or < 1 counts as 1) |
| `.SELECTIVE_GEAR_PATTERN` | `"trap\|gill\|tangle"` (matched case-insensitively) |

### `R/model-advice.R`
| Function | Purpose |
|---|---|
| `.quality_reasons()` | `"; "`-separated reasons (section 4.8) |
| `.stock_advice()` | One advice sentence (section 4.11) |
| `.col_or()` | Reads a column if present, without the tibble warning |

### `R/model-summary.R`
| Function | Purpose |
|---|---|
| `summarise_stock_status()` E | Long table → one row per stock (section 5.2) |
| `.kobe_status()` | Status from B/B0 and F/M (section 4.12) |
| `.reliability()` | Traffic light (section 4.12) |

## 4. Rules and thresholds

### 4.1 Priors
- **L∞:** FishBase `Loo`, else `MaxLengthTL`. Species without one are skipped.
- **M/K:** FishBase M/K when FishBase has M (`mk_source = "fishbase_m"`). Otherwise Then et al. 2015 (`"then2015"`) when K and L∞ exist. Otherwise 1.5 (`"default"`). FishBase currently returns no M for these species, so most priors are `then2015`.
- **Lm:** FishBase value, else Froese–Binohlan from L∞.
- **Groups:** member medians across regional members (FAO 51+57 for WIO, 71 for Timor).
- **Group confidence:**
  - genus with `linf_cv` < 0.35 (or a single member) → `high`;
  - other genera → `moderate`;
  - family with CV < 0.35 → `moderate`;
  - everything else → `low`;
  - `catch_below_prior` (prior not floored and p_mature < 0.10) → forced to `low`.

### 4.2 Lengths
- Each row counts `n_catch` times. `n_measured` counts fish; `frac_measured` is the share of records that have a length.
- Bin width is `max(1, .length_resolution())`. When it is above 1 (classed data), bins are centred on the recorded class values.
- `n_length_classes` < 6 lowers the quality score and produces the "record to nearest cm" advice.

### 4.3 LBB (`lbb_engine_jags()`)
- The L∞ prior is centred on `max(prior L∞, largest occupied bin + 1)` with SD 10%. The M/K prior has SD 0.3. Lc is bounded between the smallest bin and mode + 0.5·(L∞ − mode).
- Each chain gets `.RNG.seed = seed + chain` (Mersenne-Twister).

### 4.4 Production layer
- **Beverton–Holt:** Z/K from the mean length of fish ≥ Lc, where Lc is the modal class's lower limit.
- **E:** `E_bh = 1 − mk/zk`; `E_lbb = fm/(1 + fm)`; `e` = median of the available estimates.
- **Band:** BH with M/K × 0.75 and × 1.25; BH with the catch-implied L∞ (L99/0.95) when it differs from the prior by > 5%; the LBB `fm_lo`/`fm_hi` converted to E. `e_lo` and `e_hi` are the minimum and maximum of all these.
- **Ratios:** `e_x = e / x`; `sy_ratio = x / e`.
- **`production_status`:** `e_x` ≤ 1 `within sustainable share`, ≤ 1.5 `above sustainable share`, otherwise `well above sustainable share`.
- **`production_band`:** `straddles` if `e_lo/x` and `e_hi/x` fall on different sides of 1, otherwise `clear`.

### 4.5 Robustness grid (`lbb_robustness()`)
- **L∞ axis:** prior × (0.85, 1, 1.15), plus the catch-implied L∞ (L99/0.95) when it falls outside that range.
- **M/K axis:** prior × (0.75, 1, 1.25).
- That gives 9–12 JAGS fits per species.
- **`robust_flag`:** `robust` if `.stock_class()` and the `fm > 1` verdict are the same at every grid point, otherwise `sensitive`.
- **Other outputs:** `bb0_lo`/`bb0_hi`/`fm_lo`/`fm_hi` (min/max across the grid), `linf_catch`, and `linf_ratio = prior L∞ / linf_catch`.
- Runs on the pooled read only.

### 4.6 Consensus (`.ensemble_consensus()`)
- **Concern rules per leg:**
  - LBB: B/B0 < 0.5 or F/M > 1;
  - Froese: p_mature < 0.5 or Lmean/Lopt < 0.9;
  - production: E/x > 1.
- **Labels:**
  - 0 known legs → `unknown`;
  - 1 leg → `concern (1 method)` / `healthy (1 method)`;
  - 2 or more legs → `concern (agree)` / `healthy (agree)` / `mixed (methods disagree)`.
- Also returns `n_concern`, `n_methods` and `methods_agree`.

### 4.7 Misidentification (`flag_length_mixture()`)
- Requires n ≥ 200, species only.
- `misid_flag = TRUE` when either:
  - two modes with standardised separation ≥ 1.5, minor weight ≥ 0.15 and the upper mode above L∞; or
  - more than 1% of lengths above 1.1 × L∞.

### 4.8 Quality flag and reasons
- **Score:**
  - +1 each for n ≥ 200, n ≥ 100, `frac_measured` ≥ 0.3 and `frac_measured` ≥ 0.1;
  - +1 for `l_99 / L∞` ≥ 0.8;
  - −1 for `ci_width` > 0.5;
  - −1 for < 6 length classes.
- **Flag:** `pass` if score ≥ 4, `warn` if ≥ 2, otherwise `fail`. `unknown` if there are no lengths.
- **`quality_reasons`:** built from the same signals (classes < 6, n < 100, `frac_measured` < 0.1, reach < 0.8, CI > 0.5).

### 4.9 Gear
- **`selectivity_flag`:** share of measured fish from gears matching `trap|gill|tangle` > 0.7.
- **Per-gear reads:** only when ≥ 2 gears each have ≥ `min_gear_fish` fish.
- **`reference_gear`:** the highest `size_reach` among non-unknown per-gear reads, for species and `high`-confidence genera only. Mixed families get none, because gear differences there reflect which species each gear catches. Pooled rows have `reference_gear = NA`.

### 4.10 FishBase guard (`.fb_retry()`)
- Up to 3 attempts, 10 s apart.
- If a result is incomplete (empty, or every L∞ NA) and FishBase is unreachable, the run aborts with `fishbase_unavailable`.
- An incomplete result is accepted only when it repeats while the backbone is reachable (a genuine data gap).
- The group-layer `tryCatch` re-throws this condition and swallows all others, so a group error does not stop the run.

### 4.11 Advice (`.stock_advice()`) — first match wins
1. n < `min_n` (20 for species, 100 for groups) → "Not assessable yet: measure at least 150 fish".
2. `misid_flag` → "Check species identification".
3. < 6 length classes → "Record lengths to the nearest cm".
4. n < 100 → "Measure more fish: aim for 150+".
5. Reach < 0.8 → names the gear if `selectivity_flag`, otherwise "Large fish are missing".
6. `sensitive`, `straddles` or `mixed` → "Borderline status".
7. Low-confidence group → "Record the catch at species level".
8. Otherwise → "Reliable read: keep current sampling".

### 4.12 Summary status and reliability
- **`status`:**
  - species with an LBB fit: `.kobe_status()` → `healthy` (B/B0 ≥ 0.5 and F/M ≤ 1), `overfishing`, `overfished` or `overfished + overfishing`;
  - groups, and species where LBB could not fit (coarse classes): the Froese word from `.group_status()`.
- **`status_basis`:** `"LBB"` or `"Froese"`.
- **`headline_gear`:** the reference gear if one exists, else `"all"`. The robustness fields always come from the pooled read.
- **`reliability`** (first match wins):
  1. `not assessable`: quality unknown;
  2. `borderline`: `sensitive`, or band `straddles`, or `mixed`;
  3. `indicative`: quality `fail`, n < 100, `selectivity_flag`, < 6 length classes, or a `low`-confidence group;
  4. otherwise `reliable`.

## 5. Outputs

### 5.1 Long table (`res`, parquet `stock-assessment`)
- One row per `country × scale × scientific_name × gear × method`.
- `method` is `lbi`/`lbb`/`production` (plus `lbspr` if requested) for species, and `group-lbi` for groups.
- Shared columns repeat across a species' method rows; method-specific columns are `NA` on the other rows.

| Column group | Columns |
|---|---|
| Keys | `country`, `scale`, `scientific_name`, `gear` (`"all"` = pooled), `method`, `catch_taxon`, `rank`, `rank_basis` |
| Coverage / quality | `n_measured`, `frac_measured`, `n_length_classes`, `size_reach`, `quality`, `quality_reasons`, `advice` |
| Gear | `main_gear`, `main_gear_share`, `selective_gear_share`, `selectivity_flag`, `reference_gear` |
| Consensus | `consensus`, `methods_agree`, `n_concern`, `n_methods` |
| LBB (`method == "lbb"`) | `bb0`, `bbmsy`, `f_m`, `lc_lopt`, `ci_width` |
| Robustness (pooled species) | `bb0_lo`, `bb0_hi`, `fm_lo`, `fm_hi`, `robust_flag`, `linf_catch`, `linf_ratio` |
| Froese (`lbi` / `group-lbi`) | `lopt`, `lm`, `p_mature`, `p_opt`, `p_mega`, `lmean_lopt`, `n` |
| Production (`production` / `group-lbi`) | `zk_bh`, `lc_bh`, `e_bh`, `e_lbb`, `e`, `e_lo`, `e_hi`, `e_methods`, `trophic_level`, `x`, `x_basis`, `e_x`, `sy_ratio`, `production_status`, `production_band` |
| MisID (species) | `misid_flag`, `n_modes`, `separation`, `upper_linf`, `frac_oversize`, `mixture_note` |
| Groups | `n_species`, `confidence`, `group_status`, `prior_note` |
| Catch | `catch_kg_sampled`, `catch_share` |

### 5.2 Summary table (`attr(res, "summary")`, parquet `stock-assessment-summary`)

One row per `country × scale × scientific_name`. Sorted by country, scale, then descending `catch_share`.

| Column | Type | Meaning |
|---|---|---|
| `country`, `scale`, `scientific_name`, `catch_taxon` | chr | Keys (`scale`: `species` / `group`) |
| `headline_gear` | chr | Gear of the headline read; `all` = pooled |
| `status` | chr | Section 4.12 |
| `status_basis` | chr | `LBB` / `Froese` |
| `reliability` | chr | `reliable` / `indicative` / `borderline` / `not assessable` |
| `consensus`, `n_concern`, `n_methods` | chr, int, int | Section 4.6 |
| `bb0`, `bb0_lo`, `bb0_hi` | dbl | LBB B/B0 and robustness band (species) |
| `f_m`, `fm_lo`, `fm_hi` | dbl | LBB F/M and robustness band (species) |
| `e`, `e_lo`, `e_hi` | dbl | Exploitation rate F/Z and band |
| `x` | dbl | Sustainable share of production (0.25 / 0.5) |
| `e_x` | dbl | E/x; > 1 = more than the sustainable share is being taken |
| `sy_ratio` | dbl | Sustainable catch / current catch (x/E) |
| `production_status`, `production_band` | chr | Section 4.4 |
| `p_mature`, `lmean_lopt` | dbl | Froese indicators |
| `n_measured`, `n_length_classes` | int | Measured fish, distinct recorded lengths |
| `size_reach` | dbl | 99th-percentile length / L∞ |
| `catch_share` | dbl | Share of sampled catch weight in the country |
| `trophic_level` | dbl | FishBase FoodTroph (member median for groups) |
| `confidence` | chr | Group prior confidence (`NA` for species) |
| `quality`, `quality_reasons` | chr | Section 4.8 |
| `main_gear`, `main_gear_share`, `selectivity_flag` | chr, dbl, lgl | Section 4.9 |
| `robust_flag`, `linf_ratio` | chr, dbl | Section 4.5 |
| `misid_flag` | lgl | Section 4.7 |
| `advice` | chr | Section 4.11 |

To get catch volumes, join country totals (e.g. summed `estimated_total_catch_kg` from `calculate_district_totals()`) with `scale_sustainable_yield()`.

## 6. Running and testing

```r
devtools::load_all()
res <- run_stock_assessment(robustness = TRUE, by_gear = TRUE, upload = FALSE)
s   <- attr(res, "summary")        # or summarise_stock_status(res)
devtools::test(filter = "model-")  # 85 tests: production, gear, summary
```

| Requirement | Detail |
|---|---|
| R packages | `rjags`, with JAGS installed (already in `Dockerfile.prod`) |
| Network | GCS credentials (`.env`), and FishBase through `rfishbase` (remote parquet, read live; a short outage stops the run) |
| FishBase release | `conf$metadata$fishbase$db_version` (currently `latest`) |

The tests mock reachability for `.fb_retry()` and use a fake LBB engine for `lbb_robustness()`, so they need neither JAGS nor network.

## 7. Known limits (v1)

- **LBB:**
  - cannot fit on samples with fewer than 6 occupied length bins (5-cm classes for small species); these fall back to Froese + production;
  - assumes equilibrium and asymptotic selectivity;
  - trap / gill-net samples bias Z, F/M and E upward, which is why `selectivity_flag` exists.
- **E:** ~1 length per landing steepens the apparent mortality, so E reads high.
- **x:** 0.25 / 0.5 are heuristics (Kolding), not estimates.
- **Production layer:** needs approximate stationarity (P/B ≈ Z).
- **Robustness grid:** varies priors only; it has not been validated against simulated data with known answers.
- **Planned for v2:** simulation testing, LIME, local growth / hierarchical priors, local FishBase cache.
