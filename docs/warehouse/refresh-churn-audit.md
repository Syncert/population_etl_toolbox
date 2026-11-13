# Refresh churn audit: scheduled rewrites of unchanged rows

Audited 2026-11-06 at dev `05887e79`. Asked by Nick after the ACS serving
refresh was found deleting and re-inserting every row of a state's year when
under 1% of it changed. This audit checks every other scheduled write to a
gold, reporting, "latest", rollup or reference table for the same pattern.
ACS (`gold_acs.sql`, `refresh_gold_census_serving_layer`) is fixed in its own
thread and is left out here.

**Why it matters.** A deleted or updated row is a dead tuple until VACUUM
reclaims it, and every index on the table gets a new entry per rewritten row.
Unchanged rows rewritten on a schedule therefore cost disk (bloat), WAL,
VACUUM time (single-worker only on OFFICE_LAB: `VACUUM (PARALLEL 0, ANALYZE)`)
and planner accuracy, and buy nothing.

**How rows were counted.** Row counts and write counters were read on
OFFICE_LAB on 2026-11-06 (`/mnt/project-files/refresh_churn/live_churn_counts.sql`,
read-only, ends `ROLLBACK`; output in `/mnt/project-files/refresh_churn_counts.txt`).
The write counters run from the Postgres start on 2026-11-04 12:07 UTC, about
two days. That query also listed every gold, control and silver_ref table by
deletes plus updates, which is how #12 was found to be the largest outside ACS.

## Findings

Gate key: **none** = rewrites whatever it rewrites every run; **source** = one
fingerprint for the whole input; **year** = skips years with no changed
silver row; **run/product** = scoped to one publication; **row** = writes only
rows whose values differ.

| # | Refresh (schedule) | What one run rewrites | Change gate today | Wasted rows per scheduled run (est.) | Fix? |
|---|---|---|---|---|---|
| 1 | BLS serving: `gold_bls.refresh_rpt_bls_observations` + `refresh_mv_bls_latest` (BLS DAG, monthly 1st; daily during LAUS backfill) | Every served row of every calendar year holding one changed silver row (all series, all geographies), then every latest row of every key in those years | year 181,000 report rows per touched year (2018–2025 each 180,973–181,080); a normal run touches the current and prior year → ~317k report rows and all 15,119 latest rows, nearly all unchanged. The 10-29 run rewrote all 37 years (1990–2026), ~6.5M rows | **Yes**: row diff inside both procedures |
| 2 | FRED serving: `gold_fred.refresh_rpt_fred_observations` + `refresh_mv_fred_latest` (FRED DAG, monthly 1st) | Same pattern as BLS for FRED's 24 series | year 51,805 report rows in all; the 11-01 run rewrote 2024–2026, a few thousand rows, and all 24 latest rows | **Yes**: same change as BLS (one shape, one fix) |
| 3 | `gold_bls.dim_bls_series` / `dim_bls_measure` / `dim_bls_survey` (`refresh_bls_elements`, every BLS run) | `ON CONFLICT DO UPDATE SET …, updated_at = NOW()` with no "changed?" test, so every series row is updated | none all 15,119 series rows per BLS run | **Yes**: update only when a column differs |
| 4 | `gold_fred.dim_fred_series` (every FRED run) | Same unconditional upsert | none | ~24 rows | Yes, same one-line guard (trivial) |
| 5 | Calendar rollups `gold_bls.derived_calendar_rollup`, `gold_fbi.derived_calendar_rollup` (`semantics/rollups.py`; every BLS run, every weekly FBI run even when nothing published) | `DELETE` the whole table, re-insert every quarter and year window | none BLS 4,048 rows per BLS run; **FBI 212,400 rows every Monday**, nothing new on most weeks | **Yes**: compute into a temp table, write only the difference |
| 6 | `gold_glossary.geo_search` (`glossary_reconciliation`, **daily** 03:00) | `DELETE FROM geo_search` then re-insert every state, county, metro, place and ZCTA, plus two trigram GIN indexes | none 70,357 rows per day (140,714 deleted and inserted in the two days counted), ~2.1M per month | **Yes**: row diff |
| 7 | Lean groups `gold_elections.lean_place_change` + `lean_group_change` (`elections_lean_groups`, daily 13:00) | Whole rebuild (175,487 + 13,394 rows on 11-04) whenever the input fingerprint moved | source (any of 8 inputs; for BLS, any silver_bls write at all, CPI included) | ~189k rows per rebuild; rebuilt 11-04, unchanged 11-05 and 11-06; after the LAUS backfill lands, ~6–10 rebuilds per month as QCEW, BLS, SAE, govfin, ERS, BEA, FRED and elections publish → ~1–2M rows per month | **Yes**: keep the fingerprint skip, then write only changed rows; `refresh_id` becomes "the rebuild that last changed this row" |
| 8 | FBI `gold_fbi.county_agency_rate_month` + `release_fact_count` (on each FBI publication; Return A loader once per loaded year) | Deletes and re-inserts every rate row of the product, for **every** release it holds, though only the new release changed | run/product Return A: 389,808 rows per product (property and violent), rewritten whole on each new release and once per year during a history load; summarized products 2,412–3,618 rows each, rewritten on each release (10 products published 11-02) | **Yes**: row diff per product |
| 9 | `gold_census_lodes.coverage_gap` (each LODES publication) | Whole table (a few dozen gap rows) | none | 42 rows | Yes, folded into the same row-diff helper (trivial) |
| 10 | `silver_ref.dim_geo_entity` upsert (`silver_ref`, monthly 1st) | Every entity of the newest vintage gets `updated_at = NOW()` even when `first/last_seen_version` did not move | none up to all 194,699 entity rows per month | **Yes**: update only when the vintage bounds move |
| 11 | `silver_ref.tract_decade_crosswalk` (`silver_ref`, monthly) | Re-downloads both relationship files and `DO UPDATE` every part with `ingested_at = NOW()` | none all 237,076 crosswalk rows per month | **Yes**: update only differing parts |
| 12 | `silver_ref.geography_resolution` (every loader's replay) | Upserts each replayed geography with `resolved_at = NOW()` regardless | run (only replays write) **1,954,945 updates in two days** against 3.44M rows (604,003 dead rows, 1.3 GB table), the largest churn outside ACS | **Yes**: update only when the resolution changed |

## Checked and already fine

| Refresh | Why it is fine |
|---|---|
| `gold_glossary.refresh_dim_geo_latest` (daily) | Upsert updates a row only when its tuple `IS DISTINCT FROM` the incoming one; retirements are one-time. |
| `gold_glossary.refresh_tile_boundaries` (daily) | Rewrites a boundary only when its checksum or attributes changed; deletes only boundaries the reference dropped. |
| Glossary harvest (`dim_metric_catalog`, every 10 min + nightly) | Skips a publisher whose content fingerprint is unchanged. When it does harvest, the publication time/watermark columns really changed. |
| `gold_census_lodes.refresh_observation_fact` | Per published run, and with no runs given only runs that have no sums yet. |
| Every other gold schema (BEA, QCEW, SAE, CBP, BPS, govfin, IRS SOI, IRS migration, PEP, NASS, CDC, EIA, EPA, FCC, FEMA, FHFA, HUD, NCES, NOAA, ERS, elections results) | Gold is views over silver; there is no scheduled gold write to waste. Their silver replays were made skip-unchanged in #296/#299 (NASS/FBI same-watermark replay is the open follow-up from the storage sweep). |
| `control.data_quality_result` (daily DQ) | Append-only history, not a rewrite. Its growth per day is in section 7 of the live counts; retention is a separate question. |
| `serving_full_reserve` | Operator-triggered only; rewriting everything is its purpose. |
| `gold_census.age_structure_summary`, `gold_census.dim_acs_variable` / `dim_acs_table` | ACS family, left to the ACS serving-refresh thread. The live counters show `dim_acs_variable` with 54,941 updates in two days against 57,482 rows. |
| `gold_glossary.dim_metric_catalog` | 33,032 updates in two days against 25,675 rows: a publisher whose content changed has all its rows upserted. Moderate; a candidate for the same guard after this round. |

## Silver and raw layers

Nick asked whether the pattern also happens below gold. The counters were read
the same way (`/mnt/project-files/refresh_churn/live_churn_counts_silver_raw.sql`,
read-only, ends `ROLLBACK`; output in
`/mnt/project-files/refresh_churn_counts_silver_raw.txt`), again over the two
days since the 11-04 restart.

**Short answer.** Silver facts do not churn: every loader's fact write is
`INSERT ... ON CONFLICT DO NOTHING` per capture, and replays of unchanged
files were made skip-unchanged in #296/#299. The churn below gold is in three
smaller shapes: reference and metadata upserts that touch every row on every
run, two replays that store a repeat of the last read, and one rolled-back
replay.

| # | Layer: table (writer) | What a run rewrites | Live count (2 days) | Fix in this PR |
|---|---|---|---|---|
| S1 | silver_ref: `geography_resolution` (every loader's replay, ~25 call sites) | Upsert of every replayed geography with `resolved_at = NOW()`, changed or not | **1,954,945 updates**, 567,432 dead rows, 1.3 GB | Update only when the resolved geography, method, status or reason differs |
| S2 | raw: `raw_bls.bls_series`, `bls_datasets` (BLS metadata sync) | Every series row upserted per sync | 136,307 updates against 136,311 rows | Same guard |
| S3 | raw: `raw_census.acs_variables`, `acs_tables`, `acs_datasets` (ACS metadata sync) | Every variable row upserted per sync | 114,695 updates against 114,905 rows; 1,701 table updates | Same guard (in `census_acs/metadata.py`; the ACS serving thread was told) |
| S4 | raw: `raw_fred.fred_series` / `fred_datasets` | Same shape | small | Same guard |
| S5 | silver dims: `silver_irs_soi.dim_measure` / `dim_agi_band`, `silver_census_govfin.dim_measure`, `silver_bea.dim_line`, and the other loaders' `dim_*` upserts | Every dimension row upserted per replay | 1,150 + 650 + 273 + 66 updates against 23 + 13 + 34 + 66 rows | Same guard |
| S6 | silver_ref: `dim_geo_entity`, `tract_decade_crosswalk`, `dim_time` | Gold audit #10/#11; `dim_time` likewise | (monthly; not in the two-day window) | Same guard (`LEAST`/`GREATEST` on vintage bounds) |
| S7 | silver_fred: `observation_revision` (monthly replay re-reads two years) | A repeat of the previous read of an observation stored as a new revision | 8,713 of 62,876 rows repeat (18 MB table) | Store a revision only when its value or status differs from the newest stored one |
| S8 | silver_eia: `price_revision` and `fact_retail_price` (weekly read re-reads eight weeks) | Same, and each stored repeat also became a fact row | 4,176 of 75,864 revisions repeat; 75,864 fact rows for 71,688 distinct weeks | Same skip; the read records `repeated_row_count` and DQ-EIA-002 counts it |
| S9 | silver_census_govfin: `unit_amount` (`replay_cog_units`) | Not a design churn: replays that timed out and rolled back | 3,127,504 inserted, 596,001 live | None here: #304 fixed the timeout; a `VACUUM (PARALLEL 0, ANALYZE)` reclaims the dead rows |
| S10 | silver_fbi: Return A facts, agency links, county crosswalk, source rows (`fbi_ucr/return_a/load.py`, found 2026-11-07) | A changed master file started a new release key and loaded all eleven years again as new facts (a forced reload deleted and re-inserted the year) | ~20M facts per new release key (one release live, loaded 2026-10-31) | **Fixed (ETL-122):** one standing release revised in place; an unchanged file is skipped unread, a changed one staged and diffed so only differing rows are written; identical rows keep their lineage |

Guards compare every value column and leave out timestamps and evidence
pointers (`updated_at`, `resolved_at`, `last_checked_at`,
`evidence_capture_id`, `ingested_at` and similar), which change on every run
and are never read to decide anything. An upsert that already had a `WHERE`
or a `RETURNING`, and the release status tables (`silver_pep.release_load`,
the CDC/FBI/NASS `dim_dataset_release`), are left alone: there the write is
the status change.

### Already covered by the storage cleanups

The largest silver write counters are deletes, and every one is a storage
cleanup or a planned reload, not churn:

| Table family | Deletes in the window | Source |
|---|---|---|
| silver_bea fact + revision | 23,465,373 + 22,199,973 | BEA cleanup (11-06 14:13Z) |
| silver_pep | 6,486,258 + 6,486,274 | PEP cleanup |
| silver_bls_qcew, silver_census_cbp, silver_census_bps | 1,890,870 ×2, 1,761,372 ×2, 219,468 ×2 | Publications cleanup |
| silver_nass, silver_census_sae, silver_fbi | 456,428; 70,318 ×2; 11,084 + 3,216 | NASS/FBI and publications cleanups |
| silver_irs_migration | 883,191 ×2 | #305 superseded-parser reload |

The big silver inserts are loads, not repeats: ACS revisions (10.3M) are the
B25103 run and the #299 coverage, BLS revisions (4.5M) are the LAUS backfill,
IRS SOI and migration are the #303/#305 history loads. The NASS/FBI
same-watermark replay remains the open follow-up from the storage sweep.

**Raw capture is fine.** 301,594 captures hold 262,383 distinct payloads, and
`raw_capture.payload_blob` stores each checksum once, so a repeated answer
costs one capture row, not a second copy of the bytes.
`control.ingestion_request` (393 MB, 301,917 rows) is append-only request
history; retention is a separate question.

After deploy, a plain `VACUUM (PARALLEL 0, ANALYZE)` of
`silver_ref.geography_resolution`, `silver_irs_migration.fact_flow` /
`flow_revision` and `silver_census_govfin.unit_amount` reclaims their dead
rows for reuse. The repeated FRED and EIA rows already stored are harmless
history and are left in place.

## Fix shape

Every fix uses the same pattern, inside one transaction so readers keep
seeing the previous rows until commit:

1. Compute the refresh's intended rows into a temporary table.
2. `DELETE` served rows whose key is no longer intended.
3. `UPDATE` served rows whose key is intended but whose values differ
   (`IS DISTINCT FROM` over the value columns, so NULLs compare correctly).
4. `INSERT` intended rows whose key is not served yet.

An unchanged refresh therefore writes zero rows. Each refresh logs or returns
how many rows it deleted, updated, inserted and left unchanged, so the saving
is visible in the Airflow log.

Families (held for CI and the coordinator's go):

- **A. Time-series serving** (#1–#5): BLS and FRED report/latest procedures,
  their dimension upserts, and the BLS/FBI calendar rollups.
- **B. Derived gold tables** (#6–#9): place search, lean groups, FBI county
  rates and fact counts, LODES coverage gaps.
- **C. Reference upserts** (#10–#12, S1–S9): `dim_geo_entity`, the tract
  crosswalk, the geography resolution ledger, raw metadata and silver
  dimension upserts, and the FRED/EIA repeat skips.

All three families ship in one PR, since they share the pattern and the test
file (DB-069).

Existing bloat is not touched by these PRs. After they deploy, a plain
`VACUUM (PARALLEL 0, ANALYZE)` of the tables above reclaims the space for
reuse; the live counts show which are worth it.
