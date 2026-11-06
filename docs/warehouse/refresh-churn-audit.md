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

Planned PRs (one per family, held for CI and the coordinator's go):

- **A. Time-series serving** (#1–#5): BLS and FRED report/latest procedures,
  their dimension upserts, and the BLS/FBI calendar rollups.
- **B. Derived gold tables** (#6–#9): place search, lean groups, FBI county
  rates and fact counts, LODES coverage gaps.
- **C. Reference upserts** (#10–#12): `dim_geo_entity`, the tract
  crosswalk and the geography resolution ledger.

Existing bloat is not touched by these PRs. After they deploy, a plain
`VACUUM (PARALLEL 0, ANALYZE)` of the tables above reclaims the space for
reuse; the live counts show which are worth it.
