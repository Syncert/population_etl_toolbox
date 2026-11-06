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

**How rows were estimated.** From the code and known warehouse sizes. Each
estimate marked *(live)* is confirmed or corrected by
`/mnt/project-files/refresh_churn/live_churn_counts.sql` (read-only, ends
`ROLLBACK`), which also lists every gold/control/silver_ref table by deletes
plus updates since the statistics reset, so anything this reading missed
shows up there.

## Findings

Gate key: **none** = rewrites whatever it rewrites every run; **source** = one
fingerprint for the whole input; **year** = skips years with no changed
silver row; **run/product** = scoped to one publication; **row** = writes only
rows whose values differ.

| # | Refresh (schedule) | What one run rewrites | Change gate today | Wasted rows per scheduled run (est.) | Fix? |
|---|---|---|---|---|---|
| 1 | BLS serving: `gold_bls.refresh_rpt_bls_observations` + `refresh_mv_bls_latest` (BLS DAG, monthly 1st; daily during LAUS backfill) | Every served row of every calendar year holding one changed silver row (all series, all geographies), then every latest row of every key in those years | year | ~150k–200k rpt rows per touched year; a normal run touches 2 years (new month + revisions) → ~300k+ rpt rows, ~15k–30k latest rows; >90% unchanged *(live)* | **Yes**: row diff inside both procedures |
| 2 | FRED serving: `gold_fred.refresh_rpt_fred_observations` + `refresh_mv_fred_latest` (FRED DAG, monthly 1st) | Same pattern as BLS for FRED's 24 series | year | small: a year of 24 series, ~1k–6k rows *(live)* | **Yes**: same change as BLS (one shape, one fix) |
| 3 | `gold_bls.dim_bls_series` / `dim_bls_measure` / `dim_bls_survey` (`refresh_bls_elements`, every BLS run) | `ON CONFLICT DO UPDATE SET …, updated_at = NOW()` with no "changed?" test, so every series row is updated | none | every BLS series row, ~tens of thousands per run *(live)* | **Yes**: update only when a column differs |
| 4 | `gold_fred.dim_fred_series` (every FRED run) | Same unconditional upsert | none | ~24 rows | Yes, same one-line guard (trivial) |
| 5 | Calendar rollups `gold_bls.derived_calendar_rollup`, `gold_fbi.derived_calendar_rollup` (`semantics/rollups.py`; every BLS run, every weekly FBI run even when nothing published) | `DELETE` the whole table, re-insert every quarter and year window | none | BLS ~10k rows per run (22 national series back to 1913/1939); FBI depends on subjects, possibly much larger *(live)* | **Yes**: compute into a temp table, write only the difference |
| 6 | `gold_glossary.geo_search` (`glossary_reconciliation`, **daily** 03:00) | `DELETE FROM geo_search` then re-insert every state, county, metro, place and ZCTA, plus two trigram GIN indexes | none | ~70k rows per day (~2.1M per month), almost all unchanged *(live)* | **Yes**: row diff |
| 7 | Lean groups `gold_elections.lean_place_change` + `lean_group_change` (`elections_lean_groups`, daily 13:00) | Whole rebuild (175,487 + 13,394 rows on 11-04) whenever the input fingerprint moved | source (any of 8 inputs; for BLS, any silver_bls write at all, CPI included) | ~189k rows per rebuild; daily during the LAUS backfill, then ~6–10 rebuilds per month as QCEW, BLS, SAE, govfin, ERS, BEA, FRED and elections publish → ~1–2M rows per month *(live)* | **Yes**: keep the fingerprint skip, then write only changed rows; `refresh_id` becomes "the rebuild that last changed this row" |
| 8 | FBI `gold_fbi.county_agency_rate_month` + `release_fact_count` (on each FBI publication; Return A loader once per loaded year) | Deletes and re-inserts every rate row of the product, for **every** release it holds, though only the new release changed | run/product | every older release's rows on each new release; a Return A history load rewrites the product once per year loaded *(live)* | **Yes**: row diff per product |
| 9 | `gold_census_lodes.coverage_gap` (each LODES publication) | Whole table (a few dozen gap rows) | none | tens of rows | Yes, folded into the same row-diff helper (trivial) |
| 10 | `silver_ref.dim_geo_entity` upsert (`silver_ref`, monthly 1st) | Every entity of the newest vintage gets `updated_at = NOW()` even when `first/last_seen_version` did not move | none | every entity row the newest vintage lists (states → tracts, ~150k–250k) per month *(live)* | **Yes**: update only when the vintage bounds move |
| 11 | `silver_ref.tract_decade_crosswalk` (`silver_ref`, monthly) | Re-downloads both relationship files and `DO UPDATE` every part with `ingested_at = NOW()` | none | every crosswalk part (~2 decades of tract pairs) per month *(live)* | **Yes**: update only differing parts |
| 12 | `silver_ref.geography_resolution` (every loader's replay) | Upserts each replayed geography with `resolved_at = NOW()` regardless | run (only replays write) | one row per geography per replayed run; small after #299 stopped unchanged replays | Later: needs a check of who reads `resolved_at` first; listed, not fixed in this round |

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
- **C. Reference upserts** (#10–#11): `dim_geo_entity` and the tract
  crosswalk.

Existing bloat is not touched by these PRs. After they deploy, a plain
`VACUUM (PARALLEL 0, ANALYZE)` of the tables above reclaims the space for
reuse; the live counts show which are worth it.
