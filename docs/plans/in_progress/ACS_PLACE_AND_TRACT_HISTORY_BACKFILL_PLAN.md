---
id: acs-place-tract-history-backfill
depends_on:
  - acs-place-grain
  - sub-county-geography
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/census -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database -m "integration and database" -k "acs or geograph" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# ACS place and tract history: every published year

## Status

In progress since 2026-10-12. Decisions Nick made on 2026-10-12:

- **Docker moved to E:** (Crucial T705, PCIe 5 NVMe, 1.17 TB free before the
  move). Docker's 665 GB image now lives at `E:\DockerDesktop\wsl`, reached
  through the `%LOCALAPPDATA%\Docker\wsl` junction; the G: copy is kept as a
  backup until E: is verified under load.
- **Scope ("the recommended approach").** Tracts for every 5-year year,
  2009–2023, on the five `tract_tables`. Places for every year, but only on
  those same five tables (B01003, B19013, B17001, B25064, B25077); 2024 keeps
  the full curated place table list it already has. Measured on 2026-10-12, a
  full-list place year costs about 136 GB (2024: +60 GB serving table, about
  46 GB silver facts, about 40 GB revision rows), so every year at the full
  list (about 2 TB) does not fit; the five-table scope is about 300 GB in all.

This plan builds on work that is not on `main` yet:

- `acs-place-grain` and `sub-county-geography` (both in `needs_review/` on
  `dev`, via stacked PR #121) added the place and tract grains, scoped to the
  newest year by `place_recent_years = 1` and `tract_recent_years = 1` in
  `census_acs/config.py`.
- Branch `claude/project-thread-ypfqia` (on top of `dev`, not yet merged into
  it) made the newest year loadable at all: commit `4d1b672` reads a year
  above 20M revision rows one (dataset, geo level, state) slice at a time and
  checkpoints slices in `silver_census.transform_checkpoint`; `44cc211` makes
  `gold_census.refresh_mv_acs_latest` use hash joins above 100,000 keys;
  `77690b9` raises the ACS serving chunk timeout from 90/120 to 600 minutes.
  The diagnosis is in
  [`docs/incidents/2026-10-09_acs_reload_failures.md`](../../incidents/2026-10-09_acs_reload_failures.md).

Do not start until that branch is merged and the 2024 publish has finished
on the internal stack, because Deliverable 1 measures it.

## Why

A city or tract page with one year of ACS cannot show change, and the
compare and time-window views (ADR-0007) have nothing to work with below
county. Raising the two `*_recent_years` knobs to cover every year is a
one-line change, but on today's pipeline it would ask the local warehouse to
absorb roughly fifteen copies of the year that already froze Docker once and
needed a 10-hour publish. The work is in making that load safe, resumable,
and honest about geography that changes underneath it.

## What exists

- **Years published by the Bureau.** 5-year: 2009 (2005–2009) through 2024
  (2020–2024), 16 vintages; the Data API serves all of them
  ([ACS 5-year API, 2009–2024](https://www.census.gov/data/developers/data-sets/acs-5year.html)).
  1-year: 2005 onward, places of 65,000 or more only, no standard 2020
  release. Only the 5-year estimates publish tracts.
- **Volume of the year that is loaded.** 2024 holds 189.8M silver revision
  rows: place 162.1M, tract 10.8M, everything else about 17M. Gold holds
  about 86M facts for 2024 against 4–8M in an earlier year. Places are the
  expensive grain (the whole curated table list); tracts are cheap because
  `tract_tables` is five tables.
- **Geography reference.** `sync_geography_history` backfills historical
  vintages for counties only; places, tracts and ZCTAs are published for the
  newest vintage only (`sync_sub_county_geography`). An ACS row whose place
  or tract code the reference does not hold is ledgered `unmapped` and not
  served, which is correct behaviour but means a naive backfill would serve
  very little before 2020.
- **Retry.** Slice checkpoints are keyed by the Airflow `run_id`, so a retry
  of the same run resumes at its slice, but a new run re-checks every year
  (about 1.7 hours for 2005–2023 before this backfill; far longer after it).
- **Serving.** One ACS serving chunk is one calendar year. While a chunk
  runs, its year partition is held under `ACCESS EXCLUSIVE`, so API reads of
  that year wait.

## Expected size

Upper bound, before Deliverable 1 replaces it with measured numbers:

| Grain | Years to add | Per year | Added in total |
| --- | --- | --- | --- |
| Tract (5-year, 5 tables) | 2009–2023, 15 | ~10.8M revision rows | ~160M |
| Place (5-year, all curated tables) | 2009–2023, 15 | ~160M revision rows | ~2.4B |
| Place (1-year, 65k+) | 2005–2023 less 2020, 18 | well under 1M | small |

Older vintages publish fewer of the curated tables, so the place figure is a
ceiling. Even so the place backfill is roughly fifteen times the warehouse's
current ACS volume, and gold grows by about 1.2B facts.

## Geography across decades

Per the Bureau's
[geography boundaries by year](https://www.census.gov/programs-surveys/acs/geography-acs/geography-boundaries-by-year.html)
(a 5-year estimate takes the vintage of its last year):

- **Tracts** are Census 2000 tracts in the 2009 release, 2010 Census tracts
  for 2010–2019, and 2020 Census tracts from 2020 on. The same tract code in
  two decades can be a different area (tracts are split, merged and
  renumbered at each census).
- **Connecticut** replaced its 8 counties with 9 planning regions from the
  2022 ACS, so its tracts carry different county codes in 2020–2021 and
  2022–2024 within the same 2020 tract decade.
- **Places** are legal areas updated every year (incorporations,
  disincorporations, annexations); census-designated places are redrawn each
  decade. A place FIPS code is a stable identity, but its boundary is not.

So a crosswalk is needed, but only for analysis across a decade boundary.
Each published value is served on the tract or place as it was defined in
that release; the crosswalk is a separate, labelled, derived product.

## Deliverables

1. **Measure before loading.** A read-only operator script reports, per ACS
   year and grain, silver revision rows, gold facts, and on-disk size of the
   year's partitions and indexes, plus free disk on the warehouse volume.
   Its output for 2024 on the internal stack is recorded here and replaces
   the upper bounds above; the per-year estimate for older years is scaled
   by the number of variables each vintage publishes, from the cached
   variable metadata.
2. **Historical place and tract reference.** Extend the geography history
   so every ACS year resolves:
   - tract identities and boundaries for the 2000, 2010 and 2020 decades
     (plus the Connecticut planning-region recoding), published as
     decade-scoped versions so a period is never painted on another decade's
     tract;
   - place identities for every vintage the backfill requests, so a place
     that later disincorporated still resolves by state and place FIPS;
   - checksummed raw capture of every asset, the same as today's vintages.
   The asset list per vintage is verified against the Bureau's TIGER and
   Gazetteer directories before code is written (Gazetteer place and tract
   files are not published for every year; older years may need TIGER/Line
   or cartographic boundary files).
3. **Explicit year scope and one year per run.** Replace "the N newest
   years" with a declared year scope per grain (default unchanged: the
   newest year), and add a DAG run parameter that restricts a run to one
   ACS year and grain. The backfill is then a sequence of small runs rather
   than one run that can lose days of work.
4. **Resume across runs.** A transform slice that already succeeded against
   unchanged raw content is skipped by any later run, not only a retry of
   the same `run_id`: key the checkpoint by (dataset, year, geo level,
   state, raw content checksum). A changed provider response still produces
   a new revision, as today.
5. **Serving that does not stall.** Split the ACS serving chunk from one
   calendar year to one (year, grain) pair, so a place year publishes on its
   own and county/state/nation reads of that year are not locked behind it,
   and set a measured timeout per grain rather than one 600-minute limit
   for every chunk. Backfill runs refresh only the chunks they touched,
   never a full rebuild. `refresh_mv_acs_latest` is re-measured with every
   place year present (latest-value lookups now visit many more
   partitions); its plan stays on hash joins with a database test proving
   it.
6. **Guard rails for the local stack.** Before each year the run checks
   free disk against Deliverable 1's projection and refuses with a clear
   message instead of filling the volume; at most one ACS backfill run is
   active at a time (pool or `max_active_runs`); the transform keeps its
   bounded memory read for every year above the slice threshold.
7. **Crosswalk, derived and labelled.** Load the Bureau's tract
   relationship files (2000↔2010 and 2010↔2020) and the Connecticut
   county-to-planning-region tract mapping into
   `bridge_geo_relationship_version` as decade-succession relationships with
   their published area and population weights. A "comparable over time"
   series for a tract is a derived product built on that bridge, marked as
   derived in the API and on the page, and never written back over a
   published value. A tract that did not change keeps its own published
   series with no crosswalk at all.
8. **Operator runbook.** `docs/reference/BETA_RESET_REINGESTION.md` gains
   the backfill order, commands, expected duration and size per year, how to
   resume, and how to stop safely; the incident write-up links to it.

## Load order

Tracts first, every year, newest to oldest (small, and they prove the
decade-scoped reference). Then places one year at a time, newest to oldest,
so the years a reader is most likely to look at land first and a stop at any
point leaves a contiguous recent history. Each year is ingest, silver
transform, then that year's place chunk publish, and is confirmed in the
data-quality check before the next year starts.

## Acceptance criteria

- The 2024 measurements and per-year projections are recorded in this plan.
- A 2009 tract fixture resolves to a 2000-decade tract, a 2015 fixture to a
  2010-decade tract and a 2023 Connecticut fixture to a planning-region
  tract; a code that exists only in another decade is ledgered `unmapped`,
  not matched by code across decades.
- A place fixture for a place that disincorporated before the newest vintage
  resolves by FIPS and is served for the years it existed.
- A run restricted to one year and grain requests and transforms only that
  scope; a unit test proves no other year's request is formed.
- A second run over unchanged raw content skips every finished slice
  (database test); a changed response produces a new revision.
- A place-year serving chunk completes without holding the county/state/
  nation rows of that year; per-grain timeouts are declared and tested.
- A run refuses to start a year when projected size exceeds free disk.
- The crosswalk loads both relationship files with their weights; a derived
  comparable series is labelled derived in the API response, and the
  published series for the same tract is unchanged.
- The whole backfill has run on the internal stack, every year 2009–2024 is
  published at place and tract grain, and the nightly data-quality check is
  green; durations and sizes per year are recorded here.
- Unit, database integration, DAG and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Whether the internal stack has the disk for the full place history. If
  Deliverable 1 shows it does not, the decision goes to Nick: trim the place
  table list for older years, keep fewer place years locally and the full
  history on the remote warehouse
  (`remote-warehouse-catches-up`), or add disk.
- Whether block groups ever join; not in this plan.
- Whether the 1-year place series is worth serving before 2009, when it is
  the only place data and covers large places only. Default: load it, since
  it is small and published.

## Progress (2026-10-26, branch `feat/acs-backfill-measure-and-scope`)

Deliverables 1, 3, 4 and 6 are implemented; 2, 5, 7 and 8 remain (v1.0
build-out items W-2, W-5, W-10).

- **D1 measurement:** `python -m data_ingestion_toolbox.census_acs.measure`
  (ETL-085), run in the Airflow scheduler container on the internal stack:
  Docker's data disk 6,047 GiB with 5,016 GiB free; database 704 GiB; ACS
  silver relations 293 GiB. Measured 2024 (silver share plus the 2024 gold
  partition's share): acs5 place 137.7 GiB for 3,844 curated variables
  (38.5 MB per variable), acs5 tract 9.3 GiB for the five tables' 63
  variables (159 MB per variable), acs1 place 2.8 GiB (0.61 MB per variable).
  These are `AcsConfig.backfill_bytes_per_variable`.
- **What the warehouse holds** (`control.acs_ingestion_slices`, 2026-10-26):
  acs5 place slices for every year 2009–2024 (2024 on the whole curated
  list, the rest on the five history tables); acs1 place 2009–2024 less 2020
  on the history tables; acs5 tract 2010–2024 on the five tract tables; no
  2009 tracts (Census 2000 tracts, not in the reference).
- **Projection, full city history** (acs5 place 2009–2023 on the whole list,
  less the five tables already loaded, plus acs1 places 2005–2023):
  1,366 GiB. Per year it ranges from 41 GiB (2009, 1,276 variables) to
  133 GiB (2020–2023, about 3,840), because older vintages publish fewer of
  the curated variables.
- **Projection, wider tract tables** (W-4 candidate list of 17 tables, 558
  variables a year from 2017, 336 in 2009–2010): about 7,680 variable-years
  for 2010–2024 at 159 MB is about 1,140 GiB.
- Both together leave about 2,500 GiB, 41% of the 6,047 GiB volume, free:
  above the 20% reserve.
- **D3 declared scope:** `place_year_scope`, `tract_year_scope` and
  `place_full_table_year_scope` ("newest", "newest:N", "YYYY-YYYY", "YYYY",
  "none") replace the three "N newest years" knobs; defaults unchanged. The
  one-year-and-grain run parameter (`acs_backfill` conf, ETL-083) was already
  on `main`.
- **D4 resume across runs:** `silver_census.transform_slice_state` keys each
  finished slice by a content fingerprint (revision count and newest
  revision, variable metadata, geography reference, `TRANSFORM_VERSION`); a
  later run under any key skips an unchanged slice (ETL-087).
- **D6 guard rails:** a backfill run projects its planned slices and refuses
  before any request if the reserve (20%) would not stay free at
  `backfill_disk_path` (ETL-086); `acs_ingest` already had
  `max_active_runs=1`; the bounded slice read is unchanged.

Evidence: `pytest tests/unit/census tests/unit/shared/test_catalog_evidence.py
tests/unit/tooling` 193 passed; `pytest tests/dags/test_task_callables.py -k
acs` 11 passed; ACS database tier against the test PostGIS (`-k "acs or
census_ac"`) 26 passed; schema snapshot regenerated; `ruff check .` and
`ruff format --check .` clean.

## Progress (2026-10-26, branch `feat/acs-decade-geography-and-serving`)

- **D2, tracts:** Census 2000 tracts (66,304, the 2009 ACS's only tract
  geography) are published as reference vintage 2000 from the Bureau's
  2000-to-2010 tract relationship file, which `silver_ref` now syncs with the
  rest of the tract history from 2009 (ETL-089). Tracts of 2010–2024 already
  resolved from each vintage's Gazetteer (ETL-084), including Connecticut's
  2022 planning-region recoding: `silver_ref.geography_resolution` shows
  85,395 tracts resolved for 2021 and 85,396 for 2022, none unmapped.
- **D2, places:** places resolve for every 5-year year 2010–2024 and every
  1-year year (2010–2024) with at most 21 codes unmapped in a year (2024).
  2009 has 24,901 places resolved and 401 unmapped: places that ceased
  before 2010, for which no reference before 2010 is captured yet. They stay
  ledgered `unmapped`, never matched to a later place. A Census 2000 place
  file (`places2k.zip`, fixed width) or the 2009 TIGER place files would
  close it; that is left as an open item rather than guessed.
- **D5, serving:** `ACS_CHUNK_CONFIG.grain_steps` serves each year as three
  committed steps (area, tract, place; 90, 120 and 600 minutes), each
  deleting and re-inserting its own grain through
  `refresh_rpt_acs_observations(start, end, geo_levels)`. A database test
  shows a reader of a year's counties under a one-second lock timeout reads
  them while a place step is open, and is blocked by the whole-year path
  (ETL-088). The whole-year path (two arguments) stays for the dashboard
  procedure and the DB-056 checks. Per-step durations are logged and will be
  recorded per year during W-3.

## Checkpoint

Next pickup: W-3, the full-scope city backfill (declare
`place_full_table_year_scope` and `place_year_scope` as "2009-2024", run
one `acs_backfill` year at a time, newest to oldest, record durations and
sizes here).
