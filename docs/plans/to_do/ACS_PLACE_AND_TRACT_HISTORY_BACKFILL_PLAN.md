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

To do. Drafted 2026-10-12 at Nick's request ("the ultimate intent would be
to have every available year at that granularity"). No implementation yet.

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

## Checkpoint

Next pickup, once `claude/project-thread-ypfqia` is merged and the 2024
publish has finished: write the Deliverable 1 measurement script and record
its 2024 output here, then the failing unit test that a run restricted to
one year and grain forms no other request.
