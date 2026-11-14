---
id: focus-counties
depends_on: [sub-county-geography]
parallel_safe: false
complexity: medium
verify:
  - python -m pytest tests/unit/census -q
  - python -m pytest tests/unit/shared -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# ACS focus counties

## Status

Ready for review (2026-11-14). Implemented on `claude/project-thread-gjz03f`.
Unit tests pass locally (`tests/unit` outside `tests/unit/api`, 1768 passed;
`test_grain_vocabulary_agreement` fails the same way on `main` in this
environment). The DAG tests (`tests/dags`, including the updated planner count
in `test_task_callables.py`) and the database tier were **not** run here: the
session had no Airflow (Python 3.13) and no Postgres. CI runs both.

## Why

The survival game (a separate project) draws its crowd from Dane County, WI
by race/ethnicity x sex x age band, and its clothing by occupation. Race and
age are on the tract slices only separately today, and multiplying them
assumes every group has the same age profile, which is false (Madison's
Asian and Hispanic residents skew younger). The joint tables are the Bureau's
sex-by-age-by-race tables B01001A-I; with B03002, B24010 and B25034 (year structure built, for
facade aging) that is 12 tables.
At every tract they would be about 24 million estimates a year; at Dane's
~107 tracts, about 35,000. The owner asked to keep disk use small and to stay
at tract grain.

## What changed

- `AcsConfig.focus_counties`: a list of `FocusCounty(county_fips, label,
  tables)`; Dane (55025) with B01001A-I, B03002, B24010, B25034 by default. Validated:
  five digits, a state that publishes tracts, at least one table, no repeats,
  each county once.
- Variable metadata is synced for curated and focus tables
  (`AcsConfig.metadata_tables`); every other slice still reads
  `curated_tables` or `tract_tables`, so county, place and state-wide tract
  volume is unchanged.
- `build_geo_params(..., county_fips)` narrows a tract request to
  `in=state:SS county:CCC`. `ingest_slice(..., county_fips=)` asks for the
  county's tables and refuses `tables` beside it.
- The planner adds one tract slice per focus county per tract year, with its
  own variable-set hash.
- Migration 036 adds `control.acs_ingestion_slices.county_fips` (NULL except on
  focus slices) and swaps the per-state unique index for one including it.
- Catalog: ETL-085.

## Not in scope

Gold serving of the new tables. They land in raw capture and silver like any
tract table; whether the API serves them is a later catalog decision. The
game reads its pinned summary at build time, not live.
