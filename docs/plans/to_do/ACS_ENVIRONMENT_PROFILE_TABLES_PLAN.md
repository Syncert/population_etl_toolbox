---
id: acs-environment-profile-tables
depends_on:
  - focus-counties
parallel_safe: true
complexity: small
verify:
  - python -m pytest tests/unit/census tests/unit/semantics -q
  - python -m pytest tests/dags -m dag -q
  - ruff format --check . ; ruff check .
---

# Heating fuel for every county and city, and Dane's tracts carry housing type, cars and heating fuel

## Status

Requested by Nick on 2026-11-17 (project thread "Draft data-driven environment
profiles"). To do. Nick approved adding the tables below; he asked for this
plan, not a pull request, so it has not been built or merged.

## Why

The survival game (a separate project) builds its environment from the
warehouse. Its profile document found two gaps for the pilot county, Dane
County, WI (55025):

1. **House heating fuel (ACS B25040) is not loaded at all.** The game needs it
   to decide which homes go cold when the power fails: a gas furnace still
   needs electricity for its fan, a wood stove does not, propane means a tank.
2. **Housing type and cars are held only for counties and places.** Year
   structure built (B25034) already reaches Dane's tracts through
   `focus_counties` (ETL-137), but units in structure (B25024) and vehicles
   available (B08201) do not, so every isthmus tract inherits Madison's
   citywide mix.

## What changes

- `AcsConfig.curated_tables`: add `B25040` (House heating fuel, by tenure of
  occupied units) for the nation, states, counties, places and metros. It is
  **not** a tract table for every state: tracts would be about 25 times the
  county volume, and the census schemas are already 96% of the warehouse.
- `AcsConfig.added_tables`: append `B25040`, so the loader asks slices already
  finished for that table alone (`census_acs/table_additions.py`, ETL-114)
  instead of the whole list.
- `AcsConfig.focus_counties`, Dane County (55025): `tables` gains `B25024`,
  `B08201` and `B25040`, beside `B01001A-I`, `B24010` and `B25034`. B25024 and
  B08201 are already curated and already have metric definitions.
- Metric definitions: add `CENSUS_ACS:B25040_*` lines for the six fuel
  categories the page and the game read (utility gas, bottled gas, electricity,
  fuel oil, wood, no fuel) plus the total, with the Census table link as the
  other ACS entries have.
- Tests that pin the lists:
  - `tests/unit/census/test_focus_counties.py`: the Dane table set gains the
    three tables.
  - `tests/unit/census/test_acs_table_additions.py`: `added_tables[-1]` is
    `B25040`, it is curated, is not a tract table and is a Dane focus table;
    the "asks only for the tables it lacks" test now expects `B25040` last and
    a slice finished before it asks for `B25040` alone.
  - `tests/dags/test_task_callables.py`: the planner's work-unit count stays
    160 (no new slices; Dane's focus slice has one more table), but its
    fingerprint changes, so re-check the focus-slice assertion.
- `docs/reference/TESTING_CONTRACT.md` ETL-114 and ETL-137 rows mention the
  table lists; update both.

## Size

Dane's focus slice goes from 11 tables to 14, about 107 tracts. B25024 has
about 10 lines, B08201 about 6 and B25040 about 9, so roughly 25 more
estimates per tract and year: under 3,000 per year, 16 years (2009-2024),
under 50,000 rows in all. B25040 at county, place and metro grain is a few
hundred thousand estimates a year. Both are negligible against the existing
volume. No new tract table for the other states.

## Not in scope

- Serving B25040 through gold views and the API. It lands in raw capture and
  silver like any curated table; whether the API serves it is a later catalog
  decision. The game reads a pinned summary at build time, not live.
- Other counties' tracts. Another county is one more `FocusCounty` entry.
- FBI crime and income: both are already loaded (county crime rollup; ACS
  B19013, B17001, B19083 at tract) and need no change here.

## Definition of done

- The four verify commands pass.
- On the next scheduled `acs_ingest` run, years already held ask for `B25040`
  alone at county, place and metro grain; Dane's 16 focus slices ask for their
  14 tables.
- After the load: `B25040` has a value for Dane County (55025) and for the
  Madison place; B25024, B08201 and B25040 have values for Dane's tracts in
  the newest 5-year year, and a spot check of one tract's B25040 total against
  its B25002 occupied-unit count agrees.
