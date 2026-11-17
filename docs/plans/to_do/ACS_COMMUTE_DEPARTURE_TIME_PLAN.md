---
id: acs-commute-departure-time
depends_on:
  - focus-counties
  - acs-environment-profile-tables
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/census tests/unit/semantics -q
  - python -m pytest tests/dags -m dag -q
  - ruff format --check . ; ruff check .
---

# When people leave for work, and Dane's tracts carry the commute and work tables

## Status

Requested for Nick on 2026-11-17 (survival game thread "Build census-driven
neighbourhood routines"). To do. A plan, not a pull request: nothing is built.

## Why

The survival game now starts a run before the outbreak, on an ordinary weekday
morning, and gives every resident a day plan from census data: whether they
work, whether from home, how they travel, when they leave and how long the trip
is. Two gaps for the pilot county, Dane County, WI (55025):

1. **Time leaving home to go to work (ACS B08302) is not loaded at all.** It is
   the shape of the morning wave the player watches, so it is the one table the
   routines cannot fake for long. The game uses a US-wide stand-in until then.
2. **Checked at build time (2026-11-17): the commute and work tables are
   already tract tables.** B08301 (means of transportation), B08303 (travel
   time), B23025 (employment status) and B11001 (household type) are in
   `tract_tables` for every state, and B24010 (occupation) is a Dane focus table.
   Only B08302 is missing at tract grain, so the build adds B08302 and leaves the
   other four out of Dane's focus tables: listing them there would ask for the
   same cells a second time.

## What changes

- `AcsConfig.curated_tables`: add `B08302` (Time leaving home to go to work,
  workers 16+ who did not work from home) for the nation, states, counties,
  places and metros. Like B25040, not a tract table for every state.
- `AcsConfig.added_tables`: append `B08302`, so finished slices ask for that
  table alone (`census_acs/table_additions.py`, ETL-114).
- `AcsConfig.focus_counties`, Dane County (55025): `tables` gains `B08302`
  alone (beside B24010, B25034 and the environment-profile additions B25024,
  B08201, B25040). B08301, B08303, B23025 and B11001 reach Dane's tracts through
  `tract_tables`.
- Metric definitions: `CENSUS_ACS:B08302_*` for the total and the 14 departure
  buckets (12:00 a.m. to 4:59 a.m. ... 4:00 p.m. to 11:59 p.m.), with the Census
  table link as the other ACS entries have.
- Tests that pin the lists:
  - `tests/unit/census/test_focus_counties.py`: the Dane table set gains B08302.
  - `tests/unit/census/test_acs_table_additions.py`: `added_tables[-1]` is
    `B08302`, it is curated, is not a tract table and is a Dane focus table.
  - The planner's work-unit count is unchanged (no new slices; Dane's focus
    slice carries one more table); no DAG test pins a count.

Note: `focus_counties` (ETL-137) is on `dev`, and the environment-profile plan
(PR #396) has landed.

## What the game reads

For each tract that touches the play area, newest ACS 5-year: B08302 lines 1-15,
B08301 lines 1, 2, 10, 18, 19, 21, B08303 lines 1-13, B23025 lines 1, 2, 4, 7,
B11001 lines 1, 2, 7, 8, B24010 totals by major group. The game pins them under
`Data/population/raw/` at build time, as it does B01001 and B25001 today.
