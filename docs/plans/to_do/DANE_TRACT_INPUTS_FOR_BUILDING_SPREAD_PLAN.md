---
id: dane-tract-inputs-for-building-spread
depends_on:
  - group-quarters-by-type
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census tests/unit/census_lodes tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "acs or lodes" -q
  - python -m pytest tests/dags -m dag -q
---

# Dane County tracts carry what a game needs to spread people over buildings

## Status

To do. Replaces steps 3 and 4 of
[`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md)
(`lodes-block-grain-for-dane` and `block-population-and-age-for-dane`).

Nick decided on 2026-11-12 (22:51Z), in the game project's thread "Research
infection spread in populations":

> I'd prefer to just spread tract data and call it a day. Distribute it
> reasonably over the buildings and have some common sense allocation rules.
> Avoid burning all my disk space.

So no block data is loaded, in Dane or anywhere else. The game places people
inside its 0.5 sq km districts by spreading **tract** figures over the
OpenStreetMap buildings of the district. This plan adds the few tract figures
that spread needs and the warehouse does not yet hold, for Dane County only.

## Who uses this, and what stays outside this repository

The consumer is the game's offline map build, which is **outside this
repository**, like the outbreak model. It reads tract figures from
`/api/v1/observations`, matches them to OpenStreetMap buildings, and applies
its allocation rules (group quarters first, homes scaled to the tract's housing
units, ages from the tract's age bands, largest-remainder rounding). Those
rules are written in the game's research doc, "How infections spread through
real populations", section "Placing people in buildings".

The allocation is a model: it scales and imputes. This repository's rule for
the outbreak inputs is true to source, with no scaling or imputation, so the
allocation and the OpenStreetMap buildings stay in the game project. Nothing
here stores a per-building estimate.

## What the spread needs, checked against `dev` (33494841, 2026-11-12)

| Input | Use in the spread | Source | Tract grain today |
| --- | --- | --- | --- |
| People and ages | Tract total and age bands to hand out | ACS B01003, B01001 | Held (`tract_tables`) |
| Housing units, occupied, vacant | The number of homes to fill, and how many stay empty | ACS B25001, B25002; 2020 Census H1 | Held |
| Tenure | Owner houses against rented apartments | ACS B25003 | Held |
| Household type, living alone | Household sizes, single adults | ACS B11001 | Held |
| Group quarters by type | People in nursing homes, college housing and prisons, placed before homes | 2020 Census P5 | Held ([`group-quarters-by-type`](GROUP_QUARTERS_BY_TYPE_PLAN.md), nation to tract) |
| Nursing-home beds, college housing capacity | Caps on care-home and dorm buildings | CMS-671 and SNF cost reports; IPEDS | County, held or landing ([`nursing-home-locations`](NURSING_HOME_LOCATIONS_PLAN.md), [`college-housing-capacity`](COLLEGE_HOUSING_CAPACITY_PLAN.md)) |
| **Units in structure** | How many homes a building holds: detached houses against large apartment blocks | ACS B25024 | **Missing: county and place only** |
| **People 65 and over by living arrangement** | Older people living alone, cut off when power and phones fail | ACS B09020 | **Missing: county only** |
| **Jobs by work tract and home tract** | Daytime against night-time population: who is in the district at 2 pm | LODES8 WAC and RAC | **Missing: captured per block, served only as county sums** |

## Deliverables

1. **ACS tables at tract grain for scoped counties.** A declared list of
   counties (`55025` only) whose tracts are also requested for B25024 and
   B09020, beside the national `tract_tables`, which do not change. Newest
   5-year period only. One request per table for the scoped county
   (`for=tract:*&in=state:55 county:025`). Another county is a one-line change
   with Nick's go.
2. **LODES summed to tract for scoped counties.** A replay of the Wisconsin
   WAC and RAC files already held in `raw_capture` (no new download) that
   sums the blocks of `55025` to their tract: the tract is the first eleven
   digits of the block code, and it resolves against the `silver_ref` 2020
   tracts. No block reference and no block row are stored. Newest LODES8 year
   by default (2023), with the year list declared; 2019 is the natural second
   year if a pre-pandemic baseline is wanted. Columns kept: C000, CA01 to CA03
   (worker age), CE01 to CE03 (earnings bands), CNS01 to CNS20 (sector),
   CS01 and CS02 (sex). **The race, ethnicity and education columns (CR, CT,
   CD) are not kept.** Origin-destination stays at county pairs.
3. **Serving.** Gold views and `/api/v1/observations` at tract grain for the
   scoped county, the existing tract map painting them. A tract outside the
   scope answers 404 with the reason. The note on LODES tract figures says
   they carry the Bureau's noise infusion and sum blocks.
4. **Data quality.** The scoped tracts sum to the county row already served,
   exactly, for every LODES column and year (reported, never adjusted). The
   ACS parts add to their totals as published.
5. **Docs.** Operations guide, consumer guide and data sources updated; the
   decision above recorded in each.

## Size

Dane has about 120 tracts in the 2020 geography. B25024 and B09020 are about
30 variables; LODES keeps 31 columns for each of WAC and RAC. That is about
3,600 ACS facts and 7,400 LODES facts a year, about **15 MB** at the measured
1,300 bytes per fact (about 0.2 GB if every LODES year from 2002 were
replayed). The two block plans this replaces were estimated at about 6 GB for
Dane alone and about 2,000 GB nationally.

## Not in this plan

- Census blocks of any kind: people, ages, jobs, shapes or tiles.
- OpenStreetMap buildings and the per-building allocation (game project).
- B25010 average household size at tract: the spread derives people per
  occupied home from B01003, P5 and B25002, which are held.

## Acceptance criteria

1. `GET /api/v1/observations` returns B25024 and B09020 for each Dane tract,
   as published, with the source and period named.
2. It returns LODES WAC and RAC jobs for each Dane tract for the declared
   years, and the tracts sum exactly to the Dane county row for every column.
3. A tract outside `55025` is refused with the reason, not served empty.
4. No race, ethnicity, income, poverty or education field is requested or kept.
5. Measured size recorded here after the load, and under 0.5 GB.
6. Speed gate: one Dane tract's read answers in under 500 ms.

## Checkpoint

Not started.
