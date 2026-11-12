---
id: block-population-and-age-for-dane
depends_on:
  - group-quarters-by-type
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census_dec2020 tests/unit/silver_ref tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "dec2020 or block" -q
  - python -m pytest tests/dags -m dag -q
---

# Dane County shows how many people of each age live on each block

## Status

To do. Step 4 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).
Places people inside a district smaller than a tract.

## Sources

- 2020 Census Demographic and Housing Characteristics, `api.census.gov/data/2020/dec/dhc`,
  table **P12 "Sex by age for selected age categories"** (P12_001N to
  P12_049N), at block grain for Dane County. The race iterations P12A to P12I
  are not requested.
- Totals already requested by [`group-quarters-by-type`](GROUP_QUARTERS_BY_TYPE_PLAN.md)
  (P1_001N, H1, P5) at block grain for Dane.
- Census TIGER/Line 2020 blocks (`tabblock20`, Wisconsin file), for each
  block's shape, land and water area. Free, public domain, the same
  download family `silver_ref` already reads for tracts.

## Design

- `silver_ref` gains a block geography for scoped counties: geo id
  `state:55|county:025|tract:NNNNNN|block:NNNN`, shape, land and water area,
  vintage 2020; only blocks of `55025` are kept from the state file.
- `census_dec2020` adds the `dhc` dataset and P12, block grain scoped to
  `55025`; facts resolve to the block reference.
- Gold block view with Martin tiles for the Dane block map (one metric at a
  time, as the tract map does).
- API observations at block grain for scoped counties; the note says block
  counts carry the Bureau's disclosure-avoidance noise and are meant for
  spreading people across an area, not for reading one block's exact ages.
- DQ: P12 parts sum to P12_001N per block; blocks sum to the published tract
  and county figures for P1_001N (reported, never adjusted).

## Size

Dane has about 11,000 blocks (about 7,000 with people). 49 + 14 variables
gives about 0.7 million facts plus shapes: about 0.1 GB. Every county would
be about 80 to 100 GB and needs Nick's go.

## Acceptance criteria

1. Every Dane block has its published total and P12 ages served, with its
   shape and land area.
2. Blocks sum to the published tracts and county.
3. Speed gate under 500 ms for one tract's blocks.
