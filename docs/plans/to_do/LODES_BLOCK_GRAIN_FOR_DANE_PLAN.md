---
id: lodes-block-grain-for-dane
depends_on:
  - block-population-and-age-for-dane
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census_lodes tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "lodes" -q
  - python -m pytest tests/dags -m dag -q
---

# Dane County shows jobs by the block people work in and the block they live in

## Status

To do. Step 3 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).
The document asked first to merge the LODES pipeline: it is already merged to
`dev` and loaded; its plan
([`census-lehd-lodes`](../needs_review/CENSUS_LEHD_LODES_PLAN.md)) waits only
on Nick's acceptance. What is missing is block grain: the captured LODES8
files keep every block, and silver sums them to counties.

## Source

LEHD LODES8 workplace (WAC) and residence (RAC) area characteristics, main
job type JT00, already captured per state and year (2002 to 2023) in
`raw_capture`. No new download.

## Design

- A block replay reads the Wisconsin captures already held and writes
  `silver_census_lodes.fact_block_area` for blocks whose first five digits
  are a scoped county (`55025` declared in `census_lodes/registry.py`, a
  list so another county is a one-line change with Nick's go).
- Columns kept: C000 (all jobs), CA01 to CA03 (worker age), CE01 to CE03
  (earnings bands), CNS01 to CNS20 (sector, CNS16 health care and social
  assistance), CS01 to CS02 (sex). **The race and ethnicity columns (CR, CT)
  and education columns (CD) are not kept.** A block-column the file writes
  as zero is stored as zero; the file's blocks are 2020 blocks for LODES8.
- Blocks resolve against the Dane block reference from
  [`block-population-and-age-for-dane`](BLOCK_POPULATION_AND_AGE_FOR_DANE_PLAN.md).
- DQ-LODES block rule: the scoped blocks sum to the county row already
  served, exactly, for every column and year.
- Gold `gold_census_lodes` block view and API observations at block grain
  for the scoped county; a block outside scope is a 404 with the reason.
- Origin-destination stays at county pairs (block pairs are not requested).

LODES block figures carry the Bureau's noise infusion; the API note says so.

## Size

Dane has roughly 6,000 work blocks and 8,000 home blocks with jobs. 29
columns x 22 years with zeros dropped by the file is about 4 million rows,
about 0.6 GB. All counties would be about 150 GB and needs Nick's go.

## Acceptance criteria

1. Dane block WAC and RAC served for 2002 to 2023, summing exactly to the
   county figures.
2. No race, ethnicity or education column stored.
3. Speed gate under 500 ms for a block list of Dane for one year.
