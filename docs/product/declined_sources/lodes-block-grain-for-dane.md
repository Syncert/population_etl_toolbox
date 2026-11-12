# Set aside: Dane County shows jobs by the block people work in and the block they live in

## Decision

Set aside on 2026-11-12 (22:51Z) by Nick, in the game project's thread
"Research infection spread in populations": "I'd prefer to just spread tract
data and call it a day. Distribute it reasonably over the buildings and have
some common sense allocation rules. Avoid burning all my disk space."

The game spreads tract figures over OpenStreetMap buildings instead. The tract
figures it needs are in
[`dane-tract-inputs-for-building-spread`](../../plans/to_do/DANE_TRACT_INPUTS_FOR_BUILDING_SPREAD_PLAN.md).
The plan as drafted is kept below as the record; its links point to where the
plans stood when it was set aside.

## The plan as drafted

### Status

To do. Step 3 of [`outbreak-model-inputs`](../../plans/to_do/OUTBREAK_MODEL_INPUTS_PLAN.md).
The document asked first to merge the LODES pipeline: it is already merged to
`dev` and loaded; its plan
([`census-lehd-lodes`](../../plans/needs_review/CENSUS_LEHD_LODES_PLAN.md)) waits only
on Nick's acceptance. What is missing is block grain: the captured LODES8
files keep every block, and silver sums them to counties.

### Source

LEHD LODES8 workplace (WAC) and residence (RAC) area characteristics, main
job type JT00, already captured per state and year (2002 to 2023) in
`raw_capture`. No new download.

### Design

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
  [`block-population-and-age-for-dane`](block-population-and-age-for-dane.md).
- DQ-LODES block rule: the scoped blocks sum to the county row already
  served, exactly, for every column and year.
- Gold `gold_census_lodes` block view and API observations at block grain
  for the scoped county; a block outside scope is a 404 with the reason.
- Origin-destination stays at county pairs (block pairs are not requested).

LODES block figures carry the Bureau's noise infusion; the API note says so.

### Size

Dane has roughly 6,000 work blocks and 8,000 home blocks with jobs. 29
columns x 22 years with zeros dropped by the file is about 4 million rows,
about 5 GB at the measured 1,300 bytes per fact (was 0.6 GB at the first 150 to 200 bytes). All counties would be about 1,300 GB and needs Nick's go.

### Acceptance criteria

1. Dane block WAC and RAC served for 2002 to 2023, summing exactly to the
   county figures.
2. No race, ethnicity or education column stored.
3. Speed gate under 500 ms for a block list of Dane for one year.
