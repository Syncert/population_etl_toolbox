# Set aside: Dane County shows how many people of each age live on each block

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

To do. Step 4 of [`outbreak-model-inputs`](../../plans/to_do/OUTBREAK_MODEL_INPUTS_PLAN.md).
Places people inside a district smaller than a tract.

### Sources

- 2020 Census Demographic and Housing Characteristics, `api.census.gov/data/2020/dec/dhc`,
  table **P12 "Sex by age for selected age categories"** (P12_001N to
  P12_049N), at block grain for Dane County. The race iterations P12A to P12I
  are not requested.
- Totals already requested by [`group-quarters-by-type`](../../plans/to_do/GROUP_QUARTERS_BY_TYPE_PLAN.md)
  (P1_001N, H1, P5) at block grain for Dane.
- Census TIGER/Line 2020 blocks (`tabblock20`, Wisconsin file), for each
  block's shape, land and water area. Free, public domain, the same
  download family `silver_ref` already reads for tracts.

### Design

- `silver_ref` gains a block geography for scoped counties: geo id
  `state:55|county:025|tract:NNNNNN|block:NNNN`, shape, land and water area,
  vintage 2020; only blocks of `55025` are kept from the state file.
- `census_decennial` adds the `dhc` dataset and P12, block grain scoped to
  `55025`; facts resolve to the block reference.
- Gold block view with Martin tiles for the Dane block map (one metric at a
  time, as the tract map does).
- API observations at block grain for scoped counties; the note says block
  counts carry the Bureau's disclosure-avoidance noise and are meant for
  spreading people across an area, not for reading one block's exact ages.
- DQ: P12 parts sum to P12_001N per block; blocks sum to the published tract
  and county figures for P1_001N (reported, never adjusted).

### Size

Dane has about 11,000 blocks (about 7,000 with people). 49 + 14 variables
gives about 0.7 million facts plus shapes: about 1 GB at the measured 1,300
bytes per fact (was 0.1 GB). Every county would be about 660 GB and needs
Nick's go.

### Acceptance criteria

1. Every Dane block has its published total and P12 ages served, with its
   shape and land area.
2. Blocks sum to the published tracts and county.
3. Speed gate under 500 ms for one tract's blocks.
