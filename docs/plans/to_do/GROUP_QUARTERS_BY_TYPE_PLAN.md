---
id: group-quarters-by-type
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census_decennial tests/unit/api tests/unit/shared -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "decennial" -q
  - python -m pytest tests/dags -m dag -q
---

# Every county and tract shows how many people live in nursing homes, college housing and prisons

## Status

To do. Step 1 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md)
(Nick, 2026-11-12). Care homes, dorms and prisons are the outbreak model's
largest hotspots, and the 2020 Census counts their residents by type.

## Source

2020 Census Redistricting Data (Public Law 94-171), `https://api.census.gov/data/2020/dec/pl`,
table **P5 "Group quarters population by major group quarters type"**, on the
Census Data API the toolbox already calls with `CENSUS_API_KEY`. Public domain.

| Variable | Label |
| --- | --- |
| P5_001N | Total group quarters population |
| P5_002N | Institutionalized population |
| P5_003N | Correctional facilities for adults |
| P5_004N | Juvenile facilities |
| P5_005N | Nursing facilities / skilled-nursing facilities |
| P5_006N | Other institutional facilities |
| P5_007N | Noninstitutionalized population |
| P5_008N | College / university student housing |
| P5_009N | Military quarters |
| P5_010N | Other noninstitutional facilities |

Plus **P1_001N** (total population, the denominator a share needs; no other
P1 cell, since P1 is the race table) and **H1_001N to H1_003N** (housing
units, occupied, vacant).

Grain: nation, state, county and tract for every state; **block for Dane
County (55025) only**. The 2020 count is published once (reference date
2020-04-01); 2030 joins as a new vintage when it is published.

## Design

A new adapter `census_decennial` (the 2020 Census as its own source, beside the
ACS adapter, which keeps its crosswalk-to-ACS design for 2000 and 1990):

- `registry.py`: datasets (`pl` now; `dhc` for
  [`block-population-and-age-for-dane`](BLOCK_POPULATION_AND_AGE_FOR_DANE_PLAN.md)),
  tables with their requested variables declared, never matched by label,
  and grains with the block scope declared as a county list (`55025`).
- `client.py` and `capture.py`: one request per dataset, table, grain and
  state (blocks: per tract of a scoped county), each captured losslessly in
  `raw_capture` before parsing; the key is never in a capture, fingerprint or
  log (redaction as in `census_acs`).
- Silver `silver_census_decennial.fact_count`: one row per variable and
  geography, the published integer as written, `geo_id` in the shared form
  (`state:55|county:025|tract:000100`, blocks `...|block:1001`), a lineage row
  per fact. A value the API returns null is missing, never zero.
- Block geography: tracts resolve against `silver_ref` 2020 tracts; blocks are
  kept with `geography_status = 'unmapped'` until the block reference of
  `block-population-and-age-for-dane` lands, and are not served before then.
- Gold `gold_census_decennial` latest view, publisher metrics (catalog rows per
  variable), API discovery and observation dispatch entries, glossary
  harvest.
- DAG `census_decennial_ingest`, manual trigger only (arrives paused), with an
  `ensure_*_schema` task upstream of capture.
- DQ: `DQ-DECENNIAL-001` the parts add to the totals as published
  (P5_002 + P5_007 = P5_001; P5_003..006 = P5_002; P5_008..010 = P5_007);
  `DQ-DECENNIAL-002` tract sums equal the county figure; `DQ-DECENNIAL-003` every
  registered variable is present for every geography requested. A mismatch
  is reported (WARN) and the figures stay as published.

## Steps

1. Probe on the internal stack: P5, P1_001N, H1 for Wisconsin counties and
   Dane tracts and blocks; record row counts and timing.
2. Adapter, fixtures from the probe, unit and replay tests.
3. Gold, API, catalog, DQ, DAG; docs (data sources, operations guide,
   consumer guide, testing catalog).
4. Load on the internal stack; speed gate under 500 ms; Dane figures
   reported to Nick.

## Size

About 0.9 million facts nationally (10 variables x 88,000 counties and
tracts, plus P1 and H1) and about 0.15 million for Dane blocks: about 0.3 GB
with captures and indexes.

## Later (not in this plan)

DHC PCO3, PCO5 and PCO8 (residents of adult correctional, nursing and college
housing by sex by age) at county grain, if the model needs ages inside a
group-quarters type. About 0.1 GB.

## Acceptance criteria

1. `GET /api/v1/observations` returns P5 counts for Dane County, its tracts,
   Wisconsin and the nation, as published, with the source named.
2. DQ-DECENNIAL-001..003 pass or report published discrepancies.
3. No race, ethnicity, income or poverty variable is requested.
4. Speed gate under 500 ms on the served view.
