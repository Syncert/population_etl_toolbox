---
id: acs-family-tables
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census tests/unit/semantics -q
  - python -m pytest tests/dags -m dag -q
  - ruff format --check . ; ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Three ACS tables show births, working parents and family income for families

## Status

Approved 2026-11-10 17:17Z. Nick, answering the Starting a family plan:
"Add the 3 tables." This is slice 2 of the Starting a family collection
(`STARTING_A_FAMILY_COLLECTION_PLAN.md`, PR #346).

- **Slice 2a, loader (this PR, first commit):** the three tables join the
  ACS curated list for every grain except tracts, with metric definitions.
  They load on the next scheduled `acs_ingest` run (monthly, the 1st at
  06:00Z) after the deploy, with no manual run.
- **Slice 2b, page (same PR, after the chapter PR #346 is on dev):** three
  rows in the chapter's "More for families" fold. Until the load finishes
  each says "Not loaded into this site yet".

## The tables

All three are American Community Survey detailed tables, free on the
Census Data API, read with the key the loader already uses.

| Table | What it publishes | Lines used | Years |
|---|---|---|---|
| B19125 | Median family income by presence of own children under 18 | `_001` all families, `_002` with own children under 18, `_003` without | 5-year 2009 on; 1-year 2005 on |
| B13002 | Women 15 to 50 who had a birth in the past 12 months, by marital status and age | `_001` women 15 to 50, `_002` had a birth | 5-year 2009 on; 1-year 2005 on |
| B23008 | Own children under 18 by age, living arrangement and parents' employment status | `_002` under 6; `_004` two parents both in the labor force; `_010` father alone, in the labor force; `_013` mother alone, in the labor force | 5-year 2009 on |

Line positions for these lines were read from the Data API's group
listings for 2009 and 2023 (5-year) and 2005 (1-year B13002) and are the
same in each; the 2005 1-year B13002 has 11 lines against 19 later, the
first two unchanged. The labels say "labor force", which is working **or
looking for work**; the page says "in the labor force", never "working".
OFFICE_LAB confirms the labels for every loaded year after the metadata sync
(query below) before the page rows go live.

## Grain: every level but tracts

`curated_tables` feeds the nation, states, counties, places and metros;
`tract_tables` is a separate list. The three tables are added to the first
only, because the census schemas are 96% of the warehouse's 2.7 TB and a
tract table is about 25 times its county volume. The nation, states and
metros are small and give each county and city its comparison levels.

The loader already handles a table added after years were loaded
(`census_acs/table_additions.py`, ETL-114): `AcsConfig.added_tables` now
reads `B25103, B13002, B23008, B19125`. A county, place, metro, state or
nation slice that finished under the earlier list asks the Census API for
these tables alone, with or without B25103 still outstanding, so no held
cell is stored twice. Tract fingerprints are unchanged, so no tract slice is
planned.

## Size

Estimated from the per-variable bytes measured on 2026-10-26 and recorded in
`AcsConfig.backfill_bytes_per_variable`, with 49 estimate variables (B23008 27,
B13002 19, B19125 3):

| Grain | Per variable-year | Years | Estimate |
|---|---|---|---|
| acs5 place | 38.5 MB | 2009–2024 (16) | about 30 GB |
| acs1 place | 0.6 MB | 2005–2024 less 2020 (19) | about 0.6 GB |
| counties, metros, states, nation | about a tenth of places | as above | about 4 GB |
| **Total** | | | **about 35 GB**, some 1.3% of the warehouse |

That is an estimate. The measurement is
`python -m data_ingestion_toolbox.census_acs.measure`, which this PR gives a
`--tables` option: it projects only the named tables' variables in every
target year, the reference year included. Run in the Airflow scheduler
container on OFFICE_LAB once the deploy has synced the variable metadata
(the run's first task does it):

```
docker compose -f infra/docker/docker-compose.airflow.yml exec airflow-scheduler python -m data_ingestion_toolbox.census_acs.measure --tables B13002,B23008,B19125 --project acs5:place:2009-2024,acs1:place:2005-2024
```

County projection needs a measured county reference year; the command
projects nothing for a grain it has not measured rather than guess.

## Metric definitions

`docs/semantics/metric_definitions.json` gains draft entries for the ten
lines above (median for B19125, count for the rest), each with its
universe and caveats: own children are the householder's own, so a family
raising grandchildren counts as without; the births question is the
survey's, not registered births; labor force includes looking for work.
`glossary_harvest` adds the `dim_metric` rows after the load; until then the
catalog answers 404 and the rows say "Not loaded into this site yet".

## Page rows (slice 2b)

In the "More for families" fold of Starting a family, same row format as
the eight rows above it (value, year, level strip, gaps), no judgement words
(Nick, 2026-11-10):

1. **Median family income, families with children under 18**, with the
   families-without figure beside it as a second row of the same table.
   Plain published medians.
2. **Women 15 to 50 who had a birth in the past year, per 1,000**: a new
   ADR-0009 share pairing `births_past_year_per_1000_women`, B13002_002 over
   B13002_001, scale 1,000, same table and release, the Bureau's ratio
   margin.
3. **Children under 6 whose parents are all in the labor force**: the
   Bureau's own arithmetic in its profile table DP03 (B23008_004 + _010 +
   _013 over _002). The registry pairs one numerator with one denominator,
   so this needs a summed numerator of lines of the same table, with the
   Bureau's margin formula for sums; the formula and the three counts are
   stated under the row (Nick, 2026-11-08: derived figures show their
   inputs). If that does not fit the page budget (county page 490.8 of
   495 kB), the row shows the three published counts and the total instead,
   and nothing is summed.

The county figure stands on city pages only where the city is not
published (5-year publishes every place, so in practice never).

## Deploy and live checks (OFFICE_LAB)

1. Python-only change for slice 2a: `git merge --ff-only`; no restart.
2. Before December 1: the measurement above; post the projected total.
3. After the December 1 run: metadata labels, and that no tract slice ran:

```
SELECT dataset, year, table_id, COUNT(*) FROM raw_census.acs_variables WHERE table_id IN ('B13002','B23008','B19125') GROUP BY 1,2,3 ORDER BY 1,2,3;
SELECT geo_level, status, COUNT(*) FROM control.acs_ingestion_slices WHERE finished_at >= '2026-12-01' OR variables_hash LIKE 'added-tables-pending:%' GROUP BY 1, 2 ORDER BY 1, 2;
```

4. `glossary_harvest` has run and the Dane County page shows the three rows.

## Acceptance criteria

1. The scheduled run requests the three tables for every published year at
   nation, state, county, place and metro grain, and never at tract grain.
2. A slice already held asks for the added tables alone; no held cell gets a
   second revision.
3. Each loaded line has a metric definition and a `dim_metric` row.
4. The fold shows three rows from published values only; the births and
   labor-force rows state their arithmetic; empty rows say "Not loaded into
   this site yet".

## Checkpoint

2026-11-10: plan written; slice 2a built (config, measure `--tables`,
definitions, tests). Slice 2b waits for PR #346.
