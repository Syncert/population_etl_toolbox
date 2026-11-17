---
id: acs-family-tables
depends_on:
  - starting-a-family-collection
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
"Add the 3 tables." This is slice 2 of
[the Starting a family collection](STARTING_A_FAMILY_COLLECTION_PLAN.md).

- **Slice 2a, loader:** the three tables join the
  ACS curated list for every grain except tracts, with metric definitions.
  They load on the next scheduled `acs_ingest` run (monthly, the 1st at
  06:00Z) after the deploy, with no manual run.
- **Slice 2b, page:** four rows from the three tables in the chapter's
  "More for families" fold, read only once the fold is opened. Until the load finishes
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

Measured on OFFICE_LAB on 2026-11-10 (`census_acs.measure`, read-only, at
c0a71f11; output in the project files as `acs_measure_20261110.txt`). The
2024 5-year year holds 3,850 curated variables: counties 13.1 GiB (silver
and gold), about 3.4 MiB per variable; places 131.4 GiB, about 34 MiB per
variable. The three tables add 49 estimate variables (B23008 27, B13002 19,
B19125 3):

| Grain | Per variable-year | Years | Size |
|---|---|---|---|
| acs5 place | 34 MiB | 2009–2024 (16) | about 26 GiB |
| acs5 county | 3.4 MiB | 2009–2024 (16) | about 2.6 GiB |
| acs1 place, county; metros, states, nation | well under the above | 2005 on | about 1–2 GiB |
| **Total** | | | **about 30 GiB** |

The disk has 2,369 of 6,047 GiB free (39%), and keeps 39% free afterwards,
well above the 20% reserve. The warehouse is 3,349 GiB, ACS silver 1,246 GiB.

`measure` gains a `--tables` option for later additions: it projects only
the named tables' variables in every target year, the reference year
included. It reads their variable count from `raw_census.acs_variables`, so
it works only once the run's metadata sync has filed the tables; before
that it projects nothing rather than guess.

## Metric definitions

`docs/semantics/metric_definitions.json` gains draft entries for the ten
lines above (median for B19125, count for the rest), each with its
universe and caveats: own children are the householder's own, so a family
raising grandchildren counts as without; the births question is the
survey's, not registered births; labor force includes looking for work.
`glossary_harvest` adds the `dim_metric` rows after the load; until then the
catalog answers 404 and the rows say "Not loaded into this site yet".

## Page rows (slice 2b)

These go in the "More for families" fold of Starting a family. They use the
same row format as the eight rows above it (value, year, level strip, gaps),
with no judgement words (Nick, 2026-11-10). A closed fold reads nothing.

1. **Median income of families with children under 18** (B19125_002) and,
   as its own row, **without** (B19125_003). Plain published medians.
2. **Women 15 to 50 who had a baby in the past year**: the share pairing
   `births_past_year_share`, B13002_002 ÷ B13002_001, in percent. Both come
   from the same table and release, with the Bureau's proportion margin.
3. **Children under 6 whose parents are all in the labor force**: the
   share pairing `young_children_all_parents_in_labor_force_share`,
   B23008_004 + _010 + _013 ÷ _002. This is the Bureau's own DP03
   arithmetic, and it is added to ADR-0009 as `numerator_addends`
   (amendment of 2026-11-10). The note under the row states the formula
   and that the labor force includes looking for work. If a line is
   missing, the row is `partial_input` and is not divided.

Every ACS level is published, so a city shows its own figure.

## Deploy and live checks (OFFICE_LAB)

1. Python-only change for slice 2a: `git merge --ff-only`; no restart.
2. After the December 1 run: metadata labels, and that no tract slice ran:

```
SELECT dataset, year, table_id, COUNT(*) FROM raw_census.acs_variables WHERE table_id IN ('B13002','B23008','B19125') GROUP BY 1,2,3 ORDER BY 1,2,3;
SELECT geo_level, status, COUNT(*) FROM control.acs_ingestion_slices WHERE finished_at >= '2026-12-01' OR variables_hash LIKE 'added-tables-pending:%' GROUP BY 1, 2 ORDER BY 1, 2;
```

3. `glossary_harvest` has run and the Dane County page shows the three rows.

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

2026-11-10: plan written. Slice 2a built: the config, measure `--tables`,
the definitions and the tests. The size was measured on OFFICE_LAB at about
30 GiB. Slice 2b built on top of #346: two pairings, the summed-numerator
amendment and the fold rows. The rows say "Not loaded into this site yet"
until the December 1 run and `glossary_harvest` have both happened.
