---
id: fred-publisher-derived-grains
branch: fix/fred-publisher-derived-grains
depends_on:
  - catalog-grain-vocabulary
parallel_safe: true
complexity: low
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 integration
---

# FRED publishes the grains it serves

## Plan status

- **Status:** To do. Filed 2026-09-12 from the catalog-grain-vocabulary
  plan, which made every other publisher derive `valid_geo_grains` from
  served rows and left FRED declaring `ARRAY['NATIONAL']` because it
  happened to be true.
- **Last updated:** 2026-09-12
- **Owner surface:** `src/data_ingestion_toolbox/fred/gold_fred/DDL/publisher.sql`,
  `tests/integration/database/` (FRED silver flow), `tests/integration/api/test_catalog_serving_agreement.py`
- **Depends on:** `catalog-grain-vocabulary` (the vocabulary function and
  DB-028), in `needs_review/`.

## Context

`gold_fred.metric_publisher` publishes `ARRAY['NATIONAL']::TEXT[] AS
valid_geo_grains` for every series. Every FRED series served today is
national, so the declaration is correct — but it is correct the way the ACS
declaration was correct on the day it was written, and the ACS declaration
was found on 2026-09-12 to advertise 2,487 metric/grain pairs nothing served.
A declared grain is a claim about the future; a derived grain is a fact about
the rows.

DB-028 would catch a FRED series that stopped serving national rows, but it
would catch it as a catalog defect after the fact. Deriving removes the
category: a series with no served rows publishes no grain, and the agreement
guards report a current code with nothing behind it rather than a grain the
publisher invented.

FRED is also the source most likely to gain a non-national series (regional
FRED series exist and are one configuration entry away), and the first such
series would be published as `NATIONAL` today.

## Objective

`gold_fred.metric_publisher` derives `valid_geo_grains` from the relation the
API serves, through `gold_glossary.geo_grain`, with an empty array for a
series nothing serves.

## Scope

- The publisher aggregates `UPPER(geo_level)` (or the served relation's
  grain column) per series from the latest serving relation FRED's dispatch
  entry names, as the ACS publisher does over `mv_acs_latest`.
- The FRED silver-flow integration test asserts the harvested grain matches
  the rows it seeded, and that a series with no rows publishes no grain.
- Harvest cost measured on the development warehouse and recorded; FRED is
  small, but the number belongs in the plan.
- Re-harvest `gold_fred` on the development stack; DB-028 sweep green.

## Acceptance

- No publisher in the repository declares `valid_geo_grains` from
  configuration; `grep -rn "ARRAY\['NATIONAL'" src/ sql/` over publisher
  views is empty.
- DB-028 passes with FRED exercised.

## Non-goals

Adding non-national FRED series; changing FRED's dispatch entry.
