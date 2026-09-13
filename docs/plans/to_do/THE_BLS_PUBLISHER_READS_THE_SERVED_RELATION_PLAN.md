---
id: the-bls-publisher-reads-the-served-relation
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# The BLS publisher reads the relation the API serves, and every served code is a catalog code

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/bls/gold_bls/DDL/publisher.sql`,
  `tests/integration/api/test_catalog_serving_agreement.py`

## Context

`gold_fred.metric_publisher` reads `gold_fred.mv_fred_latest` and says why:
"the relation the dispatch entry names for a `latest` read: a grain
published here is one the API can answer" (`fred/.../publisher.sql:54-60`).
`gold_census.metric_publisher` was changed the same way under DB-025/DB-028
after 2,487 unanswerable metric/grain pairs.

`gold_bls.metric_publisher` was not. Its `measure_export` and series arm
read `gold_bls.fact_bls_observation` (`publisher.sql:28-31`, `:63`), a view
straight over `silver_bls.fact_labor_statistics`, while the dispatch entry's
`latest_relation` is `gold_bls.mv_bls_latest` (`registry.py:148`).

## Findings

- `valid_geo_grains` and `publication_time` (`MAX(fact.updated_at)`)
  advance at **silver ingest**, before the serving refresh. FRED's file
  names the hazard -- "the glossary harvest must run after the serving
  refresh" -- and for BLS the ordering does not help, because the harvest
  is satisfied by silver.
- Land 2025 county LAUS in silver, harvest, then refresh: the catalog
  publishes `COUNTY` and a new fingerprint; `/observations?geo_level=COUNTY`
  reads `mv_bls_latest` and answers an empty page; and because the
  fingerprint is recorded, the next harvest skips (`harvest.py:232-236`),
  so the catalog stays wrong until something else republishes.
- A second, related disagreement: the serving refresh assigns identity
  **per (program_code, measure_code)** (`gold_bls.sql:301-311`, COALESCE
  over `dim_bls_measure` else the series id), while the publisher's series
  arm excludes **per program** (`publisher.sql:64`). `dim_bls_measure` seeds
  seven LA measure codes; ingest accepts any two-digit code. Add LA measure
  `10` and the serving layer emits `BLS:LAU...10` rows that the publisher
  never publishes: `/observations?metric_code=...` answers "unknown metric"
  while `/bls/observations/timeseries` pages the rows. DB-025 checks only
  catalog to serving, not the reverse.

## Acceptance criteria

1. `gold_bls.metric_publisher` derives grains and publication time from
   `gold_bls.mv_bls_latest`, with the same comment its siblings carry.
2. A static guard in `tests/unit/shared/test_publisher_contract_shape.py`
   (or beside ARC-007) reads every publisher's SQL and the registry and
   fails when a publisher reads a relation other than its dispatch entry's
   `latest_relation` (or the reporting table it is built from).
3. The catalog/serving agreement tier asserts the reverse direction: every
   distinct `metric_code` in `rpt_bls_observations` has a
   `dim_metric_catalog` row. Failing first with an unseeded measure code.
4. The publisher's series arm and the serving refresh agree on when a
   series keeps its own identity -- both per `(program_code, measure_code)`
   -- so the reverse-direction guard passes for the seeded and the
   unseeded case alike.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DB-`
   identifier; DB-037 at authoring time).

## Non-goals

- Seeding more measures into `dim_bls_measure`. That is the LAUS measure
  plan's decision; this plan makes the two layers agree whatever it holds.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
