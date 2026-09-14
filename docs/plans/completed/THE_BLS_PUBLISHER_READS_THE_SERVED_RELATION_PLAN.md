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

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row DB-036 (the plan guessed DB-037; DB-035 landed first).)
- **Last updated:** 2026-09-14
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

- Both arms of `gold_bls`'s publisher now read `gold_bls.mv_bls_latest`:
  `measure_export` keys the served subquery on `'BLS:' || measure.metric_key`
  and the series arm on rows whose `metric_code` *is* the series identity.
  Grains come from the served rows and so does `publication_time`, falling
  back to the fact rows only where nothing is served yet — such a metric
  publishes no grain either, so nothing claims the API can answer it, and the
  harvest's content fingerprint (migration 016) re-harvests once the
  projection carries it.
- The identity rule now matches the serving refresh exactly. The refresh
  writes `COALESCE('BLS:' || measure.metric_key, 'BLS:' || series.series_id)`
  per row, so the series arm publishes a series identity when *its rows*
  carry a `(program_code, measure_code)` pair `dim_bls_measure` does not
  hold. A series with no rows at all keeps its identity only in a program
  with no measure identities — otherwise an empty-answer LAUS series would
  become one of the 13,261 single-place metrics measure identity exists to
  avoid, which is the one thing the old program-level predicate got right.
- Guard (criterion 2), in `test_publisher_contract_shape.py`: every publisher
  whose source's `latest_relation` is a **refreshed projection** must read it
  (or the reporting table it is built from). The scope is derived, not
  listed: a relation created as a `TABLE`/`MATERIALIZED VIEW` lags its facts
  by design, while CDC, FBI UCR and USDA NASS serve through plain views over
  silver, where reading the fact table *with the same predicates* publishes
  exactly what is served — and DB-035's guard is what holds those predicates.
- **A defect in the guards themselves, found on the way.** The shared
  statement matcher stopped at the first `;`, and `gold_fred`'s publisher
  explains its served-relation join in a comment containing one — so the
  matcher had been reading half that view, and the new guard reported FRED as
  reading nothing it serves. `_without_comments` now strips `--` comments
  before matching, in `test_publisher_contract_shape.py` and in DB-035's
  `test_served_geography_resolution.py`, where a predicate named in a comment
  would otherwise have counted as a filter.
- Reverse direction (criterion 3):
  `test_catalog_serving_agreement.py::test_every_served_bls_code_is_a_catalog_code`
  seeds a LAUS series under measure code `10` — one of the codes
  `dim_bls_measure` does not seed — through the real refresh procedures and
  the real harvest, then sweeps every distinct `metric_code` in
  `rpt_bls_observations` for a `dim_metric_catalog` row, and asserts the
  catalog's grains are the served relation's (`{COUNTY}`). DB-025 checked
  catalog to serving only.
- Corrected: two ETL-048 static tests asserted the old shape — grains from
  `fact.geo_level` and the program-level exclusion. Their intent survives and
  their assertions now name the served relation and the per-measure
  predicate.
- Break-tests:
  - reading `fact_bls_observation` for the grains fails the new static guard,
    naming BLS and the two relations it does not read;
  - restoring the program-level exclusion leaves the agreement node failing
    with `these served BLS codes resolve in no catalog row:
    ['BLS:LAUCN9599…10']`.
- Tiers: `pytest tests/unit` 1563 passed; `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 161 passed, 2 skipped,
  14 deselected; `ruff format --check .` and `ruff check .` clean.

## Remaining work

- None.
