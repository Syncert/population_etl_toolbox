---
id: observation-paging-determinism
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
  - npm --prefix apps/web run test:unit
---

# The observation list routes page a total order

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13; `to_do/` and `in_progress/` were empty, so this plan was written from an investigation of the API surface rather than claimed from the queue.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/routers/observations.py`,
  `apps/api/routers/source_observations.py`,
  `apps/api/services/observations_service.py`, `apps/api/registry.py`,
  `src/data_ingestion_toolbox/sql/observation_queries.py`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`docs/reference/API_CONSUMER_GUIDE.md` makes two promises about list
resources:

> Pagination is `limit`/`offset` with documented deterministic ordering per
> resource.

> Deterministic ordering is part of the contract, because a paging client
> depends on it whether or not anyone promised it.

Four observation reads keep neither promise.

**The time-series routes cannot page at all.** `GET /api/v1/observations/timeseries`
and `GET /api/v1/{bls,census,fred,pep}/observations/timeseries` declare
`limit` and no `offset`. Both count every matching row into `total` and then
hard-code `offset=0` into the response envelope, so a caller is told there are
more rows than were served and has no parameter that would reach them. The
truncation is not neutral either: the reads order `observation_date ASC`, so
the page that survives is the *oldest* `limit` rows and the newest values —
the end of the line a chart draws — are the ones dropped. The bound is
reachable: `gold_fred.rpt_fred_observations` holds one row per observation
date per series, and a daily FRED series passes `limit`'s 5,000 ceiling
inside twenty years.

**One latest route pages a non-total order.** `list_latest_observations_for_source`
orders by `geo_id` alone. For BLS, Census ACS, and FRED that is total —
`uq_mv_bls_latest`, `uq_mv_acs_latest`, and `uq_mv_fred_latest` leave one row
per geography once a metric is pinned — but `gold_pep.mv_pep_latest` is a
join over `gold_pep.population_estimate_latest` that keeps **every estimated
year of the current vintage**, which the consumer guide states plainly:
`CENSUS_PEP:BIRTHS` at `geo_level=COUNTY` answers 3,144 counties times six
years. Ordering 18,864 rows by 3,144 distinct `geo_id` values leaves six-way
ties, and PostgreSQL promises nothing about tie order between two executions.
Two pages of the same query can therefore repeat a row and skip another.

The time-series reads have the same defect for the same reason: the
as-published relations hold several rows per `observation_date` — an ACS
metric is published under more than one vintage, a PEP measure under more
than one vintage and capture — so `ORDER BY observation_date ASC` is not a
total order either, and adding `offset` to a non-total order would only make
the gap reachable.

This is API-level work on stable warehouse contracts: no relation changes,
and every ordering column below already exists in the relation's own unique
index.

## Objective

Every observation list route pages a declared total order, and every one of
them can reach the rows it counts.

## Acceptance criteria

1. `GET /api/v1/observations/timeseries` and
   `GET /api/v1/{source}/observations/timeseries` accept `offset`, bounded
   exactly as every other list route (`ge=0, le=100000`), and echo the value
   they were given in the response envelope.
2. Each of the four reads orders by a total order over the relation it reads:
   no two rows of one response can tie on the full ORDER BY list.
3. The per-source orders are declared once, on `ServingContract` in the
   reviewed registry, alongside the relations they order — not spelled into
   the service — and each one is the relation's own unique-index key with the
   columns the query already pins removed.
4. A registry guard proves every registered contract declares both orders, so
   a source added later cannot arrive with an empty one.
5. The change is additive under ADR-0002 (a new optional parameter, a relaxed
   bound), the reviewed OpenAPI snapshot is regenerated deliberately, and the
   consumer guide states the ordering each resource pages.
6. `TESTING_CONTRACT.md` carries the new behavior as a catalog row with CI
   ownership.

## Delivery

| Item | Where |
| --- | --- |
| `offset` on both time-series routes | `apps/api/routers/observations.py`, `apps/api/routers/source_observations.py` |
| `offset` threaded through the reads and echoed in the envelope | `apps/api/services/observations_service.py`, `src/data_ingestion_toolbox/sql/observation_queries.py` |
| `latest_order` / `history_order` declared per source | `apps/api/registry.py` |
| Cross-source union order | `src/data_ingestion_toolbox/sql/observation_queries.py` |
| Tests | `tests/unit/api/test_observations.py`, `tests/unit/api/test_source_observations.py`, `tests/unit/api/test_sql_query_builders.py`, `tests/unit/api/test_serving_registry.py` |
| Contract documents | `docs/reference/API_CONSUMER_GUIDE.md`, `docs/reference/TESTING_CONTRACT.md`, `tests/fixtures/api/openapi_contract.json` |

## Non-goals

- Changing the default `limit` of any route.
- Widening the cross-source union views, or adding a relation column to make
  an order total; every column used is already in the relation.
- Touching the provider-neutral `/observations` resource, whose orders are
  already declared per source in `OBSERVATION_DISPATCH`.
- Re-ordering the cross-source `latest` read, which is one row per geography
  for all three sources it unions.

## Evidence

### The gap, established first

`tests/unit/api/test_serving_registry.py::test_every_contract_declares_a_total_order_for_both_relations`
and `::test_reads_order_by_the_declared_order` were written before the
registry carried an order and failed 8/8 (four sources times two tests) with
`AttributeError`-free assertions on empty tuples — the contracts genuinely
declared nothing. The route-level gap needed no test to demonstrate: the
reviewed OpenAPI snapshot carried no `offset` parameter on any of the five
time-series operations, and `list_timeseries_observations` passed
`offset=0` as a literal into the envelope it returned.

### What changed

- `ServingContract` gained `latest_order` and `history_order`, and all four
  registered sources declare both. Each is the relation's own unique index
  minus the columns the query pins: BLS `(geo_id, series_id)` /
  `(observation_date, series_id)`; Census ACS `(geo_id, dataset_code,
  vintage_year, variable_code)` / the same led by `observation_date`; FRED
  `(geo_id, series_id)` / `(observation_date, series_id, realtime_start,
  realtime_end)`; Census PEP `(geo_id, observation_date, vintage_year,
  capture_id)` / `(observation_date, vintage_year, capture_id)`.
- `list_latest_observations_for_source` and
  `list_timeseries_observations_for_source` order by the declared tuples.
  PEP is the one whose latest read actually changes: `gold_pep.mv_pep_latest`
  keeps every estimated year of the current vintage, so `ORDER BY geo_id`
  left six-way ties a page boundary could fall inside.
- `build_timeseries_queries` orders the cross-source union by
  `observation_date, as_of_date, dataset_code, vintage_year` and takes an
  `offset`.
- Both time-series routes declare `offset` with the repository's one
  pagination bound (`ge=0, le=100000`), and both services echo it.
- The reviewed OpenAPI snapshot was regenerated with
  `python -m tests.support.regenerate_openapi_contract`. The diff is exactly
  five additive `offset` query parameters — one per time-series operation —
  and nothing else: 55 inserted lines, 0 deleted.
- `apps/web/lib/explorerSources.ts` names `offset` in the offline fallback's
  `timeseriesParameters`, which is a snapshot of the Census time-series
  route's parameters and would otherwise have gone stale.

### Commands

| Command | Result |
| --- | --- |
| `pytest -m "unit and api" tests/unit/api -q` | 289 passed |
| `pytest tests/unit -q` | 1354 passed |
| `python -m tests.support.catalog_evidence` | 295-row register renders; API-074 is `FULL` with five named nodes |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 439 files already formatted |
| `npm --prefix apps/web run test:unit` | 20 files, 220 tests passed |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run build` | succeeded |

### Not run

`make test-integration`, `make test-e2e`, and `make test-web-smoke` need
PostgreSQL/Docker, and this environment has no Docker daemon
(`docker info` fails). The conclusion they would carry is that the declared
order columns exist on the real relations. That was verified instead against
the checked-in DDL, which is where the unique indexes each order was copied
from: `uq_mv_bls_latest`/`uq_rpt_bls_observations_nk`
(`src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql`),
`uq_mv_acs_latest`/`uq_rpt_acs_observations_nk`
(`census_acs/gold_census/DDL/gold_acs.sql`),
`uq_mv_fred_latest`/`uq_rpt_fred_observations_nk`
(`fred/gold_fred/DDL/gold_fred.sql`), and `gold_pep.rpt_pep_observations`,
which selects `revision.capture_id` and is what `gold_pep.mv_pep_latest`
selects `*` from (`census_pep/gold_pep/DDL/gold_pep.sql`).

The order-sensitive assertions in those suites were read rather than run:
`tests/integration/api/test_real_database_contract.py` and the three source
pipelines compare value lists from fixtures whose rows carry distinct
observation dates, and `tests/e2e/test_pep_pipeline.py` looks rows up by
`geo_id`/`observation_date` rather than by index, so no assertion depends on
a tie order that changed.

## Remaining work

None.
