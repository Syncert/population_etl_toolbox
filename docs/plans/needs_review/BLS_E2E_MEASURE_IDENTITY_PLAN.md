---
id: bls-e2e-measure-identity
branch: fix/bls-e2e-measure-identity
depends_on:
  - bls-laus-measure-metrics
parallel_safe: true
complexity: low
verify:
  - ./tests/run.ps1 e2e
  - ./tests/run.ps1 unit
---

# BLS end-to-end node reads the LAUS measure identity

## Plan status

- **Status:** Implemented and verified; ready for review
- **Last updated:** 2026-09-12
- **Source owner:** U.S. Bureau of Labor Statistics, LAUS program; test tier only
- **Depends on:** `needs_review/BLS_LAUS_MEASURE_METRICS_PLAN.md` (integrated
  on `main` through PR #37). No warehouse, API, or web object changes here.

## Implementation checkpoint

**Last updated:** 2026-09-12

**Current milestone:** none outstanding

**Next pickup:** review.

### Checklist

- [x] BLE-001 the BLS product e2e node asserts the published measure identity
- [x] BLE-002 contract row and evidence

## Objective

`tests/e2e/test_census_bls_pipeline.py::test_bls_fixture_flows_raw_to_gold_and_replays_identically`
is the registered end-to-end owner of the `bls.labor_series` product (E2E-002,
E2E-004, and the E2E-008 inventory). It fails on `main` since the LAUS measure
plan merged, and the ACS catalog plan's validation record names it as a
pre-existing failure. The pipeline is correct; the node is asserting the
identity the LAUS plan deliberately retired.

## Evidence gathered 2026-09-12

Reproduced on this host against the disposable test warehouse
(`population-testing-postgres-1`, PostGIS 16 on `127.0.0.1:55532`), from
`fix/acs-catalog-metric-code` at `6eaf3b2`:

```
FAILED tests/e2e/test_census_bls_pipeline.py::test_bls_fixture_flows_raw_to_gold_and_replays_identically
  assert source.json()["total"] == common.json()["total"] == 1
  assert 0 == 1
```

- The fixture seeds `raw_bls.bls_series` with program `la`, measure `03`,
  series `LAUST970000000000003`, and reads the API under
  `BLS:LAUST970000000000003`.
- `gold_bls.refresh_rpt_bls_observations` writes
  `COALESCE('BLS:' || bm.metric_key, 'BLS:' || bs.series_id)` joined through
  `gold_bls.dim_bls_measure` on `(program_code, measure_code)`, and
  `refresh_bls_elements` seeds that dimension with the seven LAUS measures. An
  LA/03 row is therefore served as `BLS:LAU:UNEMP_RATE`, and the series-shaped
  code answers nothing. This is Decision 1 of the LAUS plan (retire the
  series-level LAUS codes, do not dual-publish) working as designed.
- The other e2e nodes in the file and tier are unaffected; the ACS plan's
  record reads 8 passed, 1 failed for `-m "e2e and not martin"`.

## Decisions

1. **Fix the test, not the warehouse.** The LAUS plan's identity is reviewed
   and integrated; the node must read what the warehouse promises. Changing
   the fixture to a non-LA program would keep the node green but stop it
   exercising the only BLS program with a measure-level identity.
2. **Pin the latest reads to the fixture geography.** The measure code spans
   every LAUS geography, so `/observations/latest` and the source-scoped
   latest read are filtered by `state_fips=97`; the timeseries read was
   already scoped by `geo_id`. Without this the node would depend on no other
   suite having seeded an LA row, which the combined product run (E2E-012)
   does not guarantee.
3. **Assert the lineage column.** The LAUS plan promises `series_id` on
   every served row (its Decision 5). The node now reads
   `gold_bls.mv_bls_latest` for the fixture geography and requires exactly
   one row carrying the measure code and the fixture series id, so a future
   refresh that drops lineage or dual-publishes fails here. The evidence
   section records why this is a relation read rather than a neutral API
   read.
4. **Drop the catalog delete from teardown.** The node never harvests, and the
   measure code is a shared catalog row another suite may legitimately have
   published; `warehouse_scope` already owns harvested-row cleanup for nodes
   that do harvest.

## Deliverables

### BLE-001 — the node asserts the published measure identity

- `metric_code` is `BLS:LAU:UNEMP_RATE`; `series_id` stays the seeded series.
- The source timeseries and common latest reads answer exactly one row for
  the fixture state; the served `metric_code` is the measure code, the served
  `geo_level` is `STATE`, and the served row's `series_id` is the series.
- Replay (E2E-004) and revision (E2E-005 behaviour in this node) assertions
  unchanged in intent.

### BLE-002 — contract row and evidence

- `docs/reference/TESTING_CONTRACT.md` E2E-002 names the measure identity and
  the lineage column. `CI_EVIDENCE_MAP.md` needs no change: the row still
  belongs to the e2e job.
- This plan's evidence section carries the exact commands and results.

## Non-goals

- No change to `gold_bls`, `apps/api`, `apps/web`, or the fixture JSON.
- No change to the LAUS plan's evidence; its BLM-005 API evidence already
  covers the measure code on the development stack.

## Implementation evidence

All runs on this host on 2026-09-12 against the disposable test warehouse
(`population-testing-postgres-1`, PostGIS 16 on `127.0.0.1:55532`, Redis on
`127.0.0.1:56379/15`), with `--basetemp` pointed at a scratch directory.

### BLE-001 — the node asserts the published measure identity

- `tests/e2e/test_census_bls_pipeline.py::test_bls_fixture_flows_raw_to_gold_and_replays_identically`
  reads `BLS:LAU:UNEMP_RATE`. The source timeseries read (scoped by
  `geo_id=state:97`) and the common latest read (scoped by `state_fips=97`)
  both answer `total: 1`, value `4.5`, `metric_code` equal to the measure code,
  `geo_level: STATE`; both replays produce identical JSON; the revised value
  `5.25` is served on the third pass with `total: 1`.
- Lineage: `SELECT metric_code, series_id FROM gold_bls.mv_bls_latest WHERE
  geo_id = 'state:97'` returns exactly `('BLS:LAU:UNEMP_RATE',
  'LAUST970000000000003')`.
- **Why the lineage assertion reads the relation and not the neutral
  resource.** A first revision read `GET /api/v1/observations?metric_code=…`
  to assert `dimensions.series_id`, and it answered 404. The neutral route
  resolves a metric through the glossary catalog before dispatching
  (`resolve_metric` in `apps/api/services/neutral_observations_service.py`),
  and this node never harvests its publisher, so the code is unknown to the
  catalog on the scratch warehouse. The registered neutral route for the
  `bls.labor_series` product is `/api/v1/observations/latest` only, which
  answers from the union relation without a catalog lookup, so the node stays
  within its registered routes and reads lineage from the served relation.
  `dimensions.series_id` on the neutral resource is already pinned by the API
  unit tier (`tests/unit/api/test_neutral_observations.py`).
- Teardown no longer deletes from `gold_glossary.dim_metric_catalog`.

### BLE-002 — contract row and evidence

- `docs/reference/TESTING_CONTRACT.md` E2E-002 now names the measure code,
  the common and source reads, and the lineage column. The catalog
  evidence audit (`tests/unit/shared/test_catalog_evidence.py`) and the
  product coverage inventory (E2E-008) still pass, so the register total and
  the node's registration are unchanged.

### Commands

| Command | Result |
| --- | --- |
| `pytest -m e2e tests/e2e/test_census_bls_pipeline.py::test_bls_fixture_flows_raw_to_gold_and_replays_identically` before the change | 1 failed (`assert 0 == 1`) |
| same, after the change | 1 passed |
| `pytest -m "e2e and not martin" tests/e2e` | **9 passed** in 46.5s (the ACS plan's record for the same selection was 8 passed, 1 failed) |
| `pytest tests/unit/shared/test_catalog_evidence.py tests/unit/shared/test_data_product_e2e_coverage.py tests/unit/api/test_data_product_api_coverage.py tests/unit/shared/test_repository_hygiene.py` | 21 passed |
| `pytest tests/unit` | 1315 passed |
| `ruff check` and `ruff format --check` on the test file | clean |

The `martin` e2e node was not run: it needs the Martin tile service, and this
change touches nothing it reads.
