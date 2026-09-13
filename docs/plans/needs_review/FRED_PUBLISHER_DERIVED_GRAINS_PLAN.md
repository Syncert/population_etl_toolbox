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

- **Status:** Ready for review. Claimed and delivered 2026-09-12. Both
  remaining declared-grain publishers — FRED and the BLS series arm — now
  read their grains from served rows, and a static guard (ARC-006) makes the
  rule mechanical rather than remembered.
- **Last updated:** 2026-09-12
- **Owner surface:** `src/data_ingestion_toolbox/fred/gold_fred/DDL/publisher.sql`,
  `src/data_ingestion_toolbox/bls/gold_bls/DDL/publisher.sql`,
  `tests/unit/shared/test_catalog_serving_identity.py`,
  `tests/integration/database/test_fred_silver_flow.py`,
  `tests/integration/api/test_catalog_serving_agreement.py`
- **Depends on:** `catalog-grain-vocabulary` (the vocabulary function and
  DB-028) — satisfied; merged to `main` in `a74c43f`, plan in `needs_review/`.

## Context

`gold_fred.metric_publisher` published `ARRAY['NATIONAL']::TEXT[] AS
valid_geo_grains` for every series. Every FRED series served today is
national, so the declaration was correct — but it was correct the way the ACS
declaration was correct on the day it was written, and the ACS declaration was
found on 2026-09-12 to advertise 2,487 metric/grain pairs nothing served. A
declared grain is a claim about the future; a derived grain is a fact about the
rows.

DB-028 would catch a FRED series that stopped serving national rows, but it
would catch it as a catalog defect after the fact. Deriving removes the
category: a series with no served rows publishes no grain, and the agreement
guards report a current code with nothing behind it rather than a grain the
publisher invented.

FRED is also the source most likely to gain a non-national series (regional
FRED series exist and are one configuration entry away), and the first such
series would have been published as `NATIONAL`.

## Objective

`gold_fred.metric_publisher` derives `valid_geo_grains` from the relation the
API serves, with an empty array for a series nothing serves.

## Scope, as delivered

The plan's acceptance criterion is repository-wide — *no* publisher declares
grains from configuration — and one other publisher still did. **BLS is
therefore in scope and was changed too.** Its LAUS measure arm already derived
grains; its series arm mapped `dim_bls_series.geographic_level`, a configured
attribute, through a `CASE` whose `ELSE` published `NATIONAL`. So an
unrecognised or absent level was published as national, and a series serving
nothing at all was published as serving the nation. Both are the same defect
class as FRED's, in the same acceptance criterion, and leaving one would have
made the criterion unmeetable.

| Item | Where |
| --- | --- |
| FRED derives from `gold_fred.mv_fred_latest`, joined on the identity the dispatch entry serves (`'FRED:' \|\| series_id`), `COALESCE`d to the empty array | `fred/gold_fred/DDL/publisher.sql` |
| BLS's series arm derives from `fact_bls_observation.geo_level`, `ARRAY_REMOVE`d so an unserved series publishes `{}` rather than `{NULL}` | `bls/gold_bls/DDL/publisher.sql` |
| The rule is mechanical: no `metric_publisher` view assigns a string literal to `valid_geo_grains` | `tests/unit/shared/test_catalog_serving_identity.py` (ARC-006) |
| FRED proves it end to end: two series seeded, one served, harvest after the serving refresh | `tests/integration/database/test_fred_silver_flow.py` (ARC-006) |
| DB-028 exercises FRED, not only ACS | `tests/integration/api/test_catalog_serving_agreement.py` (`published_fred_metric`) |
| ARC-006 is a catalog row with CI ownership | `docs/reference/TESTING_CONTRACT.md`, `docs/reference/CI_EVIDENCE_MAP.md`, `tests/support/catalog_evidence.py` |

### The ordering this makes load-bearing

A derived grain is read at harvest time, so **the glossary harvest must run
after the serving refresh** or it publishes the empty array for everything.
The ingest DAG already sequences `emit_fred_publisher_ready` downstream of
`refresh_gold_fred_serving_layer`, so production was already right; the
integration test states the order explicitly and the publisher view says why.

`gold_glossary.geo_grain` is not used here. It is created in the glossary
phase, after both publisher files run at bootstrap
(`sql/bootstrap/warehouse_manifest.json`: FRED's publisher is asset 147,
migration 018 is 167), so a view referencing it would fail to create. Both
relations already write the vocabulary word directly — FRED's refresh writes
the literal `'NATIONAL'`, and BLS's gold refresh normalises `us`/`state`/
`county` — so `UPPER(...)` is the same answer, with the same comment the ACS
publisher carries.

## Acceptance

- [x] No publisher declares `valid_geo_grains` from configuration. Enforced
      executably by ARC-006 rather than by a grep: the guard reads every
      `metric_publisher` body with comments stripped and fails on a string
      literal in the grain expression. Reverting either publisher fails it
      with both files named.
- [x] DB-028 passes with FRED exercised (`published_fred_metric`, asserting
      `@NATIONAL` present and `@STATE`/`@COUNTY` absent — the one row seeded).

## Validation

| Check | Command | Result |
| --- | --- | --- |
| Unit tier | `python -m pytest tests/unit --basetemp=…` | 1341 passed |
| Integration tier | `RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_*=… python -m pytest -m "integration and not e2e" tests/integration` | **139 passed, 2 skipped** (4:12) on a force-recreated warehouse |
| Gap, static | ARC-006 against the pre-change publishers | fails, naming `gold_bls` (`'COUNTY'`, `'STATE'`, `'NATIONAL'`) and `gold_fred` (`'NATIONAL'`) |
| Gap, database | the new FRED test against the pre-change publisher | fails: `a series nothing serves published ['NATIONAL']` |
| Lint | `ruff check .`, `ruff format --check .` | clean (see the separate formatting commit) |

The two skips are declared and environment-gated: the production-DAG import
(CI's `postgres-integration` job installs `.[airflow-dev]`) and Martin
(`RUN_MARTIN_TESTS=1` plus the Compose stack).

An earlier integration run reported one failure in
`test_source_quality_checks.py::test_reference_and_registry_defects_fail`
(`UniqueViolation` on a `CENSUS_ACS` publisher_registry row). It was residue:
a teardown of this branch's new fixture had aborted mid-transaction and rolled
back its own registration cleanup. On a force-recreated warehouse the tier is
green, and the fixture's cleanup was fixed and re-run twice to prove it.

## Evidence on the development warehouse

Applied both publisher views to `docker-analytics_postgres-1` and measured.

| | FRED | BLS |
| --- | --- | --- |
| Rows published | 24 series | 63 (LAUS publishes per measure) |
| View read, warm, before | 11 ms | 716 / 656 ms |
| View read, warm, after | 20 ms | 681 / 636 ms |
| Forced harvest | 24 rows in 69 ms | not re-harvested |
| Derived vs published today | identical: 24 × `{NATIONAL}` | identical: 56 × `{NATIONAL}`, 4 × `{COUNTY,STATE}`, 3 × `{STATE}` |

The harvest's content fingerprint covers `valid_geo_grains`, so an unforced
harvest **skipped** — correctly, because the derived answer equals the declared
one on today's data. That is the whole argument for the change in one
measurement: nothing published changes, and nothing published can now drift
from what is served. The forced run confirms the derivation reaches the catalog.

DB-028's sweep, run against the development API rather than the test client
(the disposable warehouse is the only one tests may write to): 24 current FRED
codes, 24 metric/grain pairs, **0 unanswered, 0 off-vocabulary**.

## Non-goals

Adding non-national FRED series; changing FRED's dispatch entry. Routing either
publisher through `gold_glossary.geo_grain`, which the bootstrap order forbids
and neither relation needs.
