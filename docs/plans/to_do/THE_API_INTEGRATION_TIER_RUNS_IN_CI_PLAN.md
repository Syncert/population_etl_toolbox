---
id: the-api-integration-tier-runs-in-ci
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# The API integration tier is run by a required workflow, and the map says which

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present gap:
  a tier this branch has been adding guards to is not run by CI.**
- **Last updated:** 2026-09-13
- **Owner surface:** `.github/workflows/`, `tests/support/ci_evidence_manifest.json`,
  `tests/unit/shared/test_ci_evidence_manifest.py`,
  `docs/reference/CI_EVIDENCE_MAP.md`, `docs/user-guides/RUNNING_TESTS.md`

## Context

`tests/run.ps1` and `TESTING_CONTRACT.md` define the integration tier as
`-m "integration and not e2e" tests/integration`. No workflow runs that. Each
required workflow runs one subdirectory:

| Workflow | Directory it runs |
|---|---|
| `postgres-integration` | `tests/integration/database` |
| `redis-integration` | `tests/integration/redis` |
| `martin-integration` | `tests/integration/martin` |
| `deployment-smoke` | `tests/integration/deployment` |
| `e2e-performance` (schedule only) | five named files under `tests/integration/api` |

`tests/integration/api` has eleven files. Four of them are run by **no
workflow at all**, scheduled or otherwise:

- `test_catalog_serving_agreement.py` -- the DB-030..DB-034 sweeps, ARC-005,
  the source-route grain and code sweeps this branch built;
- `test_evidence_packet_contract.py` -- DB-027, ADR-0004;
- `test_request_snapshot.py` -- API-100, added on this branch;
- `test_stored_work_listing.py` -- API-103, added on this branch.

`CI_EVIDENCE_MAP.md` says otherwise. Its rows for evidence packets and for
catalog/serving agreement state that those files "ride `api-unit`,
`postgres-integration`, and `frontend`". They ride nothing. Eleven plan
frontmatters on this branch verify with
`pytest tests/integration/api -m "integration and database ..."`, and every
one of those plans recorded a green local run believing CI would repeat it.

## Findings

- `postgres-integration` cannot simply widen its path: it installs
  `.[airflow-dev]`, and `TESTING_CONTRACT.md` records that Airflow and the
  API cannot share one environment (SQLAlchemy 1.4 versus 2.x). The
  workflows that already carry `.[api,dev]` **and** a PostgreSQL service
  are `e2e` (Postgres and Redis, per push) and `coverage` (Postgres).
- `RUNNING_TESTS.md` compounds it. Under `# postgres-integration` it
  documents `pytest tests/integration/api -m "integration and database and
  not slow"` -- a scope the job does not have -- and under
  `# redis-integration` it documents
  `tests/integration/redis tests/integration/api -m "integration and (redis
  or database) and not slow"` while the job runs
  `tests/integration/redis -m "integration and api and redis"`. The guide
  promises "the marker expression its CI job uses, so a local result means
  what a CI result means". ENV-013's guard
  (`test_repository_hygiene.py::test_the_guide_documents_a_path_that_needs_no_container_runtime`)
  asserts one expression against one workflow, so the Redis half diverged
  unnoticed.
- The local run of the four unrun files against a bootstrapped PostGIS is
  recorded below once it completes; a red result there is a second finding,
  not a reason to narrow this plan.

## Acceptance criteria

1. A **required** (push and pull request) workflow runs
   `tests/integration/api -m "integration and database and not slow"` in an
   environment that carries the API dependencies and a PostgreSQL service --
   a new job in `e2e.yml`, a new workflow, or `coverage`; the plan records
   the choice and why. The Redis-marked file stays with the tier that has
   Redis.
2. `tests/unit/shared/test_ci_evidence_manifest.py` derives, from the
   `run:` steps of every workflow the manifest marks required, the set of
   `tests/**` directories CI executes, and fails naming any directory under
   `tests/integration` that no required job invokes. Derived from the
   workflows, not a restated list, so the next unrun directory fails on its
   own.
3. `CI_EVIDENCE_MAP.md` names the job that actually runs each file it
   cites, and its executable mirror agrees.
4. `RUNNING_TESTS.md` documents, for every required job, the exact
   directory and marker expression that job runs, and ENV-013's guard
   iterates the manifest's required entries instead of hard-coding
   `postgres-integration.yml`.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `ENV-`
   identifier; ENV-015 at authoring time).

## Non-goals

- Making `postgres-integration` install the API. The environment split is
  a documented contract.
- Moving the scheduled-only files (`test_connection_capacity.py`,
  `test_cache_real_services.py`, the real-database and NASS contracts) into
  the per-push tier; they are slow or need services by design.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
