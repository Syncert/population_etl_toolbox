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

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row ENV-015.)
- **Last updated:** 2026-09-14
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

- **The choice: a new required workflow, `api-integration.yml`, job
  `api-database`.** Not `postgres-integration` (it installs
  `.[airflow-dev]`, and Airflow 2.9 pins SQLAlchemy 1.4 against the API's
  2.x — the environment split `TESTING_CONTRACT.md` records); not `e2e` or
  `coverage`, because a failure there would read as an end-to-end or a
  coverage failure rather than as this tier's. It carries `.[api,dev]`, a
  PostGIS 16 service and a Redis 7 service — the tier's own expression
  selects one Redis-marked file, and the service is what makes a local result
  mean what a CI result means — and runs exactly
  `pytest tests/integration/api -m "integration and database and not slow"`,
  37 of 42 collected nodes locally. Push (every plan branch prefix) and
  pull request, `workflow_dispatch`, and registered in
  `tests/support/ci_evidence_manifest.json`, which takes the required list
  from 15 jobs to 16.
- Three guards, each derived from the workflows rather than restating them:
  - `test_ci_evidence_manifest.py::test_every_integration_directory_is_run_by_a_required_job`
    reads the `run:` steps of every job the manifest marks required, collects
    the `tests/` directories they invoke, and fails naming any directory
    under `tests/integration` no required job runs. The next unrun directory
    fails on its own.
  - `...::test_the_evidence_map_names_the_job_that_runs_each_file_it_cites`
    checks each `CI_EVIDENCE_MAP.md` row's cited `tests/integration` files
    against the jobs the row names — a directory a job runs covers everything
    under it, because pytest recurses.
  - `test_repository_hygiene.py::test_the_guide_documents_a_path_that_needs_no_container_runtime`
    (ENV-013) now iterates the manifest's required entries, extracts each
    pytest invocation's paths and marker expression from the workflow, and
    requires `RUNNING_TESTS.md` to document them under a block named for that
    workflow. It previously compared one expression against
    `postgres-integration.yml`, which is how the Redis block came to document
    a scope its job never had.
- `CI_EVIDENCE_MAP.md` gains an authoritative row for the new job, the four
  rows that claimed `tests/integration/api` files rode other jobs now name
  `api-integration`, the scheduled bounded-E2E row says which of its files
  also ride the new job per change, and the branch-protection paragraph says
  sixteen jobs and why the sixteenth exists.
- `RUNNING_TESTS.md` documents one block per required service-backed job —
  `postgres-integration`, `api-integration`, `redis-integration`, and
  `coverage` (which runs the database tier again for the ratchet) — with the
  exact directory and expression each runs, and says why the API path was
  documented under a job that cannot run it.
- The four previously unrun files pass locally against a bootstrapped PostGIS
  (they are part of the 159-node integration run this branch records), so
  criterion's "a red result there is a second finding" did not arise.
- Break-tests: removing the `api-integration` entry from the required
  manifest leaves `2 failed, 18 passed` — the unrun-directory guard and the
  evidence-map guard; changing the guide's documented expression for the new
  job fails ENV-013's guard with "documents a different marker expression
  than 'integration and database and not slow'".
- Tiers: `pytest tests/unit` 1560 passed; `ruff format --check .` and
  `ruff check .` clean. The new workflow's first real run is this push.

- **Confirmed green on its first run.** `api-integration` run #1 on
  `4cf94d8` concluded `success`
  ([34769260097](https://github.com/Syncert/population_etl_toolbox/actions/runs/34769260097)),
  so the four files nothing had ever graded in CI pass there. `e2e` and
  `postgres-integration` are green on the same commit.

## Remaining work

- Ask the repository owner to add the `API integration (PostGIS 16 + Redis 7
  + Python 3.11)` check to branch protection: a required check is only
  required once protection names it, and that setting is outside this
  repository.
