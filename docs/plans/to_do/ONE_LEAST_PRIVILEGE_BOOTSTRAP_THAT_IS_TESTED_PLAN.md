---
id: api-reader-privileges
branch: claude/api-reader-privileges
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/deployment tests/unit/api/test_serving_registry.py -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_api_reader_privileges.py -q
  - ruff format --check . ; ruff check .
---

# One least-privilege bootstrap, and a test that it is one

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

The API and Martin read the warehouse as `api_reader`, a role that is meant to
see gold and nothing else. The policy exists in two copies and is tested in
none.

- `sql/bootstrap/001_api_readonly.sql:12` runs `GRANT CONNECT ON DATABASE
  population_etl TO api_reader;` and `:14` `CREATE SCHEMA IF NOT EXISTS gold
  AUTHORIZATION analytics;`, with `ALTER DEFAULT PRIVILEGES FOR ROLE analytics`
  at `:24` and `:28`. The database name and owner role are literals. The
  sibling file `sql/bootstrap/002_app_api.sql:23-27` uses `current_database()`
  precisely so that "one reviewed bootstrap serves the Compose stack, an
  externally hosted deployment, and the disposable test database". The
  external deployment documented in `docs/reference/BETA_RESET_REINGESTION.md`
  §2–§3 names its owner `airflow_admin` and its database as
  deployment-specific, so `001` cannot be applied there as written.
- `scripts/provision_api_readonly.py:52-120` is a second, parameterised
  implementation of the same grants. Two copies of one policy drift.
- `infra/docker/docker-compose.external.yml:18` falls back to the warehouse
  owner when the API role is unset:
  `${ANALYTICS_API_DB_USER:-${ANALYTICS_DB_USER}}`. Martin does the same at
  `:57`. A deployment that skips one variable runs the public API as the ETL
  owner, with write access to every schema.
- `apps/api/registry.py:913-925` defines `ALLOWED_OBSERVATION_RELATIONS` "for
  the privilege and allowlist assertions", and no privilege assertion exists:
  `api_reader`, `001_api_readonly`, and `has_table_privilege` appear nowhere
  under `tests/`. Every integration test connects as the owner
  (`tests/support/postgres.py`). `docs/reference/CI_EVIDENCE_MAP.md` never
  mentions the file.

## Deliverables

### 1. One bootstrap, deployment-neutral

Rewrite `001_api_readonly.sql` in the style of `002_app_api.sql`: derive the
database from `current_database()` and the owning role from the database
owner (`pg_database.datdba`) or `current_user`, and keep the schema list as
the one place it is written. `scripts/provision_api_readonly.py` becomes a
thin runner that applies that file (password handling stays in the script;
grants do not).

### 2. The external stack does not default to the owner

In `docker-compose.external.yml`, `ANALYTICS_API_DB_USER` and its password
are required (`:?Set ...`) for the API and Martin services, matching how the
same file already requires the host and database name. `stack.env.example`
and the deployment guide name the variables.

### 3. The privileges are asserted

`tests/integration/database/test_api_reader_privileges.py`: apply the
bootstrap to the disposable warehouse, then for every relation in
`ALLOWED_OBSERVATION_RELATIONS` plus the catalog relations the registry
names, assert `has_table_privilege('api_reader', relation, 'SELECT')` and not
`INSERT`, `UPDATE` or `DELETE`; assert `api_reader` holds no privilege on any
`silver_*`, `raw_capture`, `control` or `app_api` relation; assert
`api_app_writer` (from `002`) can write only `app_api`. Then connect *as*
`api_reader` and confirm one served relation answers and one silver relation
raises `insufficient_privilege`.

### 4. CI owns it

Add the test to the `postgres-integration` job's paths and to
`CI_EVIDENCE_MAP.md`; add a `DB-` catalog row in
`docs/reference/TESTING_CONTRACT.md` and a `DEPLOY-` row for the Compose
requirement, with the unit test for the latter in `tests/unit/deployment`.

## Acceptance criteria

- [ ] `001_api_readonly.sql` contains no database-name or owner-role literal
      and applies cleanly to the disposable test database under its own
      name and owner.
- [ ] `scripts/provision_api_readonly.py` no longer carries its own `GRANT`
      statements.
- [ ] Rendering `docker-compose.external.yml` without `ANALYTICS_API_DB_USER`
      fails with the variable named, for both the API and Martin services.
- [ ] The privilege test passes on the bootstrapped warehouse and fails when
      one grant is removed (prove failing-first by commenting a grant).
- [ ] Every new test carries a `Covers:` label; `CI_EVIDENCE_MAP.md` and its
      executable mirror name the new owning paths.

## Definition of done

There is one reviewed statement of what the API may read, it applies to
every deployment shape the repository documents, and a change that widens it
turns a required job red.

## What this plan deliberately does not do

- It does not add row-level security; the warehouse holds public data and
  `app_api` isolation is by owner key and role (DB-027).
- It does not change the API's own relation allowlist or how the registry
  chooses relations.
