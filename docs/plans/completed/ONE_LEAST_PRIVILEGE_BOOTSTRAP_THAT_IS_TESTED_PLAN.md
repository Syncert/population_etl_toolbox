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

- **Status:** Ready for review. All four deliverables are implemented and every
  acceptance criterion was run on a machine session on 2026-09-18 against the
  pinned disposable PostGIS 16 container, including the failing-first proof.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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

- [x] `001_api_readonly.sql` contains no database-name or owner-role literal
      and applies cleanly to the disposable test database under its own name
      and owner -- `population_etl_test` owned by `population_test`.
- [x] `scripts/provision_api_readonly.py` no longer carries its own `GRANT`
      statements: zero remain, and it applies the file instead.
- [x] Rendering `docker-compose.external.yml` without `ANALYTICS_API_DB_USER`
      fails with the variable named, for both the API and Martin services.
- [x] The privilege test passes on the bootstrapped warehouse and fails when
      one grant is removed; proven by commenting the SELECT grant and
      rebuilding the warehouse from empty.
- [x] Every new test carries a `Covers:` label; DB-050 and DEPLOY-012 added,
      and `CI_EVIDENCE_MAP.md` names the owning paths.

## Implementation evidence

### The bootstrap derives what it used to hardcode

`001_api_readonly.sql` named `population_etl` and `analytics` as literals,
which is why a second implementation existed: it could not be applied to the
external deployment, whose owner is `airflow_admin` and whose database is named
per deployment, nor to the disposable test database. Both are now derived --
the database from `current_database()`, the owner from `pg_database.datdba` --
in the style `002_app_api.sql` already used and for the reason its own comment
gives.

**The owner derivation is the part that was quietly broken, not just
inconvenient.** `ALTER DEFAULT PRIVILEGES` is recorded *per granting role*.
Recorded against `analytics` on a warehouse whose relations are created by
someone else, it applies to nothing -- silently -- and the next relation a
transform creates is unreadable by the API. Applying the rewritten file to the
disposable database records the default privileges against `population_test`,
which is the identity that will actually create those relations.

### One policy, one file

`provision_api_readonly.py` carried a second copy of the grants, and the two
had already drifted: the SQL file granted `SELECT ON ALL SEQUENCES` and a
sequence default privilege, and the script granted neither. The script now
applies the file and owns only what a checked-in file must not carry -- a
password -- plus role membership. Zero `GRANT` statements remain in it.

`api_reader` is the policy role. A deployment naming its login role something
else through `ANALYTICS_API_DB_USER` gets one that is a *member* of it rather
than a second role someone must remember to grant the same things to. Verified
on the disposable database: `serving_reader` is created, joined to
`api_reader`, and carries `default_transaction_read_only` itself -- set
explicitly, because a role-level setting is not inherited through membership.

### The external stack stops defaulting to the owner

`${ANALYTICS_API_DB_USER:-${ANALYTICS_DB_USER}}` meant a deployment that set
every other variable and forgot this one ran its public API and tile server as
the ETL owner, with write access to every schema, and came up cleanly. Both
services now require the variable with `:?`. Compose refuses the render and
names it:

```text
ANALYTICS_API_DB_USER is missing a value: Set ANALYTICS_API_DB_USER to the
read-only serving role; it must not fall back to the warehouse owner
```

### The privileges are asserted in both directions, and in a session

Four tests. The catalog half reads `has_table_privilege` over every relation in
`ALLOWED_OBSERVATION_RELATIONS` -- whose own comment says it exists "for the
privilege and allowlist assertions", and until now there was no privilege
assertion -- and over every `raw_*`, `silver_*`, `control` and `app_api`
relation the warehouse holds. The forbidden set is read from
`information_schema` rather than listed, so a schema a later migration adds is
covered the day it exists. Both directions carry a floor assertion, because a
sweep over an empty set passes.

The fourth connects *as* the role, because a privilege catalog and a session
are not the same claim: schema `USAGE`, `default_transaction_read_only` and
role membership all sit between them. A served relation answers, a silver
relation raises `insufficient_privilege`, and a write raises
`read_only_sql_transaction`.

**Failing-first.** Commenting out the `GRANT SELECT ON ALL TABLES` line and
rebuilding the warehouse from empty fails two of the four -- the catalog sweep
and the session read -- and leaves the two negative-space tests passing, which
is the right shape: removing a grant cannot make the role reach *more*.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/deployment tests/unit/api/test_serving_registry.py -q` | 87 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_api_reader_privileges.py -q` | 4 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 188 passed, 2 skipped (was 184, 2) |
| `python -m pytest tests/unit -q` | 1869 passed |
| `ruff format --check .` / `ruff check .` | clean, 496 files |

The whole database tier is run, not just the new file: the rewritten bootstrap
is applied by fixtures across that tier.

## Definition of done

There is one reviewed statement of what the API may read, it applies to
every deployment shape the repository documents, and a change that widens it
turns a required job red.

## What this plan deliberately does not do

- It does not add row-level security; the warehouse holds public data and
  `app_api` isolation is by owner key and role (DB-027).
- It does not change the API's own relation allowlist or how the registry
  chooses relations.
