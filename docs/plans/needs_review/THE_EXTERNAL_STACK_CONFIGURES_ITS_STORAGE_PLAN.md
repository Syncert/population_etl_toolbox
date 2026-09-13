---
id: the-external-stack-configures-its-storage
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/deployment -q
  - python -m pytest tests/unit/shared -q
---

# The external-warehouse stack configures the storage its routes need, and its env example is complete

## Plan status

- **Status:** Needs review. Implemented 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `infra/docker/docker-compose.external.yml`,
  `infra/docker/stack.external.env.example`,
  `tests/unit/deployment/`

## Context

`apps/api/appdb.py` treats an unset `APP_API_DATABASE_URL` as "storage not
configured", and documents the consequence: every saved-analysis, account,
and evidence-packet route answers 503. That is the honest answer for a
deployment that chose not to configure storage.

`docker-compose.yml` configures it (line 95). `docker-compose.external.yml`
-- the stack for a warehouse that already exists -- sets `DATABASE_URL` and
`MARTIN_DATABASE_URL` and never sets `APP_API_DATABASE_URL`. Its env example
still declares `APP_API_DB_PASSWORD`, the variable only the other compose
file interpolates. So the external deployment ships with the account and
stored-work routes dead, the password it asks the operator for unused, and
nothing that says so.

## Findings

- `infra/docker/stack.external.env.example` also lacks `CENSUS_API_KEY`,
  `BLS_API_KEY`, `FRED_API_KEY`, and `FBI_CDE_API_KEY`, which
  `stack.env.example` declares. `docker-compose.external.yml` interpolates
  `${FBI_CDE_API_KEY:-}`, so the key resolves to empty and the FBI DAG
  fails at request time rather than at compose time.
- No test reads either `*.env.example` (`grep stack.env.example tests/`
  finds nothing), so a compose file and its example can drift in both
  directions.
- `docker-compose.smoke.yml` leaves `APP_API_DATABASE_URL` unset **on
  purpose** and says so in a comment. That is the distinction the guard
  must keep: an omission stated is a choice; an omission unstated is a
  defect.

## Acceptance criteria

1. `docker-compose.external.yml` configures `APP_API_DATABASE_URL` for the
   `api` service the way `docker-compose.yml` does, from the same
   `APP_API_DB_PASSWORD` and the external warehouse's host, port, and
   database, or the file states in a comment that the routes are
   intentionally unconfigured and the env example stops asking for the
   password. The first is the expected answer; the plan records the choice.
2. `stack.external.env.example` declares every variable
   `docker-compose.external.yml` interpolates, and nothing it does not.
3. A guard in `tests/unit/deployment/` reads each compose file and its
   paired env example and fails, naming the variable and the direction, on
   any `${VAR}` reference absent from the example or any example key no
   compose file reads. The intentional smoke-stack omission stays allowed
   by the comment convention the guard reads, not by an exclusion list.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free
   `DEPLOY-` identifier; DEPLOY-006 at authoring time).

## Non-goals

- Provisioning the `api_app_writer` role on an external warehouse. That is
  the operator's bootstrap step and is documented in
  `BETA_RESET_REINGESTION.md`; the compose file only has to ask for it.

## Validation

- **Criterion 1 — the answer taken was the expected one.**
  `docker-compose.external.yml` now sets `APP_API_DATABASE_URL` for the `api`
  service, from `APP_API_DB_PASSWORD` and the external warehouse's
  `ANALYTICS_DB_HOST`/`PORT`/`NAME`, in the same shape
  `docker-compose.yml` uses. Two details are deliberate and recorded here
  because a reviewer would otherwise have to guess:
  - the host/port/database references are the bare `${ANALYTICS_DB_*}` this
    file already uses for `DATABASE_URL`, not the internal file's
    `analytics_postgres` defaults. An external stack has no compose-network
    Postgres to fall back to, so a default there would name a host that does
    not exist.
  - `${APP_API_DB_PASSWORD:-api_app_writer}` keeps the internal file's
    default rather than becoming `:?`-required. Making it required would stop
    the stack for an operator who chose not to configure application storage
    at all, which `apps/api/appdb.py` supports and answers 503 for; the
    example already ships the same placeholder and points at
    `scripts/provision_app_api.py`.
- **A second defect in the same family, found while reading the file.**
  `docker-compose.external.yml` passed `CDC_SOCRATA_APP_TOKEN`,
  `FBI_CDE_API_KEY`, and `USDA_NASS_API_KEY` into the Airflow containers and
  **not** `CENSUS_API_KEY`, `BLS_API_KEY`, or `FRED_API_KEY`.
  `infra/airflow/airflow.env.example`, the `env_file` both stacks read, holds
  only `AIRFLOW__CORE__DAGS_FOLDER`, `PYTHONPATH`, and
  `AIRFLOW__CORE__LOAD_EXAMPLES`, so on the external stack the Census, ACS,
  BLS, and FRED DAGs authenticated with nothing at all. All three are now
  passed, and the parity guard below is why this cannot come back.
- **Criterion 2.** `stack.external.env.example` gained `CENSUS_API_KEY`,
  `BLS_API_KEY`, `FRED_API_KEY`, `FBI_CDE_API_KEY`, and
  `DATA_QUALITY_COMMIT_SHA`; `APP_API_DB_PASSWORD` is now read by the compose
  file that asks for it. `stack.env.example` gained
  `DATA_QUALITY_COMMIT_SHA`, which `docker-compose.yml` has always
  interpolated and no example ever mentioned.
- **Criterion 3.** `tests/unit/deployment/test_stack_configuration_contracts.py`
  reads each stack and its paired example and fails in both directions,
  naming the variable and the direction. It classifies the three shapes a
  Compose interpolation can take rather than enforcing the literal "every
  variable it interpolates" of criterion 2, and the plan records that
  narrowing:
  - `${VAR}` and `${VAR:?msg}` have no value behind them, so the example must
    declare them or the stack does not come up. (`:?` needed its own handling:
    the text after it is Compose's error message, not a default, and reading
    it as one called `${ANALYTICS_DB_USER:?Set ANALYTICS_DB_USER}` satisfied.)
  - `${VAR:-}` resolves silently to empty and the container then fails at
    request time on a credential nobody was told to set, so the example must
    declare these too. This is the class `FBI_CDE_API_KEY` was in.
  - `${VAR:-something}` is a documented override with a working value behind
    it; the example may declare it but need not. Enforcing the literal
    reading would put the fifteen `ANALYTICS_PG_*` tuning knobs in
    `stack.env.example` in front of an operator who needs five settings.
  - The reverse direction has no exemption at all: every key an example
    declares must be interpolated by its own compose file.
  - Nested references are parsed by matching braces rather than by regex,
    because Compose allows `${ANALYTICS_API_DB_USER:-${ANALYTICS_DB_USER}}`
    and a regex reads the outer default as empty -- calling a working
    fallback a missing value and flagging four variables that are fine.
- **Criterion 3, the smoke distinction.** A separate test derives the stacks
  that serve the API (`grep` for `uvicorn apps.api.main:app` across
  `docker-compose*.yml`, which finds exactly the internal, external, and
  smoke files) and requires each to either set `APP_API_DATABASE_URL` or
  carry a comment naming it followed by `is unset` -- the wording
  `docker-compose.smoke.yml` already uses. No file is named in an exclusion
  list, and the guard asserts it read at least one stack, so a rename that
  makes the derivation find nothing fails rather than passing vacuously.
- **Criterion 4.** `DEPLOY-006` is in `TESTING_CONTRACT.md`, the family range
  line reads `DEPLOY-001–DEPLOY-006`, `AUDITED_COUNTS["DEPLOY"]` is 6, and
  the totals are 402.
- **The guard fails on the code as it was.** Reverting the three files to
  `HEAD` and running the new module gives 4 failed, 2 passed, naming every
  defect the Findings describe and one they did not:
  - `docker-compose.external.yml interpolates these with no usable default
    ... stack.external.env.example never mentions them: ['DATA_QUALITY_COMMIT_SHA',
    'FBI_CDE_API_KEY']`
  - `docker-compose.yml interpolates these with no usable default ...
    stack.env.example never mentions them: ['DATA_QUALITY_COMMIT_SHA']`
  - `stack.external.env.example asks the operator to configure these, and
    docker-compose.external.yml never interpolates them, so setting them
    changes nothing: ['APP_API_DB_PASSWORD']`
  - `these stacks run uvicorn apps.api.main:app without setting
    APP_API_DATABASE_URL and without a comment saying it 'is unset' ...:
    ['docker-compose.external.yml']`
  - and the parity guard, against the old external file: `docker-compose.yml
    alone passes ['BLS_API_KEY', 'CENSUS_API_KEY', 'FRED_API_KEY']`.
- **Each assertion break-tests individually.** Dropping
  `DATA_QUALITY_COMMIT_SHA` from an example, adding an `APP_API_DB_HOSTNAME`
  key no stack reads, declaring `ANALYTICS_DB_PORT` twice, and removing
  `APP_API_DATABASE_URL` from the external stack each fail exactly one test,
  naming the variable.
- **Compose itself agrees, not just the YAML parser.** `docker compose
  --env-file <the example> -f docker-compose.external.yml config` renders
  with no warnings and resolves
  `APP_API_DATABASE_URL: postgresql+psycopg2://api_app_writer:api_app_writer@your-analytics-postgres-host:5432/your_analytics_db`
  plus the three new empty source keys; the internal stack renders clean
  against its own example too.
- `infra/docker/README.md` now runs `scripts/provision_app_api.py
  --env-file infra/docker/stack.external.env --apply-schema` beside the
  read-only provisioning step in both external recipes, because the stack now
  expects that role to exist. The script reads `ANALYTICS_DB_*` and
  `APP_API_DB_PASSWORD` from exactly that file.
- `pytest tests/unit` 1571 passed. `pytest tests/unit/deployment` 10 passed.
  `ruff check .` and `ruff format --check .` clean.

## Remaining work

- None. Provisioning `api_app_writer` on a real external warehouse stays the
  operator's step, as the non-goals state; the compose file and the README now
  ask for it.
