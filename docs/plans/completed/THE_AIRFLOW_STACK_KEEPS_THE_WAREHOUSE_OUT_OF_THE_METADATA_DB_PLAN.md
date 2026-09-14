---
id: the-airflow-stack-keeps-the-warehouse-out-of-the-metadata-db
branch: claude/iterate-plans-improvements-ir885c
depends_on: [the-external-stack-configures-its-storage]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/deployment -q
---

# The Airflow stack points `public_data` at a warehouse, not at Airflow's metadata database

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `infra/docker/docker-compose.airflow.yml`,
  `infra/docker/stack.env.example`, `docs/reference/BETA_RESET_REINGESTION.md`,
  `README.md`

## Context

`BETA_RESET_REINGESTION.md` §2: "Confirm that `public_data` is the
disposable analytics database and not the Airflow metadata database."
`docker-compose.airflow.yml` does the opposite:

```yaml
POSTGRES_DB: airflow                                           # :7
PUBLIC_DATA_DB_NAME: ${PUBLIC_DATA_DB_NAME:-airflow}            # :25
airflow connections add public_data ... --conn-login airflow --conn-schema airflow;   # :53
```

Every DAG resolves `PostgresHook(postgres_conn_id="public_data")`, so on the
stack `README.md` presents as "DAG orchestration + metadata DB", ingestion
writes `raw_capture`, `control`, `silver_*` and `gold_*` into the Airflow
metadata database, and the documented reset (`DROP DATABASE public_data`)
names a database that does not exist there.

## Findings

- Names disagree three ways: `stack.env.example` calls the warehouse
  `population_etl`, every module defaults `PUBLIC_DATA_DB_NAME` to
  `public_data`, and the reset guide hard-codes `public_data` and
  `airflow_admin`, which the shipped stack never creates.
- `tests/unit/deployment/test_container_contracts.py` parses all the
  compose files for image digests and port bindings and asserts nothing
  about where `public_data` points.

## Acceptance criteria

1. `docker-compose.airflow.yml` creates or targets a warehouse database
   distinct from the metadata database, and the `public_data` connection
   names it.
2. One warehouse name, defaulted in one place, used by the compose files,
   the env examples, the modules, and the reset guide; the plan records the
   name chosen.
3. A deployment guard reads the compose files and fails when a
   `public_data` connection resolves to the same database as
   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free
   `DEPLOY-` identifier; DEPLOY-007 at authoring time).

## Non-goals

- Changing the production stack (`docker-compose.yml`), which already
  separates them.

## Validation

- **Criterion 1 — creates, rather than targets.** `docker-compose.airflow.yml`
  now runs one PostGIS cluster with two databases: Airflow's `airflow`
  metadata database and the warehouse, created at first start by
  `infra/docker/initdb/create_warehouse_database.sh` from
  `PUBLIC_DATA_DB_NAME`. The `public_data` connection's `--conn-schema` is the
  same expression. Two consequences of that choice, both deliberate:
  - the image changed from `postgres:16-alpine` to the PostGIS digest the
    internal stack's warehouse uses. `silver_ref/DDL/silver_ref.sql` and every
    gold DDL run `CREATE EXTENSION postgis`, so the stock image could never
    have hosted the DAGs this stack exists to run -- the warehouse was only
    ever "working" there because it was the metadata database and nothing had
    reached the geography DDL.
  - the hook refuses to initialize when `WAREHOUSE_DB_NAME` equals
    `POSTGRES_DB`, so the defect cannot be reintroduced by setting
    `PUBLIC_DATA_DB_NAME=airflow`. Proved with a stubbed `createdb`: a
    distinct name prints `Created warehouse database population_etl`; `airflow`
    exits 1 with `WAREHOUSE_DB_NAME is airflow, which is Airflow's own
    metadata database`; unset exits 2 naming the variable.
- **Criterion 2 — the name chosen, and where the criterion is narrowed.**
  There is no single literal that can be right in every deployment: the
  shipped stacks' warehouse is `population_etl`, and an external operator's is
  whatever they already run. What the three-way disagreement in the Findings
  actually was is a name stated *more than once per stack*. So:
  - the Airflow-only stack states `population_etl` once, in
    `PUBLIC_DATA_DB_NAME`, and its connection schema and its initdb hook both
    read that.
  - `docker-compose.external.yml` had `PUBLIC_DATA_DB_NAME:-public_data` while
    its own connection resolved `--conn-schema ${PUBLIC_DATA_DB_NAME:-${ANALYTICS_DB_NAME}}`.
    **Those are two different databases in the same DAG run**: a
    `PostgresHook` write went to the operator's analytics database and a
    module's own psycopg connection (`database=_TARGET_DATABASE`) went to
    `public_data`, which the operator never created. The default is now the
    same expression as the connection's.
  - the modules: ten of them carried
    `os.environ.get("PUBLIC_DATA_DB_NAME", "public_data")` at import time.
    One definition now, `utility.db_connection.warehouse_database()`, beside
    `WAREHOUSE_CONNECTION_ID` and `DEFAULT_WAREHOUSE_DATABASE`, with the
    comment that says why a connection id and a database name are not the
    same thing. The default literal stays `public_data`, because every shipped
    stack sets the variable and that default is reached only by a bare
    `python -m` run -- retargeting it would silently move an existing
    deployment.
  - the reset guide: §2 no longer hard-codes a database name. It says
    `public_data` is the connection id, gives the `airflow connections get
    public_data` command that reads the schema, and tells the operator to
    substitute it. It also states that a deployment where the warehouse and
    the metadata database coincide has a defect predating the reset.
- **Criterion 3.** `tests/unit/deployment/test_warehouse_target_contracts.py`
  reads every `docker-compose*.yml` that creates the connection and fails when
  it resolves to the metadata database. Resolution alone could not decide it:
  the internal and Airflow-only stacks default every reference so resolving
  says which database each really names, but the external stack defaults
  nothing, so both sides resolve to the empty string and a naive comparison
  calls every external stack a collision. It therefore compares resolved
  values when they are non-empty and the *expressions* otherwise -- pointing
  the warehouse at the same variables as the metadata database is a collision
  whatever the operator sets them to. Three more rules ride with it: the
  connection schema must agree with `PUBLIC_DATA_DB_NAME`, the variable must
  be read in exactly one module under `src/`, and every pool a DAG asks for
  must be created by every stack.
- **A third defect, found by that last rule.** The DAGs name six pools and
  `docker-compose.external.yml` created five: `usda_nass_api` was missing, so
  on that stack the USDA NASS DAG's request tasks would never schedule, and
  the failure is a scheduler message about a missing pool rather than
  anything the ingestion code could report. The pool is now created there too.
  (`docker-compose.yml` already had it, which is how the omission stayed
  invisible.) The rule derives the six from the DAGs rather than listing them:
  it walks each `*_dag.py` AST for `pool=` keywords, resolves a `Name` through
  the file's own module constants and an `Attribute` by importing the config
  module the DAG imports `CONFIG` from (`pool=CONFIG.airflow_pool` in
  `pep_ingest_dag.py` resolves to `census_api` that way), and raises rather
  than skipping a shape it cannot resolve.
- **Criterion 4.** `DEPLOY-007` is in `TESTING_CONTRACT.md`, the family range
  reads `DEPLOY-001–DEPLOY-007`, `AUDITED_COUNTS["DEPLOY"]` is 7, and the
  totals are 403.
- **The guard fails on the code as it was.** Reverting
  `docker-compose.airflow.yml`, `docker-compose.external.yml`, and one module
  to `HEAD` produces all four failures, each naming the real defect:
  - `these stacks point ingestion at Airflow's own metadata database ...:
    ['docker-compose.airflow.yml -> postgres/airflow']`
  - `these stacks name two warehouses ...: ["docker-compose.external.yml:
    PUBLIC_DATA_DB_NAME='public_data' but the connection's schema is ''"]`
  - `PUBLIC_DATA_DB_NAME is named in more than one module ...:
    ['src/data_ingestion_toolbox/cdc/config.py', '.../utility/db_connection.py']`
  - `these stacks initialize Airflow without the pools their DAGs ask for ...:
    {'docker-compose.external.yml': ['usda_nass_api']}`
- **Compose agrees.** `docker compose -f docker-compose.airflow.yml config`
  renders `--conn-schema population_etl`, `PUBLIC_DATA_DB_NAME:
  population_etl`, `WAREHOUSE_DB_NAME: population_etl`, and all six pools,
  with no warnings; the external stack renders `PUBLIC_DATA_DB_NAME:
  your_analytics_db` from its own example. A real container run was not
  possible -- this environment has the Compose CLI but no Docker daemon -- so
  the initdb hook was exercised directly with a stubbed `createdb`, as above.
- The `${...}` reader the two deployment guards share moved to
  `tests/support/compose_expressions.py` rather than being copied into the
  second one.
- `pytest tests/unit` 1575 passed. `pytest tests/unit/deployment` 14 passed.
  `pytest tests/integration -m "integration and (redis or database) and not
  slow"` 161 passed, 2 skipped, 14 deselected -- the tier that imports the ten
  retargeted modules for real. `ruff check .` and `ruff format --check .`
  clean.

## Remaining work

- None.

## Correction at review (2026-09-14)

`README.md` described the new layout correctly where the stack is
introduced, and still carried the old claim further down, in the list of
what `airflow-init` seeds: "Airflow-only compose seeds `public_data` -> host
`postgres`, schema `airflow` (metadata DB)". That is the exact sentence this
plan exists to make false, and `README.md` is in the plan's own owner
surface. It now reads `${PUBLIC_DATA_DB_NAME:-population_etl}`, naming the
warehouse database beside the metadata one and the hook that creates it.

