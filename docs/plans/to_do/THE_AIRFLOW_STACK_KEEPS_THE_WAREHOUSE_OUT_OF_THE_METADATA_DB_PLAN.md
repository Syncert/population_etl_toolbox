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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
