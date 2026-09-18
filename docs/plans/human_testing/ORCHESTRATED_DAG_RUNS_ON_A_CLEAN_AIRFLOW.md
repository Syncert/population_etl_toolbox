# Check: the three orchestrated DagRun tests pass on a clean Airflow install

## What this checks

That `tests/dags/test_dag_pipeline_execution.py` -- the three tests that run a
whole DAG as a real `DagRun` against the disposable warehouse -- still pass.
They are the only tests that exercise Airflow's scheduler path rather than
calling the DAG's callables directly.

```text
tests/dags/test_dag_pipeline_execution.py::test_all_pipelines_execute_end_to_end_through_airflow
tests/dags/test_dag_pipeline_execution.py::test_orchestrated_run_populates_shared_dimensions
tests/dags/test_dag_pipeline_execution.py::test_orchestrated_run_publishes_partial_county_place_overlaps
```

The rest of the DAG tier -- 146 tests, including the three sources'
`ensure_*_schema` tasks added by `source-ddl-under-src` -- passes on the
Windows machine and needs nothing from you.

## Why it was not automated

These three need an Airflow *metadata database*, which means `airflow db init`
has to succeed. On the Windows dev machine's interpreter it cannot, and the
reason is the install rather than the repository:

```console
$ python -m airflow db init
  File ".../airflow/migrations/versions/0047_3_0_0_add_dag_versioning.py", line 35
    from airflow.migrations.utils import ignore_sqlite_value_error
ImportError: cannot import name 'ignore_sqlite_value_error'
    from 'airflow.migrations.utils'
```

`airflow.__version__` there is `2.11.2`, and `0047_3_0_0_add_dag_versioning.py`
is an Airflow **3.0** migration. The site-packages tree holds files from two
releases, so alembic cannot build a revision map and no metadata database can
be created. Without it the three tests error at fixture setup with
`sqlite3.OperationalError: no such table: connection` or the ImportError above.

This is not the `sqlite:////` defect that used to abort the whole tier -- that
one was in this repository and is fixed (ENV-023); the tier collects and runs
now. This is the interpreter's own package tree.

- Filed by:
  [`docs/plans/needs_review/EVERY_SOURCE_OWNS_ITS_DDL_UNDER_SRC_PLAN.md`](../needs_review/EVERY_SOURCE_OWNS_ITS_DDL_UNDER_SRC_PLAN.md)
- Catalog row: **DAG-016** in
  [`docs/reference/TESTING_CONTRACT.md`](../../reference/TESTING_CONTRACT.md)
- No acceptance criterion of that plan needs these three: the plan's criterion
  is that `tests/dags` asserts the `ensure_*` task's placement and its DDL
  files, which
  [`tests/dags/test_source_schema_tasks.py`](../../../tests/dags/test_source_schema_tasks.py)
  does and which passes.

## You need

- Any machine that can create a Python 3.11 virtual environment
- The disposable PostGIS container (`make test-integration-up`, or
  `docker compose -f infra/docker/docker-compose.test.yml up -d postgres`)

**Time:** about 10 minutes, most of it the Airflow install.

## What to run

Install the extra beside the project venv, never into it -- the `airflow-dev`
extra pins SQLAlchemy 1.4 against the API's 2.x, and installing it into
`.venv` downgrades four packages under every other tier:

```bash
python3.11 -m venv /tmp/airflow-venv
/tmp/airflow-venv/bin/python -m pip install -e '.[airflow-dev]'

export AIRFLOW_HOME=/tmp/airflow-home AIRFLOW__CORE__LOAD_EXAMPLES=False
/tmp/airflow-venv/bin/airflow db init

RUN_DAG_TESTS=1 RUN_INTEGRATION_TESTS=1 \
  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
  TEST_POSTGRES_DATABASE=population_etl_test \
  /tmp/airflow-venv/bin/python -m pytest -q \
  tests/dags/test_dag_pipeline_execution.py
```

## What "passes" looks like

```text
3 passed
```

Each test runs a real `DagRun` per ingestion DAG. With the schema tasks added
by `source-ddl-under-src`, the CDC, FBI and USDA NASS runs now begin with
`ensure_cdc_schema`, `ensure_fbi_schema` and `ensure_nass_schema`; those tasks
apply DDL the fixture warehouse already carries, so they should report
`already bootstrapped with matching hash; skipping` and succeed.

## If it fails

The interesting failure is a schema task erroring rather than skipping, or a
DagRun that stops at one. Report the task's log line and which of the three
tests it belonged to. A failure in `airflow db init` itself is an install
problem, not a repository one -- say which Python and which Airflow version.
