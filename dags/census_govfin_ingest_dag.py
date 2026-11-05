"""Capture-first Census state and local government finance pipeline (taxes-and-public-money T-5).

Three datasets: the Annual Survey of State and Local Government Finances
and the Quarterly Summary of State and Local Tax Revenue from the Census
Data API, and the Census of Governments finance Individual Unit Files,
which the replay rolls up to county areas. Each run captures each dataset's
revision window and every registered period the warehouse has never had
answered (``data_ingestion_toolbox.utility.backfill``), so the first run on
a fresh warehouse loads every registered year and edition without a manual
history run, and later runs are the window again. ``{"history": true}``
re-sweeps every registered period, answered or not.

The API requests share the ``census_api`` pool with the ACS and SAIPE: one
host, one key, one rate limit.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.census_govfin.capture import capture_period
from data_ingestion_toolbox.census_govfin.config import GovFinConfig
from data_ingestion_toolbox.census_govfin.plan import answered, periods_to_capture
from data_ingestion_toolbox.census_govfin.registry import DATASET_IDS
from data_ingestion_toolbox.census_govfin.schema import ensure_census_govfin_schema
from data_ingestion_toolbox.census_govfin.silver_census_govfin.load import (
    publish_run,
    replay_run,
)
from data_ingestion_toolbox.silver_ref.geography_guard import (
    require_shared_geography_loaded,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

#: The unit files come from www2.census.gov, not the API host.
POOLS = {"asslgf": "census_api", "qtax": "census_api", "cog_units": "default_pool"}


def _get_postgres_hook():
    conn_id = GovFinConfig.from_environment().postgres_conn_id.strip()
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=conn_id)


def _require_shared_geography() -> None:
    with _get_postgres_hook().get_conn() as connection:
        counts = require_shared_geography_loaded(connection)
    logging.getLogger("airflow.task").info(
        "shared geography reference is loaded: %s",
        ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
    )


def _ensure_schema() -> None:
    ensure_census_govfin_schema(_get_postgres_hook())


def _capture(dataset_id: str, **context: Any) -> list[str]:
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    connection_factory = _get_postgres_hook().get_conn
    config = GovFinConfig.from_environment()
    with _get_postgres_hook().get_conn() as connection:
        done = answered(connection, dataset_id)
    periods = periods_to_capture(dataset_id, conf, done)
    logging.getLogger("airflow.task").info(
        "%s: capturing %d period(s) (%s); %d already answered",
        dataset_id,
        len(periods),
        ", ".join(map(str, periods)),
        len(done),
    )
    run_ids = []
    for period in periods:
        run_id, _slices = capture_period(
            connection_factory, dataset_id, period, config=config
        )
        run_ids.append(str(run_id))
    return run_ids


def _replay(run_ids: list[str]) -> int:
    connection_factory = _get_postgres_hook().get_conn
    return sum(
        replay_run(connection_factory, run_id=UUID(run_id)) for run_id in run_ids
    )


def _publish(run_ids: list[str]) -> int:
    connection_factory = _get_postgres_hook().get_conn
    return sum(
        publish_run(connection_factory, run_id=UUID(run_id)) for run_id in run_ids
    )


with DAG(
    dag_id="census_govfin_ingest",
    description="Capture, replay, roll up and publish Census state and local government finance",
    default_args=DEFAULT_ARGS,
    # Monthly: QTAX publishes each quarter, the annual survey each autumn and
    # a Census of Governments edition every five years; a run that finds
    # every period answered captures only the revision window.
    schedule="0 7 18 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["census", "government-finance", "taxes", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_census_govfin_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    for dataset_id in DATASET_IDS:
        capture = PythonOperator(
            task_id=f"ingest_batch_{dataset_id}",
            python_callable=_capture,
            op_kwargs={"dataset_id": dataset_id},
            pool=POOLS[dataset_id],
        )
        replay = PythonOperator(
            task_id=f"replay_{dataset_id}",
            python_callable=_replay,
            op_kwargs={"run_ids": capture.output},
            execution_timeout=timedelta(hours=2),
        )
        publish = PythonOperator(
            task_id=f"publish_{dataset_id}",
            python_callable=_publish,
            op_kwargs={"run_ids": capture.output},
        )
        ensure_schema >> require_shared_geography >> capture >> replay >> publish
