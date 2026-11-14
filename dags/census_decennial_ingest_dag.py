"""Capture-first 2020 Census counts pipeline (group-quarters-by-type).

The 2020 Census redistricting data are published once. A run captures every
registered slice -- the nation, the states, the counties, each state's
tracts, and the blocks of any county ``BLOCK_COUNTIES`` declares (none
today) -- that the warehouse has never had answered, one request per slice,
and every byte is committed before anything parses it. So the first run on
a fresh warehouse loads everything, and later runs request nothing until a
slice is registered. A manual run with ``{"history": true}`` in its conf asks
for every registered slice again (``docs/reference/BETA_RESET_REINGESTION.md``).

Operator-triggered only: there is nothing new to fetch on a schedule. The
requests share the ``census_api`` pool with the ACS and SAIPE/SAHIE: one
host, one key, one rate limit.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.census_decennial.capture import capture_slices
from data_ingestion_toolbox.census_decennial.config import DecennialConfig
from data_ingestion_toolbox.census_decennial.plan import (
    answered_slices,
    slices_to_capture,
)
from data_ingestion_toolbox.census_decennial.registry import DATASETS, get_dataset
from data_ingestion_toolbox.census_decennial.schema import (
    ensure_census_decennial_schema,
)
from data_ingestion_toolbox.census_decennial.silver_census_decennial.load import (
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


def _get_postgres_hook():
    conn_id = DecennialConfig.from_environment().postgres_conn_id.strip()
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
    ensure_census_decennial_schema(_get_postgres_hook())


def _capture(dataset_id: str, **context: Any) -> list[str]:
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    dataset = get_dataset(dataset_id)
    with _get_postgres_hook().get_conn() as connection:
        answered = answered_slices(connection, dataset_id)
    wanted = slices_to_capture(conf, answered)
    logging.getLogger("airflow.task").info(
        "%s: capturing %d slice(s); %d already answered",
        dataset_id,
        len(wanted),
        len(answered),
    )
    if not wanted:
        return []
    run_id, _slices = capture_slices(
        _get_postgres_hook().get_conn,
        dataset,
        wanted,
        config=DecennialConfig.from_environment(),
    )
    return [str(run_id)]


def _replay(dataset_id: str, run_ids: list[str]) -> int:
    dataset = get_dataset(dataset_id)
    connection_factory = _get_postgres_hook().get_conn
    return sum(
        replay_run(connection_factory, run_id=UUID(run_id), dataset=dataset)
        for run_id in run_ids
    )


def _publish(run_ids: list[str]) -> int:
    connection_factory = _get_postgres_hook().get_conn
    return sum(
        publish_run(connection_factory, run_id=UUID(run_id)) for run_id in run_ids
    )


with DAG(
    dag_id="census_decennial_ingest",
    description="Capture, replay, reconcile, and publish 2020 Census group quarters counts",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["census", "decennial", "group-quarters", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_census_decennial_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    for registered in DATASETS:
        capture = PythonOperator(
            task_id=f"ingest_batch_{registered.dataset_id}",
            python_callable=_capture,
            op_kwargs={"dataset_id": registered.dataset_id},
            pool="census_api",
        )
        replay = PythonOperator(
            task_id=f"replay_{registered.dataset_id}",
            python_callable=_replay,
            op_kwargs={"dataset_id": registered.dataset_id, "run_ids": capture.output},
        )
        publish = PythonOperator(
            task_id=f"publish_{registered.dataset_id}",
            python_callable=_publish,
            op_kwargs={"run_ids": capture.output},
        )
        ensure_schema >> require_shared_geography >> capture >> replay >> publish
