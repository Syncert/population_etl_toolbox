"""Capture-first HHS hospital capacity pipeline (hospital-capacity-and-locations).

HHS's "COVID-19 Reported Patient Impact and Hospital Capacity by Facility"
(healthdata.gov ``anag-cw7u``) holds one row per hospital and collection
week from 2020-03-29 to 2024-04-21 and has not changed since 2024-05-03. A
run reads the dataset's view document, then every page of the registered
columns in a stable order, one capture per page and a pause apart; each
page is committed before anything parses it, so an interrupted run resumes
at the next page. The snapshot is then replayed (each hospital week stored
as published, each county, state and national weekly sum written once) and
published. A snapshot whose pages all equal the published one is
``unchanged`` and writes nothing (CHURN RULE).

Operator-triggered only: the dataset is frozen. Once a snapshot is
published a run reads nothing; ``{"history": true}`` reads the dataset
again.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.hhs_hospital_capacity.capture import capture_snapshot
from data_ingestion_toolbox.hhs_hospital_capacity.config import (
    HhsHospitalCapacityConfig,
)
from data_ingestion_toolbox.hhs_hospital_capacity.schema import (
    ensure_hhs_hospital_capacity_schema,
)
from data_ingestion_toolbox.hhs_hospital_capacity.silver_hhs_hospital_capacity.load import (
    publish_snapshot,
    replay_snapshot,
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
    conn_id = HhsHospitalCapacityConfig().postgres_conn_id.strip()
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
    ensure_hhs_hospital_capacity_schema(_get_postgres_hook())


def _ingest(**context: Any) -> dict[str, Any]:
    log = logging.getLogger("airflow.task")
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    connection_factory = _get_postgres_hook().get_conn
    snapshot_id = capture_snapshot(
        connection_factory, conf=conf, config=HhsHospitalCapacityConfig()
    )
    if snapshot_id is None:
        log.info("nothing to read: the published snapshot stands")
        return {"snapshot_id": None, "rows": 0, "published": 0}
    rows = replay_snapshot(connection_factory, snapshot_id=snapshot_id)
    published = publish_snapshot(connection_factory, snapshot_id=snapshot_id)
    log.info(
        "snapshot %s: %d hospital week(s) stored, published=%d",
        snapshot_id,
        rows,
        published,
    )
    return {"snapshot_id": str(snapshot_id), "rows": rows, "published": published}


with DAG(
    dag_id="hhs_hospital_capacity_ingest",
    description="Capture, replay and publish HHS weekly hospital beds and occupancy by facility",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["hhs", "hospitals", "capacity", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_hhs_hospital_capacity_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    ingest = PythonOperator(
        task_id="ingest_batch_pages",
        python_callable=_ingest,
    )
    ensure_schema >> require_shared_geography >> ingest
