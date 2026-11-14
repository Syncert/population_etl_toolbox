"""Capture-first CDC COVID-19 case surveillance counts (outbreak-calibration-sets).

CDC's "COVID-19 Case Surveillance Public Use Data with Geography" (data.cdc.gov
``n8mc-b4w4``) holds one row per case record, 2020-01 to 2024-07, and is
frozen. A run never captures the 106-million-row file: it reads the dataset's
view document, then the slice index (records per state and month), then one
grouped request per state and month (counts by county, age group,
hospitalisation and death; race and ethnicity are never requested), a pause
apart, each response committed before anything parses it. A slice whose
groups do not add up to the index is refused. The snapshot is then replayed
(each group stored as returned, each county, state and national monthly
count written once) and published. A snapshot whose bytes all equal the
published one is ``unchanged`` and writes nothing (CHURN RULE).

Operator-triggered only: the dataset is frozen. The first run sends about
2,900 requests over about 2.5 hours and resumes at the next slice if it
stops. Once a snapshot is published a run reads nothing;
``{"history": true}`` reads the dataset again.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.cdc_covid_cases.capture import capture_snapshot
from data_ingestion_toolbox.cdc_covid_cases.config import CdcCovidCasesConfig
from data_ingestion_toolbox.cdc_covid_cases.schema import (
    ensure_cdc_covid_cases_schema,
)
from data_ingestion_toolbox.cdc_covid_cases.silver_cdc_covid_cases.load import (
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
    conn_id = CdcCovidCasesConfig().postgres_conn_id.strip()
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
    ensure_cdc_covid_cases_schema(_get_postgres_hook())


def _ingest(**context: Any) -> dict[str, Any]:
    log = logging.getLogger("airflow.task")
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    connection_factory = _get_postgres_hook().get_conn
    snapshot_id = capture_snapshot(
        connection_factory, conf=conf, config=CdcCovidCasesConfig()
    )
    if snapshot_id is None:
        log.info("nothing to read: the published snapshot stands")
        return {"snapshot_id": None, "records": 0, "published": 0}
    records = replay_snapshot(connection_factory, snapshot_id=snapshot_id)
    published = publish_snapshot(connection_factory, snapshot_id=snapshot_id)
    log.info(
        "snapshot %s: %d case record(s) counted, published=%d",
        snapshot_id,
        records,
        published,
    )
    return {"snapshot_id": str(snapshot_id), "records": records, "published": published}


with DAG(
    dag_id="cdc_covid_cases_ingest",
    description="Capture, replay and publish CDC COVID-19 case-record counts by county, month and age",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "covid", "case-surveillance", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_cdc_covid_cases_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    ingest = PythonOperator(
        task_id="ingest_state_month_slices",
        python_callable=_ingest,
    )
    ensure_schema >> require_shared_geography >> ingest
