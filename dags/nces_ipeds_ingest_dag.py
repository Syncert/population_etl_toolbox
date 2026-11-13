"""Capture-first NCES IPEDS pipeline (college-housing-capacity).

A run reads, for each registered component (directory HD, housing IC, fall
enrolment EF A), every registered collection year the warehouse has not yet
answered, plus the year after the newest one published, one file at a time
and a pause apart. Each file is captured, replayed and published in turn,
the directory first within a year: an institution's housing capacity and
enrolment are summed into the county that year's directory names. NCES
answering 404 is recorded ``not_published`` and asked again by a later run.
A manual run with ``{"history": true}`` reads every registered year again
(identical bytes are ``unchanged`` and write nothing).

Monthly: NCES posts IPEDS files as each collection's data are released, on no
fixed calendar.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.nces_ipeds import capture as ipeds_capture
from data_ingestion_toolbox.nces_ipeds.config import NcesIpedsConfig
from data_ingestion_toolbox.nces_ipeds.plan import answered, files_to_capture
from data_ingestion_toolbox.nces_ipeds.schema import ensure_nces_ipeds_schema
from data_ingestion_toolbox.nces_ipeds.silver_nces_ipeds.load import (
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
    conn_id = NcesIpedsConfig().postgres_conn_id.strip()
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
    ensure_nces_ipeds_schema(_get_postgres_hook())


def _ingest(**context: Any) -> list[dict[str, Any]]:
    log = logging.getLogger("airflow.task")
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    connection_factory = _get_postgres_hook().get_conn
    config = NcesIpedsConfig()
    with connection_factory() as connection:
        done = answered(connection)
    wanted = files_to_capture(done, conf)
    log.info("reading %d IPEDS file(s)", len(wanted))
    results = []
    for index, (component, year) in enumerate(wanted):
        if index:
            ipeds_capture.pause_between_files(config)
        run_id, status = ipeds_capture.capture_file(
            connection_factory, component, year, config=config
        )
        rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        log.info(
            "%s: %s, %d row(s), published=%d",
            component.file_stem(year),
            status,
            rows,
            published,
        )
        results.append(
            {
                "file": component.file_stem(year),
                "run_id": str(run_id),
                "status": status,
                "rows": rows,
                "published": published,
            }
        )
    return results


with DAG(
    dag_id="nces_ipeds_ingest",
    description="Capture, replay and publish IPEDS college locations, housing capacity and fall enrolment",
    default_args=DEFAULT_ARGS,
    schedule="0 17 25 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nces", "ipeds", "colleges", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_nces_ipeds_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    ingest = PythonOperator(
        task_id="ingest_batch_files",
        python_callable=_ingest,
    )
    ensure_schema >> require_shared_geography >> ingest
