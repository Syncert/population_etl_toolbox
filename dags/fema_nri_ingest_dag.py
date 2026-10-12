"""Capture-first FEMA National Risk Index and disaster declarations pipeline.

Each run reads both FEMA streams page by page -- the National Risk Index
county layer and OpenFEMA's disaster declarations -- replays each into
silver and publishes it. An NRI read whose pages match the last published
read replays nothing; a declarations read keeps only revisions it has not
seen.

One task per stream, through the one-slot ``fema_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.fema_nri.capture import capture_stream
from data_ingestion_toolbox.fema_nri.config import FemaConfig
from data_ingestion_toolbox.fema_nri.registry import DECLARATIONS, NRI
from data_ingestion_toolbox.fema_nri.schema import ensure_fema_nri_schema
from data_ingestion_toolbox.fema_nri.silver_fema_nri.load import (
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
    "retry_delay": timedelta(minutes=20),
}


def _get_postgres_hook():  # noqa: ANN202
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=FemaConfig().postgres_conn_id.strip())


def _ingest(stream: str) -> dict[str, Any]:
    connection_factory = _get_postgres_hook().get_conn
    run_id, status = capture_stream(connection_factory, stream, config=FemaConfig())
    rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
    published = publish_run(connection_factory, run_id=UUID(str(run_id)))
    return {
        "stream": stream,
        "run_id": str(run_id),
        "status": status,
        "rows": rows,
        "published": published,
    }


@dag(
    dag_id="fema_nri_ingest",
    description="Capture, replay, reconcile, and publish the FEMA National Risk Index and disaster declarations",
    default_args=DEFAULT_ARGS,
    # Daily: OpenFEMA refreshes declarations every twenty minutes, and an
    # unchanged NRI read replays nothing.
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["fema", "nri", "disasters", "capture-first"],
)
def fema_nri_ingest():
    @task()
    def ensure_fema_nri_schema_task() -> None:
        ensure_fema_nri_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="fema_files")
    def ingest_batch_nri() -> dict[str, Any]:
        return _ingest(NRI)

    @task(pool="fema_files")
    def ingest_batch_declarations() -> dict[str, Any]:
        return _ingest(DECLARATIONS)

    schema = ensure_fema_nri_schema_task.override(task_id="ensure_fema_nri_schema")()
    geography = require_shared_geography()
    schema >> geography >> [ingest_batch_nri(), ingest_batch_declarations()]


fema_nri_ingest()
