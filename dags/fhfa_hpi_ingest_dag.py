"""Capture-first FHFA annual House Price Index pipeline (fhfa-house-price-index).

Each run reads every registered workbook -- today the county file -- and,
when its bytes differ from the last published file's, replays it into
silver and publishes it. FHFA revises every year's index in each new file,
so each changed file is a new vintage kept beside the previous one; an
unchanged read replays nothing.

One mapped task per file, through the one-slot ``fhfa_hpi_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.fhfa_hpi.capture import capture_file
from data_ingestion_toolbox.fhfa_hpi.config import HpiConfig
from data_ingestion_toolbox.fhfa_hpi.registry import get_file, registered_files
from data_ingestion_toolbox.fhfa_hpi.schema import ensure_fhfa_hpi_schema
from data_ingestion_toolbox.fhfa_hpi.silver_fhfa_hpi.load import (
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

    return PostgresHook(postgres_conn_id=HpiConfig().postgres_conn_id.strip())


@dag(
    dag_id="fhfa_hpi_ingest",
    description="Capture, replay, reconcile, and publish the FHFA annual House Price Index",
    default_args=DEFAULT_ARGS,
    # Monthly: FHFA revises the annual workbook without a published
    # calendar, and an unchanged file replays nothing.
    schedule="0 15 25 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["fhfa", "hpi", "housing", "capture-first"],
)
def fhfa_hpi_ingest():
    @task()
    def ensure_fhfa_hpi_schema_task() -> None:
        ensure_fhfa_hpi_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task()
    def plan_files() -> list[str]:
        return [item.key for item in registered_files()]

    @task(pool="fhfa_hpi_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_file(
            connection_factory, get_file(file_key), config=HpiConfig()
        )
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "file": file_key,
            "run_id": str(run_id),
            "status": status,
            "facts": facts,
            "published": published,
        }

    schema = ensure_fhfa_hpi_schema_task.override(task_id="ensure_fhfa_hpi_schema")()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


fhfa_hpi_ingest()
