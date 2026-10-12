"""Capture-first EPA air quality pipeline (AirData annual monitor files).

Each run reads every registered year's annual monitor file and, when a
file's bytes differ from that year's last published capture, replays it
into silver and publishes it. EPA regenerates the files in June and
December, and AQS allows old data to change; a changed file is kept beside
the one it replaced.

One mapped task per year, through the one-slot ``epa_aqs_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.epa_aqs.capture import capture_file
from data_ingestion_toolbox.epa_aqs.config import AqsConfig
from data_ingestion_toolbox.epa_aqs.registry import get_file, registered_files
from data_ingestion_toolbox.epa_aqs.schema import ensure_epa_aqs_schema
from data_ingestion_toolbox.epa_aqs.silver_epa_aqs.load import (
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

    return PostgresHook(postgres_conn_id=AqsConfig().postgres_conn_id.strip())


@dag(
    dag_id="epa_aqs_ingest",
    description="Capture, replay, reconcile, and publish EPA AirData annual monitor files",
    default_args=DEFAULT_ARGS,
    # Monthly: EPA regenerates the files in June and December without a
    # fixed date, and an unchanged file replays nothing.
    schedule="0 18 25 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["epa", "aqs", "air-quality", "capture-first"],
)
def epa_aqs_ingest():
    @task()
    def ensure_epa_aqs_schema_task() -> None:
        ensure_epa_aqs_schema(_get_postgres_hook())

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

    @task(pool="epa_aqs_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_file(
            connection_factory, get_file(file_key), config=AqsConfig()
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

    schema = ensure_epa_aqs_schema_task.override(task_id="ensure_epa_aqs_schema")()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


epa_aqs_ingest()
