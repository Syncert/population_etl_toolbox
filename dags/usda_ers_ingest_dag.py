"""Capture-first USDA ERS county codes and atlas pipeline.

Each run reads every registered ERS file -- the 2023 Rural-Urban Continuum
Codes, the 2025 County Typology Codes and the July 2025 Food Environment
Atlas -- and, when a file's bytes differ from its last published capture,
replays it into silver and publishes it. ERS replaces files in place; an
unchanged read replays nothing, and a replaced file is kept beside the one
it replaced.

One mapped task per file, through the one-slot ``usda_ers_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.usda_ers.capture import capture_file
from data_ingestion_toolbox.usda_ers.config import ErsConfig
from data_ingestion_toolbox.usda_ers.registry import get_file, registered_files
from data_ingestion_toolbox.usda_ers.schema import ensure_usda_ers_schema
from data_ingestion_toolbox.usda_ers.silver_usda_ers.load import (
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

    return PostgresHook(postgres_conn_id=ErsConfig().postgres_conn_id.strip())


@dag(
    dag_id="usda_ers_ingest",
    description="Capture, replay, reconcile, and publish USDA ERS county codes and atlas indicators",
    default_args=DEFAULT_ARGS,
    # Monthly: ERS replaces files in place without a calendar, and an
    # unchanged file replays nothing.
    schedule="0 17 25 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["usda", "ers", "rural", "food", "capture-first"],
)
def usda_ers_ingest():
    @task()
    def ensure_usda_ers_schema_task() -> None:
        ensure_usda_ers_schema(_get_postgres_hook())

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

    @task(pool="usda_ers_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_file(
            connection_factory, get_file(file_key), config=ErsConfig()
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

    schema = ensure_usda_ers_schema_task.override(task_id="ensure_usda_ers_schema")()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


usda_ers_ingest()
