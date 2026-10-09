"""Capture-first NCES Common Core of Data public-school pipeline.

Each run reads every registered CCD school-universe file and EDGE school
geocode file and, when a file's bytes differ from its last published
capture, replays it into silver and publishes it. NCES names each new
release in the file name, so a new release is a registry change and is kept
beside the one it supersedes.

One mapped task per file, through the one-slot ``nces_ccd_files`` pool:
the membership file alone is over 200 MB compressed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.nces_ccd.capture import capture_file
from data_ingestion_toolbox.nces_ccd.config import CcdConfig
from data_ingestion_toolbox.nces_ccd.registry import get_file, registered_files
from data_ingestion_toolbox.nces_ccd.schema import ensure_nces_ccd_schema
from data_ingestion_toolbox.nces_ccd.silver_nces_ccd.load import (
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

    return PostgresHook(postgres_conn_id=CcdConfig().postgres_conn_id.strip())


@dag(
    dag_id="nces_ccd_ingest",
    description="Capture, replay, reconcile, and publish NCES CCD school files and EDGE geocodes",
    default_args=DEFAULT_ARGS,
    # Quarterly: NCES releases a school year's files once or twice a year
    # under new names, and an unchanged file replays nothing.
    schedule="0 20 3 1,4,7,10 *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nces", "ccd", "schools", "capture-first"],
)
def nces_ccd_ingest():
    @task()
    def ensure_nces_ccd_schema_task() -> None:
        ensure_nces_ccd_schema(_get_postgres_hook())

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

    @task(pool="nces_ccd_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_file(
            connection_factory, get_file(file_key), config=CcdConfig()
        )
        rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "file": file_key,
            "run_id": str(run_id),
            "status": status,
            "rows": rows,
            "published": published,
        }

    schema = ensure_nces_ccd_schema_task.override(task_id="ensure_nces_ccd_schema")()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


nces_ccd_ingest()
