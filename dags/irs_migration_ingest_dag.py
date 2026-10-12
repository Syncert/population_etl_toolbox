"""Capture-first IRS SOI county migration pipeline (irs-county-migration).

Each run captures every registered county inflow and outflow file -- one
per direction and pair of filing years -- replays it into silver and
publishes it. SOI publishes a new pair of years about once a year and
seldom revises an old one; a run that finds the same bytes adds a capture
and no flow, and a revised file is kept beside the one it revised.

One mapped task per file, through the one-slot ``irs_soi_files`` pool:
each file is about 4.5 MB.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.irs_migration.capture import capture_file
from data_ingestion_toolbox.irs_migration.config import IrsMigrationConfig
from data_ingestion_toolbox.irs_migration.registry import get_file, registered_files
from data_ingestion_toolbox.irs_migration.schema import ensure_irs_migration_schema
from data_ingestion_toolbox.irs_migration.silver_irs_migration.load import (
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

    return PostgresHook(postgres_conn_id=IrsMigrationConfig().postgres_conn_id.strip())


@dag(
    dag_id="irs_migration_ingest",
    description="Capture, replay, reconcile, and publish IRS SOI county migration flows",
    default_args=DEFAULT_ARGS,
    # Monthly: SOI publishes a new pair of years about once a year, and an
    # unchanged file costs one capture.
    schedule="0 15 5 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["irs", "migration", "flows", "capture-first"],
)
def irs_migration_ingest():
    @task()
    def ensure_irs_migration_schema_task() -> None:
        ensure_irs_migration_schema(_get_postgres_hook())

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
        return [f"{item.direction}:{item.year_pair}" for item in registered_files()]

    @task(pool="irs_soi_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        direction, year_pair = file_key.split(":")
        connection_factory = _get_postgres_hook().get_conn
        run_id, _capture = capture_file(
            connection_factory,
            get_file(direction, year_pair),
            config=IrsMigrationConfig(),
        )
        flows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "file": file_key,
            "run_id": str(run_id),
            "flows": flows,
            "published": published,
        }

    schema = ensure_irs_migration_schema_task.override(
        task_id="ensure_irs_migration_schema"
    )()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


irs_migration_ingest()
