"""Capture-first County Business Patterns pipeline (census-county-business-patterns).

Each run captures every registered county, state and nation file -- one per
level and year -- replays it into silver and publishes it. The Bureau
publishes one year a year; a run that finds the same bytes adds a capture
and no observation, and a corrected file is kept beside the one it
corrected.

One mapped task per file, through the one-slot ``census_cbp_files`` pool:
the state files are about 15 MB zipped.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.census_cbp.capture import capture_file
from data_ingestion_toolbox.census_cbp.config import CbpConfig
from data_ingestion_toolbox.census_cbp.registry import get_file, registered_files
from data_ingestion_toolbox.census_cbp.schema import ensure_census_cbp_schema
from data_ingestion_toolbox.census_cbp.silver_census_cbp.load import (
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

    return PostgresHook(postgres_conn_id=CbpConfig().postgres_conn_id.strip())


@dag(
    dag_id="census_cbp_ingest",
    description="Capture, replay, reconcile, and publish Census County Business Patterns",
    default_args=DEFAULT_ARGS,
    # Monthly: the Bureau publishes one year a year, and an unchanged file
    # costs one capture.
    schedule="0 13 15 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["census", "cbp", "business", "capture-first"],
)
def census_cbp_ingest():
    @task()
    def ensure_census_cbp_schema_task() -> None:
        ensure_census_cbp_schema(_get_postgres_hook())

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

    @task(pool="census_cbp_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        kind, year = file_key.split(":")
        connection_factory = _get_postgres_hook().get_conn
        run_id, _capture = capture_file(
            connection_factory,
            get_file(kind, int(year)),
            config=CbpConfig(),
        )
        flows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "file": file_key,
            "run_id": str(run_id),
            "facts": flows,
            "published": published,
        }

    schema = ensure_census_cbp_schema_task.override(
        task_id="ensure_census_cbp_schema"
    )()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


census_cbp_ingest()
