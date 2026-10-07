"""Capture-first NOAA U.S. Climate Normals 1991-2020 pipeline.

Each run reads the registered annual/seasonal by-station archive and, when
its bytes differ from the last published capture, replays it into silver,
assigns each station to a county from its coordinates, and publishes it.
NCEI names a new archive for a new version, so a changed archive is a
registry change; the scheduled read proves the registered one still serves.

One task through the one-slot ``noaa_normals_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.noaa_normals.capture import capture_archive
from data_ingestion_toolbox.noaa_normals.config import NormalsConfig
from data_ingestion_toolbox.noaa_normals.schema import ensure_noaa_normals_schema
from data_ingestion_toolbox.noaa_normals.silver_noaa_normals.load import (
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

    return PostgresHook(postgres_conn_id=NormalsConfig().postgres_conn_id.strip())


@dag(
    dag_id="noaa_normals_ingest",
    description="Capture, replay, assign counties, reconcile, and publish NOAA climate normals",
    default_args=DEFAULT_ARGS,
    # Quarterly: the 1991-2020 normals change only by a new archive version,
    # and an unchanged archive replays nothing.
    schedule="0 19 2 1,4,7,10 *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "ncei", "climate-normals", "capture-first"],
)
def noaa_normals_ingest():
    @task()
    def ensure_noaa_normals_schema_task() -> None:
        ensure_noaa_normals_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="noaa_normals_files")
    def ingest_batch_archive() -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_archive(connection_factory, config=NormalsConfig())
        normals = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "run_id": str(run_id),
            "status": status,
            "normals": normals,
            "published": published,
        }

    schema = ensure_noaa_normals_schema_task.override(
        task_id="ensure_noaa_normals_schema"
    )()
    geography = require_shared_geography()
    schema >> geography >> ingest_batch_archive()


noaa_normals_ingest()
