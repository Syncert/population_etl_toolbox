"""Capture-first NOAA nClimDiv county monthly temperature and precipitation pipeline.

Each run reads NCEI's climdiv directory for the current file of each
registered element. A file name already published is not downloaded again;
any other is captured, and when its bytes differ from the element's last
published capture it is replayed into silver -- writing only the months
whose published value changed -- and published. NCEI replaces the files
early each month, so the DAG runs monthly after that.

One task through the one-slot ``noaa_climdiv_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.noaa_climdiv.capture import capture_release
from data_ingestion_toolbox.noaa_climdiv.config import ClimdivConfig
from data_ingestion_toolbox.noaa_climdiv.schema import ensure_noaa_climdiv_schema
from data_ingestion_toolbox.noaa_climdiv.silver_noaa_climdiv.load import (
    publish_run,
    replay_run,
    settle_unreplayed_runs,
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

    return PostgresHook(postgres_conn_id=ClimdivConfig().postgres_conn_id.strip())


@dag(
    dag_id="noaa_climdiv_ingest",
    description="Capture, replay changed months, reconcile, and publish NOAA nClimDiv county series",
    default_args=DEFAULT_ARGS,
    # Monthly on the 12th: NCEI's release of the 5th (2026-11-05) is in
    # place by then, and an already-loaded file name is not fetched again.
    schedule="0 15 12 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "ncei", "nclimdiv", "capture-first"],
)
def noaa_climdiv_ingest():
    @task()
    def ensure_noaa_climdiv_schema_task() -> None:
        ensure_noaa_climdiv_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="noaa_climdiv_files")
    def ingest_batch_files() -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        # A retry after a failed replay reads the directory again under a
        # new run; finish the run it left behind first, oldest first.
        settled = settle_unreplayed_runs(connection_factory)
        run_id, statuses = capture_release(connection_factory, config=ClimdivConfig())
        written = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "run_id": str(run_id),
            "files": statuses,
            "settled_runs": [str(settled_run) for settled_run in settled],
            "written": written,
            "published": published,
        }

    schema = ensure_noaa_climdiv_schema_task.override(
        task_id="ensure_noaa_climdiv_schema"
    )()
    geography = require_shared_geography()
    schema >> geography >> ingest_batch_files()


noaa_climdiv_ingest()
