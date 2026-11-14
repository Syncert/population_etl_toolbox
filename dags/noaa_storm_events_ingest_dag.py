"""Capture-first NOAA Storm Events Database pipeline (1950 onward).

Each run reads the NWS zone-county page for the zone-county files and NCEI's
Storm Events directory for the newest details file of every year. A file
name already published is not downloaded again (the first run reads every
year; later runs the current year and any year NCEI restamped); any other is
captured, and when its bytes differ from the last published capture of its
year it is replayed into silver -- writing only the events whose published
row changed, placing them on counties and recounting their year -- and
published. NCEI updates the current year's file each month.

One task through the one-slot ``noaa_storm_events_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.noaa_storm_events.capture import capture_release
from data_ingestion_toolbox.noaa_storm_events.config import StormEventsConfig
from data_ingestion_toolbox.noaa_storm_events.schema import (
    ensure_noaa_storm_events_schema,
)
from data_ingestion_toolbox.noaa_storm_events.silver_noaa_storm_events.load import (
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

    return PostgresHook(postgres_conn_id=StormEventsConfig().postgres_conn_id.strip())


@dag(
    dag_id="noaa_storm_events_ingest",
    description="Capture, replay changed events, place, count, and publish NOAA Storm Events",
    default_args=DEFAULT_ARGS,
    # Monthly on the 20th: NCEI restamps the current year's file each month
    # (c20261026 on 2026-11-11), and an already-loaded file name is not
    # fetched again.
    schedule="0 16 20 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "ncei", "storm-events", "capture-first"],
)
def noaa_storm_events_ingest():
    @task()
    def ensure_noaa_storm_events_schema_task() -> None:
        ensure_noaa_storm_events_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="noaa_storm_events_files")
    def ingest_batch_files() -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        # A retry after a failed replay reads the directory again under a
        # new run; finish the run it left behind first, oldest first.
        settled = settle_unreplayed_runs(connection_factory)
        run_id, statuses = capture_release(
            connection_factory, config=StormEventsConfig()
        )
        written = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "run_id": str(run_id),
            "files": statuses,
            "settled_runs": [str(settled_run) for settled_run in settled],
            "written": written,
            "published": published,
        }

    schema = ensure_noaa_storm_events_schema_task.override(
        task_id="ensure_noaa_storm_events_schema"
    )()
    geography = require_shared_geography()
    schema >> geography >> ingest_batch_files()


noaa_storm_events_ingest()
