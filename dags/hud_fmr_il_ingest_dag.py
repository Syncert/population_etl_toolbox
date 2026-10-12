"""Capture-first HUD Fair Market Rent and income-limit pipeline.

Each run reads every registered HUD User Data API read -- each fiscal year's
FMRs, state by state, and each year's income limits, county by county -- and,
when a read differs from its last published read, replays it into silver
and publishes it. An unchanged read replays nothing. HUD User challenges
automated workbook downloads, so the workbook path (still registered, with
its parser and tests) is for loading a file by hand, not for the schedule.

One mapped task per read, through the one-slot ``hud_fmr_il_files`` pool; an
income-limit read is about 3,300 calls at HUD's 60 a minute.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.hud_fmr_il.api import get_read, registered_reads
from data_ingestion_toolbox.hud_fmr_il.api_capture import capture_api_read
from data_ingestion_toolbox.hud_fmr_il.config import HudConfig
from data_ingestion_toolbox.hud_fmr_il.schema import ensure_hud_fmr_il_schema
from data_ingestion_toolbox.hud_fmr_il.silver_hud_fmr_il.load import (
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

    return PostgresHook(postgres_conn_id=HudConfig().postgres_conn_id.strip())


@dag(
    dag_id="hud_fmr_il_ingest",
    description="Capture, replay, reconcile, and publish HUD Fair Market Rents and income limits",
    default_args=DEFAULT_ARGS,
    # Monthly: HUD reissues FMRs within a fiscal year without a calendar, and
    # an unchanged workbook replays nothing.
    schedule="0 16 25 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["hud", "fmr", "income-limits", "housing", "capture-first"],
)
def hud_fmr_il_ingest():
    @task()
    def ensure_hud_fmr_il_schema_task() -> None:
        ensure_hud_fmr_il_schema(_get_postgres_hook())

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
        return [read.key for read in registered_reads()]

    @task(pool="hud_fmr_il_files", execution_timeout=timedelta(hours=3))
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_api_read(
            connection_factory, get_read(file_key), config=HudConfig.from_environment()
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

    schema = ensure_hud_fmr_il_schema_task.override(
        task_id="ensure_hud_fmr_il_schema"
    )()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


hud_fmr_il_ingest()
