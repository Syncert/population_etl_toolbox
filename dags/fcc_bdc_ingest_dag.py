"""Capture-first FCC Broadband Data Collection availability pipeline.

Each run reads every registered vintage (December 31 as-of dates): the
vintage's file listing, the national other-geographies summary and each
state's place summary, and, when the read differs from the vintage's last
published read, replays it into silver and publishes it. The FCC republishes
a vintage under a new revision date, so a revision is a new release.

One mapped task per vintage through the one-slot ``fcc_bdc_api`` pool; the
API allows 10 calls a minute, so a vintage's 58 calls take about seven
minutes.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.fcc_bdc.capture import capture_vintage
from data_ingestion_toolbox.fcc_bdc.config import BdcConfig
from data_ingestion_toolbox.fcc_bdc.registry import AS_OF_DATES
from data_ingestion_toolbox.fcc_bdc.schema import ensure_fcc_bdc_schema
from data_ingestion_toolbox.fcc_bdc.silver_fcc_bdc.load import publish_run, replay_run
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

    return PostgresHook(postgres_conn_id=BdcConfig().postgres_conn_id.strip())


@dag(
    dag_id="fcc_bdc_ingest",
    description="Capture, replay, reconcile, and publish FCC broadband availability summaries",
    default_args=DEFAULT_ARGS,
    # Monthly: the FCC republishes vintages under new revision dates as
    # challenges and corrections land, and an unchanged read replays nothing.
    schedule="0 21 12 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["fcc", "broadband", "capture-first"],
)
def fcc_bdc_ingest():
    @task()
    def ensure_fcc_bdc_schema_task() -> None:
        ensure_fcc_bdc_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task()
    def plan_vintages() -> list[str]:
        return [as_of.isoformat() for as_of in AS_OF_DATES]

    @task(pool="fcc_bdc_api", execution_timeout=timedelta(hours=1))
    def ingest_batch_vintage(as_of: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_vintage(
            connection_factory,
            date.fromisoformat(as_of),
            config=BdcConfig.from_environment(),
        )
        rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "as_of_date": as_of,
            "run_id": str(run_id),
            "status": status,
            "rows": rows,
            "published": published,
        }

    schema = ensure_fcc_bdc_schema_task.override(task_id="ensure_fcc_bdc_schema")()
    geography = require_shared_geography()
    vintages = plan_vintages()
    schema >> geography >> vintages
    ingest_batch_vintage.expand(as_of=vintages)


fcc_bdc_ingest()
