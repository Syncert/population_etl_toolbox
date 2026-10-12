"""Capture-first BEA regional accounts pipeline (bea-regional-accounts).

Each run captures every registered table's bulk zip -- county personal
income, its components, earnings by industry, and county GDP -- replays it
into silver and publishes it. BEA releases county income each November and
county GDP each December and revises earlier years with each release; a run
that finds the same bytes adds a capture and no observation, and a new
release is kept beside the one it revised.

One mapped task per table, through the one-slot ``bea_files`` pool: the
larger zips are tens of megabytes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.bea.capture import capture_table
from data_ingestion_toolbox.bea.config import BeaConfig
from data_ingestion_toolbox.bea.registry import TABLES, get_table
from data_ingestion_toolbox.bea.schema import ensure_bea_schema
from data_ingestion_toolbox.bea.silver_bea.load import publish_run, replay_run
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

    return PostgresHook(postgres_conn_id=BeaConfig().postgres_conn_id.strip())


@dag(
    dag_id="bea_regional_ingest",
    description="Capture, replay, reconcile, and publish BEA regional income and GDP tables",
    default_args=DEFAULT_ARGS,
    # Weekly: BEA's county releases land in November and December, and an
    # unchanged file costs one capture.
    schedule="0 14 * * 3",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bea", "income", "gdp", "capture-first"],
)
def bea_regional_ingest():
    @task()
    def ensure_bea_schema_task() -> None:
        ensure_bea_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task()
    def plan_tables() -> list[str]:
        return [table.code for table in TABLES]

    @task(pool="bea_files")
    def ingest_batch_table(table_code: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, _capture = capture_table(
            connection_factory, get_table(table_code), config=BeaConfig()
        )
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "table": table_code,
            "run_id": str(run_id),
            "facts": facts,
            "published": published,
        }

    schema = ensure_bea_schema_task.override(task_id="ensure_bea_schema")()
    geography = require_shared_geography()
    tables = plan_tables()
    schema >> geography >> tables
    ingest_batch_table.expand(table_code=tables)


bea_regional_ingest()
