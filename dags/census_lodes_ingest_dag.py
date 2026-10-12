"""Capture-first LEHD LODES pipeline (census-lehd-lodes).

Each run takes every state for the configured newest years: it captures the
state's vintage and checksum list, and only when the vintage is new for that
state-year fetches the residence, workplace and origin-destination files,
checks each against the published checksum, sums the blocks to counties,
and publishes them. A run that finds the vintage it already published
fetches nothing else.

One mapped task per state-year, through the one-slot ``census_lodes_files``
pool: a large state's origin-destination file is tens of megabytes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.census_lodes.capture import capture_state_year
from data_ingestion_toolbox.census_lodes.config import LodesConfig
from data_ingestion_toolbox.census_lodes.registry import STATES, YEARS
from data_ingestion_toolbox.census_lodes.schema import ensure_census_lodes_schema
from data_ingestion_toolbox.census_lodes.silver_census_lodes.load import (
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

    return PostgresHook(postgres_conn_id=LodesConfig().postgres_conn_id.strip())


@dag(
    dag_id="census_lodes_ingest",
    description="Capture, verify, aggregate, and publish LEHD LODES county job counts",
    default_args=DEFAULT_ARGS,
    # Monthly: LODES vintages arrive about once a year, and a state whose
    # vintage is unchanged costs two small captures.
    schedule="0 14 20 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["census", "lodes", "commuting", "capture-first"],
)
def census_lodes_ingest():
    @task()
    def ensure_census_lodes_schema_task() -> None:
        ensure_census_lodes_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task()
    def plan_slices() -> list[str]:
        years = sorted(YEARS)[-LodesConfig().recent_years :]
        return [f"{state}:{year}" for year in years for state, _fips in STATES]

    @task(pool="census_lodes_files")
    def ingest_batch_slice(slice_key: str) -> dict[str, Any]:
        state, year = slice_key.split(":")
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_state_year(
            connection_factory, state, int(year), config=LodesConfig()
        )
        rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "slice": slice_key,
            "run_id": str(run_id),
            "status": status,
            "rows": rows,
            "published": published,
        }

    schema = ensure_census_lodes_schema_task.override(
        task_id="ensure_census_lodes_schema"
    )()
    geography = require_shared_geography()
    slices = plan_slices()
    schema >> geography >> slices
    ingest_batch_slice.expand(slice_key=slices)


census_lodes_ingest()
