"""Capture-first EIA weekly retail gasoline pipeline (grocery-and-gasoline-prices).

Each run reads a window of weeks of EIA's retail gasoline prices -- regular,
midgrade, premium and all grades for the nation, the PADDs and their
sub-districts, nine states and ten cities -- captures every page, replays it
into silver and publishes it. EIA publishes each Monday's prices that
Monday afternoon; the run is Tuesday. The first run reads the whole history
from 2015; later runs go back eight weeks, so a revised week is read again
and kept beside the first reading.

The key is ``EIA_API_KEY``, read from the environment when the capture runs,
never at import. Requests go through the one-slot ``eia_api`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.eia.capture import capture_window, plan_window
from data_ingestion_toolbox.eia.config import EiaConfig
from data_ingestion_toolbox.eia.schema import ensure_eia_schema
from data_ingestion_toolbox.eia.silver_eia.load import publish_run, replay_run
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

    return PostgresHook(postgres_conn_id=EiaConfig().postgres_conn_id.strip())


@dag(
    dag_id="eia_retail_gasoline_ingest",
    description="Capture, replay, reconcile, and publish EIA weekly retail gasoline prices",
    default_args=DEFAULT_ARGS,
    # Tuesdays: EIA publishes Monday's prices on Monday afternoon.
    schedule="0 15 * * 2",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["eia", "gasoline", "prices", "capture-first"],
)
def eia_retail_gasoline_ingest():
    @task()
    def ensure_eia_schema_task() -> None:
        ensure_eia_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="eia_api")
    def load_eia_areas() -> int:
        # EIA's PADDs and cities, from the route's own area facet: the list
        # needs the key, so this DAG loads it rather than the reference DAG.
        from data_ingestion_toolbox.silver_ref.provider_areas import sync_provider_areas

        return sync_provider_areas("eia")["provider_areas"]

    @task(pool="eia_api")
    def ingest_batch_weeks() -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        config = EiaConfig.from_environment()
        start = plan_window(connection_factory, config)
        run_id = capture_window(connection_factory, start, config=config)
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "start_week": start.isoformat(),
            "run_id": str(run_id),
            "facts": facts,
            "published": published,
        }

    schema = ensure_eia_schema_task.override(task_id="ensure_eia_schema")()
    geography = require_shared_geography()
    schema >> geography >> load_eia_areas() >> ingest_batch_weeks()


eia_retail_gasoline_ingest()
