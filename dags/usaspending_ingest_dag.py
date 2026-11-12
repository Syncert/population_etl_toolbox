"""Capture-first USAspending federal award obligations by place of performance.

Each run reads the current and prior federal fiscal years again (agencies
are still reporting them): for the state and county layers, every award,
each award type group, and each awarding agency the year's agency list
names. ``{"history": true}`` also reads, oldest year first, every request of
fiscal 2008 onward never captured before, up to ``max_requests`` (default
1,500) a run; trigger it again to continue an unfinished history load.
Every answer is captured, then replayed into silver -- writing only the rows
whose published text changed -- reconciled, and published.

One task through the one-slot ``usaspending_api`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from data_ingestion_toolbox.silver_ref.geography_guard import (
    require_shared_geography_loaded,
)
from data_ingestion_toolbox.usaspending.capture import capture_awards
from data_ingestion_toolbox.usaspending.config import UsaspendingConfig
from data_ingestion_toolbox.usaspending.schema import ensure_usaspending_schema
from data_ingestion_toolbox.usaspending.silver_usaspending.load import (
    publish_run,
    replay_run,
    settle_unreplayed_runs,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=20),
}


def _get_postgres_hook():  # noqa: ANN202
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=UsaspendingConfig().postgres_conn_id.strip())


@dag(
    dag_id="usaspending_ingest",
    description="Capture, replay changed rows, reconcile, and publish USAspending award obligations by place",
    default_args=DEFAULT_ARGS,
    # Monthly on the 20th: the current and prior fiscal years are read again
    # each month. Every changed amount of a refresh year is kept as a
    # revision, so a weekly read would keep about four times the revisions
    # for the same annual figures.
    schedule="0 9 20 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["usaspending", "treasury", "federal-awards", "capture-first"],
)
def usaspending_ingest():
    @task()
    def ensure_usaspending_schema_task() -> None:
        ensure_usaspending_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="usaspending_api", execution_timeout=timedelta(hours=6))
    def ingest_batch_awards() -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        dag_run = get_current_context()["dag_run"]
        conf = dict(dag_run.conf or {})
        # A retry after a failed replay reads again under a new run; finish
        # the run it left behind first, oldest first.
        settled = settle_unreplayed_runs(connection_factory)
        # A retry of this run skips the refresh-year reads its earlier
        # attempts already captured (they were replayed just above).
        run_id, requests = capture_awards(
            connection_factory,
            conf=conf,
            config=UsaspendingConfig(),
            refresh_since=dag_run.start_date,
        )
        written = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "run_id": str(run_id),
            "requests": requests,
            "settled_runs": [str(settled_run) for settled_run in settled],
            "written": written,
            "published": published,
        }

    schema = ensure_usaspending_schema_task.override(
        task_id="ensure_usaspending_schema"
    )()
    geography = require_shared_geography()
    schema >> geography >> ingest_batch_awards()


usaspending_ingest()
