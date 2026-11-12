"""Capture-first Monthly Treasury Statement (Table 9) pipeline.

Each run reads Table 9 from the Fiscal Data API, from three months before the
newest statement already published (Treasury can restate a recent month), or
from March 2015 when nothing is published or ``{"history": true}`` is passed.
Every page is captured, then replayed into silver -- writing only the lines
whose published row changed -- reconciled, and published. Treasury releases
the statement on the eighth business day of the following month, so the DAG
runs monthly after that.

One task through the one-slot ``treasury_mts_pages`` pool.
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
from data_ingestion_toolbox.treasury_mts.capture import capture_statements
from data_ingestion_toolbox.treasury_mts.config import TreasuryMtsConfig
from data_ingestion_toolbox.treasury_mts.schema import ensure_treasury_mts_schema
from data_ingestion_toolbox.treasury_mts.silver_treasury_mts.load import (
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

    return PostgresHook(postgres_conn_id=TreasuryMtsConfig().postgres_conn_id.strip())


@dag(
    dag_id="treasury_mts_ingest",
    description="Capture, replay changed lines, reconcile, and publish Monthly Treasury Statement Table 9",
    default_args=DEFAULT_ARGS,
    # Monthly on the 16th: the eighth business day of the month has passed,
    # so the prior month's statement is out.
    schedule="0 16 16 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["treasury", "fiscal-data", "mts", "capture-first"],
)
def treasury_mts_ingest():
    @task()
    def ensure_treasury_mts_schema_task() -> None:
        ensure_treasury_mts_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task(pool="treasury_mts_pages")
    def ingest_batch_statements() -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        conf = dict(get_current_context()["dag_run"].conf or {})
        # A retry after a failed replay reads again under a new run; finish
        # the run it left behind first, oldest first.
        settled = settle_unreplayed_runs(connection_factory)
        run_id, pages = capture_statements(
            connection_factory, conf=conf, config=TreasuryMtsConfig()
        )
        written = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "run_id": str(run_id),
            "pages": pages,
            "settled_runs": [str(settled_run) for settled_run in settled],
            "written": written,
            "published": published,
        }

    schema = ensure_treasury_mts_schema_task.override(
        task_id="ensure_treasury_mts_schema"
    )()
    geography = require_shared_geography()
    schema >> geography >> ingest_batch_statements()


treasury_mts_ingest()
