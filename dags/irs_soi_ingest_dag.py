"""Capture-first IRS SOI county and state income pipeline (irs-soi).

Each run captures the files of the newest two registered tax years -- SOI
posts a corrected file now and then -- and every file of each registered
year the warehouse has never had published, so the first scheduled run on
a fresh warehouse loads every registered year without anyone asking and
later runs read two years (``data_ingestion_toolbox.utility.backfill``). A
manual run with ``{"history": true}`` re-reads every registered year.

One mapped task per file, through the one-slot ``irs_soi_files`` pool the
migration files share: one host, read one file at a time.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from data_ingestion_toolbox.irs_soi.capture import capture_file
from data_ingestion_toolbox.irs_soi.config import IrsSoiConfig
from data_ingestion_toolbox.irs_soi.plan import answered_years, files_to_capture
from data_ingestion_toolbox.irs_soi.registry import get_file
from data_ingestion_toolbox.irs_soi.schema import ensure_irs_soi_schema
from data_ingestion_toolbox.irs_soi.silver_irs_soi.load import (
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

    return PostgresHook(postgres_conn_id=IrsSoiConfig().postgres_conn_id.strip())


@dag(
    dag_id="irs_soi_ingest",
    description="Capture, replay, reconcile, and publish IRS SOI county and state income data",
    default_args=DEFAULT_ARGS,
    # Monthly: SOI posts a new tax year about once a year, and a run past
    # the history reads the two newest years again.
    schedule="0 15 7 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["irs", "soi", "income", "tax", "capture-first"],
)
def irs_soi_ingest():
    @task()
    def ensure_irs_soi_schema_task() -> None:
        ensure_irs_soi_schema(_get_postgres_hook())

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
        dag_run = get_current_context().get("dag_run")
        conf = dict(getattr(dag_run, "conf", None) or {})
        with _get_postgres_hook().get_conn() as connection:
            answered = answered_years(connection)
        files = files_to_capture(conf, answered)
        logging.getLogger("airflow.task").info(
            "capturing %d file(s) (%s); %d tax year(s) already answered",
            len(files),
            ", ".join(item.key for item in files),
            len(answered),
        )
        return [item.key for item in files]

    @task(pool="irs_soi_files")
    def ingest_batch_file(file_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, _capture = capture_file(
            connection_factory, get_file(file_key), config=IrsSoiConfig()
        )
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "file": file_key,
            "run_id": str(run_id),
            "facts": facts,
            "published": published,
        }

    schema = ensure_irs_soi_schema_task.override(task_id="ensure_irs_soi_schema")()
    geography = require_shared_geography()
    files = plan_files()
    schema >> geography >> files
    ingest_batch_file.expand(file_key=files)


irs_soi_ingest()
