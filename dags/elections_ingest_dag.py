"""Capture-first elections pipeline: seat holders, governors and MEDSL results.

Each run reads every stream whole -- the congress-legislators files, the
Wikidata governor query and the four MEDSL results files on Harvard
Dataverse -- replays each into silver and publishes it. A read whose files
match the last published read replays nothing. Because every stream is
re-read whole, an election or a Congress the warehouse never loaded is
loaded by the next scheduled run (the loader rule).

One task per stream, through the one-slot ``elections_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.elections.capture import capture_stream
from data_ingestion_toolbox.elections.config import ElectionsConfig
from data_ingestion_toolbox.elections.registry import STREAMS
from data_ingestion_toolbox.elections.schema import ensure_elections_schema
from data_ingestion_toolbox.elections.silver_elections.load import (
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

    return PostgresHook(postgres_conn_id=ElectionsConfig().postgres_conn_id.strip())


def _ingest(stream: str) -> dict[str, Any]:
    connection_factory = _get_postgres_hook().get_conn
    run_id, status = capture_stream(
        connection_factory, stream, config=ElectionsConfig.from_environment()
    )
    rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
    published = publish_run(connection_factory, run_id=UUID(str(run_id)))
    return {
        "stream": stream,
        "run_id": str(run_id),
        "status": status,
        "rows": rows,
        "published": published,
    }


@dag(
    dag_id="elections_ingest",
    description="Capture, replay, reconcile, and publish seat terms, governors and MEDSL election results",
    default_args=DEFAULT_ARGS,
    # Monthly, on the 3rd: members change between elections (deaths,
    # resignations, appointments) and MEDSL republishes after certification.
    # An unchanged read replays nothing.
    schedule="0 7 3 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["elections", "medsl", "congress", "capture-first"],
)
def elections_ingest():
    @task()
    def ensure_elections_schema_task() -> None:
        ensure_elections_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    schema = ensure_elections_schema_task.override(task_id="ensure_elections_schema")()
    geography = require_shared_geography()
    ingests = []
    for stream in STREAMS:

        @task(task_id=f"ingest_batch_{stream}", pool="elections_files")
        def ingest(stream: str = stream) -> dict[str, Any]:
            return _ingest(stream)

        ingests.append(ingest())
    schema >> geography >> ingests


elections_ingest()
