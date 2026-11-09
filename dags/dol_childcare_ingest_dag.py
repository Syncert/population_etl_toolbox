"""Capture-first DOL Women's Bureau county childcare prices pipeline (county-childcare-prices).

Each run reads every registered NDCP release from DOL's data portal API
(www.dol.gov refuses the warehouse's client), with the key the deployment
holds as ``DOL_API_KEY``. A release never
published is loaded whole on the first scheduled run (LOADER RULE: every
study year the workbook holds, with nobody asking); a later read whose
bytes repeat the last published capture is recorded ``unchanged`` and
replays nothing, and a changed workbook stores only the prices that changed
(CHURN RULE). One mapped task per release, through the one-slot
``dol_childcare_files`` pool: one host, one release's pages at a time.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.dol_childcare.capture import capture_release
from data_ingestion_toolbox.dol_childcare.config import ChildcareConfig
from data_ingestion_toolbox.dol_childcare.registry import RELEASES, get_release
from data_ingestion_toolbox.dol_childcare.schema import ensure_dol_childcare_schema
from data_ingestion_toolbox.dol_childcare.silver_dol_childcare.load import (
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


def _get_postgres_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=ChildcareConfig().postgres_conn_id.strip())


@dag(
    dag_id="dol_childcare_ingest",
    description="Capture, replay, reconcile, and publish DOL county childcare prices",
    default_args=DEFAULT_ARGS,
    # Monthly: the Women's Bureau publishes a release every year or two, and
    # an unchanged workbook replays nothing.
    schedule="0 16 9 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["dol", "childcare", "prices", "cost-of-living", "capture-first"],
)
def dol_childcare_ingest():
    @task()
    def ensure_dol_childcare_schema_task() -> None:
        ensure_dol_childcare_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task()
    def plan_releases() -> list[str]:
        keys = [release.key for release in RELEASES]
        logging.getLogger("airflow.task").info(
            "reading %d NDCP release(s): %s", len(keys), ", ".join(keys)
        )
        return keys

    @task(pool="dol_childcare_files")
    def ingest_batch_release(release_key: str) -> dict[str, Any]:
        connection_factory = _get_postgres_hook().get_conn
        run_id, status = capture_release(
            connection_factory, get_release(release_key), config=ChildcareConfig()
        )
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "release": release_key,
            "run_id": str(run_id),
            "status": status,
            "facts": facts,
            "published": published,
        }

    schema = ensure_dol_childcare_schema_task.override(
        task_id="ensure_dol_childcare_schema"
    )()
    geography = require_shared_geography()
    releases = plan_releases()
    schema >> geography >> releases
    ingest_batch_release.expand(release_key=releases)


dol_childcare_ingest()
