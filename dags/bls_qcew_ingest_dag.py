"""Capture-first BLS QCEW pipeline (bls-qcew-county-wages).

Each run asks for the registered industry slices of the last few calendar
quarters and of the annual averages of the years they fall in
(``QcewConfig.recent_quarters``). A quarter QCEW has not published yet
answers 404 and is recorded empty, so the run that first finds it published
needs no release calendar. A manual run with ``{"history": true}`` in its
conf sweeps every registered period from 2014, which is how a fresh
warehouse is loaded (``docs/reference/BETA_RESET_REINGESTION.md``).

One mapped task per (year, period): capture every industry slice, replay
the run into silver, publish it. Requests go through the ``bls_qcew_api``
pool, one at a time, because data.bls.gov throttles bursts.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.bls_qcew.capture import capture_period
from data_ingestion_toolbox.bls_qcew.config import QcewConfig
from data_ingestion_toolbox.bls_qcew.registry import recent_periods, registered_periods
from data_ingestion_toolbox.bls_qcew.schema import ensure_bls_qcew_schema
from data_ingestion_toolbox.bls_qcew.silver_bls_qcew.load import publish_run, replay_run
from data_ingestion_toolbox.silver_ref.geography_guard import (
    require_shared_geography_loaded,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


def _get_postgres_hook():  # noqa: ANN202
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=QcewConfig().postgres_conn_id.strip())


def periods_to_capture(
    today: date, conf: dict[str, Any] | None, config: QcewConfig
) -> list[str]:
    """The `year-period` keys one run captures: recent by default, all on request."""
    if conf and conf.get("history"):
        quarter = (today.month - 1) // 3
        year = today.year if quarter else today.year - 1
        periods = registered_periods(year, quarter or 4)
    else:
        periods = recent_periods(today, config.recent_quarters)
    return [f"{year}-{period}" for year, period in periods]


@dag(
    dag_id="bls_qcew_ingest",
    description="Capture, replay, reconcile, and publish BLS QCEW county employment and wages",
    default_args=DEFAULT_ARGS,
    # Monthly: QCEW publishes one quarter at a time, about five months after
    # it ends, and a run that finds the same bytes adds no observation.
    schedule="0 12 20 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bls", "qcew", "employment", "capture-first"],
)
def bls_qcew_ingest():
    @task()
    def ensure_bls_qcew_schema_task() -> None:
        ensure_bls_qcew_schema(_get_postgres_hook())

    @task()
    def require_shared_geography() -> None:
        with _get_postgres_hook().get_conn() as connection:
            counts = require_shared_geography_loaded(connection)
        logging.getLogger("airflow.task").info(
            "shared geography reference is loaded: %s",
            ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
        )

    @task()
    def plan_periods(**context: Any) -> list[str]:
        dag_run = context.get("dag_run")
        conf = dict(getattr(dag_run, "conf", None) or {})
        logical = context.get("logical_date") or datetime.now(timezone.utc)
        return periods_to_capture(logical.date(), conf, QcewConfig())

    @task(pool="bls_qcew_api")
    def ingest_batch_period(period_key: str) -> dict[str, Any]:
        year_text, period = period_key.split("-", 1)
        connection_factory = _get_postgres_hook().get_conn
        run_id, slices = capture_period(
            connection_factory, int(year_text), period, config=QcewConfig()
        )
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "period": period_key,
            "run_id": str(run_id),
            "slices": len(slices),
            "published_slices": published,
            "facts": facts,
        }

    schema = ensure_bls_qcew_schema_task.override(task_id="ensure_bls_qcew_schema")()
    geography = require_shared_geography()
    periods = plan_periods()
    schema >> geography >> periods
    ingest_batch_period.expand(period_key=periods)


bls_qcew_ingest()
