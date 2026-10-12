"""Capture-first Census Building Permits Survey pipeline (census-building-permits).

Each run asks for the registered county and state files of the last few
calendar months, and the annual county, state and place files of the years
they fall in (``BpsConfig.recent_months``). A month not yet published answers
404 and is recorded empty. A manual run with ``{"history": true}`` sweeps
every registered period from 2000 (places from 2007), which is how a fresh
warehouse is loaded (``docs/reference/BETA_RESET_REINGESTION.md``).

One mapped task per (frequency, year, month): capture every file, replay the
run into silver, publish it. Requests go through the one-slot
``census_bps_files`` pool.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow.decorators import dag, task

from data_ingestion_toolbox.census_bps.capture import capture_period
from data_ingestion_toolbox.census_bps.config import BpsConfig
from data_ingestion_toolbox.census_bps.registry import (
    recent_periods,
    registered_periods,
)
from data_ingestion_toolbox.census_bps.schema import ensure_census_bps_schema
from data_ingestion_toolbox.census_bps.silver_census_bps.load import (
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
    "retry_delay": timedelta(minutes=15),
}


def _get_postgres_hook():  # noqa: ANN202
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=BpsConfig().postgres_conn_id.strip())


def periods_to_capture(
    today: date, conf: dict[str, Any] | None, config: BpsConfig
) -> list[str]:
    """The `frequency-year-month` keys one run captures: recent by default, all on request."""
    if conf and conf.get("history"):
        periods = registered_periods(today)
    else:
        periods = recent_periods(today, config.recent_months)
    return [f"{frequency}-{year}-{month:02d}" for frequency, year, month in periods]


@dag(
    dag_id="census_building_permits_ingest",
    description="Capture, replay, reconcile, and publish Census Building Permits Survey files",
    default_args=DEFAULT_ARGS,
    # Monthly, after the Bureau's mid-month release of the prior month.
    schedule="0 13 25 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["census", "building-permits", "housing", "capture-first"],
)
def census_building_permits_ingest():
    @task()
    def ensure_census_bps_schema_task() -> None:
        ensure_census_bps_schema(_get_postgres_hook())

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
        return periods_to_capture(logical.date(), conf, BpsConfig())

    @task(pool="census_bps_files")
    def ingest_batch_period(period_key: str) -> dict[str, Any]:
        frequency, year_text, month_text = period_key.split("-")
        connection_factory = _get_postgres_hook().get_conn
        run_id, files = capture_period(
            connection_factory,
            frequency,
            int(year_text),
            int(month_text),
            config=BpsConfig(),
        )
        facts = replay_run(connection_factory, run_id=UUID(str(run_id)))
        published = publish_run(connection_factory, run_id=UUID(str(run_id)))
        return {
            "period": period_key,
            "run_id": str(run_id),
            "files": len(files),
            "published": published,
            "facts": facts,
        }

    schema = ensure_census_bps_schema_task.override(
        task_id="ensure_census_bps_schema"
    )()
    geography = require_shared_geography()
    periods = plan_periods()
    schema >> geography >> periods
    ingest_batch_period.expand(period_key=periods)


census_building_permits_ingest()
