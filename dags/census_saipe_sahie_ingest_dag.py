"""Capture-first Census SAIPE and SAHIE pipeline (census-saipe-sahie).

Each run captures the two newest registered estimate years of each dataset --
the Bureau revises nothing older in place, it publishes the next year -- one
request per grain, and every byte is committed before anything parses it. A
manual run with ``{"history": true}`` in its conf sweeps every registered
year instead, which is how a fresh warehouse is loaded
(``docs/reference/BETA_RESET_REINGESTION.md``).

The requests share the ``census_api`` pool with the ACS: one host, one key,
one rate limit.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion_toolbox.census_saipe_sahie.capture import capture_dataset_year
from data_ingestion_toolbox.census_saipe_sahie.config import SaeConfig
from data_ingestion_toolbox.census_saipe_sahie.registry import DATASETS, get_dataset
from data_ingestion_toolbox.census_saipe_sahie.schema import ensure_census_sae_schema
from data_ingestion_toolbox.census_saipe_sahie.silver_census_sae.load import (
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
    "retry_delay": timedelta(minutes=10),
}

#: Estimate years an ordinary run captures per dataset, newest first.
RECENT_YEARS = 2


def _get_postgres_hook():  # noqa: ANN202
    conn_id = SaeConfig.from_environment().postgres_conn_id.strip()
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=conn_id)


def _require_shared_geography() -> None:
    with _get_postgres_hook().get_conn() as connection:
        counts = require_shared_geography_loaded(connection)
    logging.getLogger("airflow.task").info(
        "shared geography reference is loaded: %s",
        ", ".join(f"{grain}={count}" for grain, count in sorted(counts.items())),
    )


def _ensure_schema() -> None:
    ensure_census_sae_schema(_get_postgres_hook())


def years_to_capture(dataset_id: str, conf: dict[str, Any] | None) -> list[int]:
    """The registered years one run captures: recent by default, all on request."""
    dataset = get_dataset(dataset_id)
    if conf and conf.get("history"):
        return list(dataset.years)
    return list(dataset.years[-RECENT_YEARS:])


def _capture(dataset_id: str, **context: Any) -> list[str]:
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    dataset = get_dataset(dataset_id)
    connection_factory = _get_postgres_hook().get_conn
    config = SaeConfig.from_environment()
    run_ids = []
    for year in years_to_capture(dataset_id, conf):
        run_id, _slices = capture_dataset_year(
            connection_factory, dataset, year, config=config
        )
        run_ids.append(str(run_id))
    return run_ids


def _replay(dataset_id: str, run_ids: list[str]) -> int:
    dataset = get_dataset(dataset_id)
    connection_factory = _get_postgres_hook().get_conn
    return sum(
        replay_run(connection_factory, run_id=UUID(run_id), dataset=dataset)
        for run_id in run_ids
    )


def _publish(run_ids: list[str]) -> int:
    connection_factory = _get_postgres_hook().get_conn
    return sum(
        publish_run(connection_factory, run_id=UUID(run_id)) for run_id in run_ids
    )


with DAG(
    dag_id="census_saipe_sahie_ingest",
    description="Capture, replay, reconcile, and publish Census SAIPE and SAHIE estimates",
    default_args=DEFAULT_ARGS,
    # Monthly: SAIPE publishes each December and SAHIE each spring, and a run
    # that finds the same bytes adds a capture and no estimate.
    schedule="0 6 15 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["census", "saipe", "sahie", "capture-first"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_census_sae_schema",
        python_callable=_ensure_schema,
    )
    require_shared_geography = PythonOperator(
        task_id="require_shared_geography",
        python_callable=_require_shared_geography,
    )
    for registered in DATASETS:
        capture = PythonOperator(
            task_id=f"ingest_batch_{registered.dataset_id}",
            python_callable=_capture,
            op_kwargs={"dataset_id": registered.dataset_id},
            pool="census_api",
        )
        replay = PythonOperator(
            task_id=f"replay_{registered.dataset_id}",
            python_callable=_replay,
            op_kwargs={"dataset_id": registered.dataset_id, "run_ids": capture.output},
        )
        publish = PythonOperator(
            task_id=f"publish_{registered.dataset_id}",
            python_callable=_publish,
            op_kwargs={"run_ids": capture.output},
        )
        ensure_schema >> require_shared_geography >> capture >> replay >> publish
