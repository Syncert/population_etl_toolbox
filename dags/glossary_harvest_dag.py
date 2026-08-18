"""Independent event-driven glossary harvest and reconciliation DAGs."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.glossary.harvest import (
    harvest_all_publishers,
    process_pending_events,
)

POSTGRES_CONN_ID = "public_data"


def _connection_factory():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


@dag(
    dag_id="glossary_harvest",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["glossary", "publisher-events"],
)
def glossary_harvest():
    @task
    def harvest_pending_events() -> int:
        return process_pending_events(_connection_factory)

    harvest_pending_events()


@dag(
    dag_id="glossary_reconciliation",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["glossary", "reconciliation"],
)
def glossary_reconciliation():
    @task
    def reconcile_all_publishers() -> dict[str, int | str]:
        return harvest_all_publishers(_connection_factory)

    reconcile_all_publishers()


glossary_harvest_dag = glossary_harvest()
glossary_reconciliation_dag = glossary_reconciliation()
