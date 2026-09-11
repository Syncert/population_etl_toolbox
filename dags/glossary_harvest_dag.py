"""Independent event-driven glossary harvest and reconciliation DAGs."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.glossary.harvest import (
    harvest_all_publishers,
    process_pending_events,
    reconciliation_arguments,
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
    def reconcile_all_publishers(**context) -> dict[str, int | str]:
        """Reconcile every publisher, or repair one on an operator request.

        Two optional ``dag_run.conf`` keys, both unset on every scheduled run:

        - ``force``: re-harvest even where the publisher has published nothing
          newer and says exactly what it said last time. Needed after a change
          to what a publisher *says* rather than to its facts -- a metric
          identity, units, grains -- since no fact watermark moves for those.
        - ``schemas``: a list of publisher schemas (for example
          ``["gold_bls"]``) to limit the run to, so a repair does not rewrite
          every source's catalog.
        """
        conf = getattr(context.get("dag_run"), "conf", None)
        return harvest_all_publishers(
            _connection_factory, **reconciliation_arguments(conf)
        )

    @task
    def refresh_shared_geography() -> None:
        """Refresh the glossary-owned geography projection independently."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("CALL gold_glossary.refresh_dim_geo_latest()")

    reconcile_all_publishers() >> refresh_shared_geography()


glossary_harvest_dag = glossary_harvest()
glossary_reconciliation_dag = glossary_reconciliation()
