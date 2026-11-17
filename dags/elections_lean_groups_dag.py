"""Rebuild the election lean groups when any of their inputs published.

The lean groups (ADR-0014, ``gold_elections.lean_place_change`` and
``gold_elections.lean_group_change``) are derived from the presidential vote
and five economic measures from four other sources. Each source publishes on
its own schedule, so this runs daily and rebuilds only when the inputs'
fingerprint changed; an unchanged day records ``unchanged`` and writes
nothing else.

It is a separate DAG rather than a task of ``elections_ingest`` because that
DAG must stay monthly: each MEDSL county or House download answers Harvard
Dataverse's guestbook with the operator's identity.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.decorators import dag, task

from data_ingestion_toolbox.elections.config import ElectionsConfig
from data_ingestion_toolbox.elections.lean_groups import refresh_lean_groups
from data_ingestion_toolbox.elections.schema import ensure_elections_schema

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}


def _get_postgres_hook():  # noqa: ANN202
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=ElectionsConfig().postgres_conn_id.strip())


@dag(
    dag_id="elections_lean_groups",
    description="Rebuild the election lean groups (ADR-0014) when an input source published",
    default_args=DEFAULT_ARGS,
    # Daily, after the morning ingests; a day with no new input publication
    # rebuilds nothing.
    schedule="0 13 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["elections", "derived", "adr-0014"],
)
def elections_lean_groups():
    @task()
    def ensure_elections_schema_task() -> None:
        ensure_elections_schema(_get_postgres_hook())

    @task(execution_timeout=timedelta(minutes=60))
    def refresh() -> dict[str, Any]:
        outcome = refresh_lean_groups(_get_postgres_hook().get_conn)
        logging.getLogger("airflow.task").info("lean groups: %s", outcome)
        return outcome

    schema = ensure_elections_schema_task.override(task_id="ensure_elections_schema")()
    schema >> refresh.override(task_id="refresh_lean_groups")()


elections_lean_groups()
