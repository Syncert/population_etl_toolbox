"""Production DAG for registered Census Population Estimates bulk releases."""

from __future__ import annotations

import datetime as dt
import logging
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.silver_ref.geography_guard import (
    require_shared_geography_loaded,
)
from data_ingestion_toolbox import census_pep as pep_package
from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.census_pep.gold_pep.transform import (
    ensure_pep_gold_schema,
    refresh_pep_elements,
)
from data_ingestion_toolbox.census_pep.ingest import ingest_census_pep
from data_ingestion_toolbox.census_pep.registry import PEPRegistry
from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.normalization import sanitize_error_message

logger = logging.getLogger(__name__)
DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=20),
}


def _get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _ensure_silver_schema() -> None:
    sql_path = Path(pep_package.__file__).resolve().parent / "DDL" / "silver_pep.sql"
    with _get_postgres_hook().get_conn() as connection, connection.cursor() as cursor:
        cursor.execute(sql_path.read_text(encoding="utf-8"))
        connection.commit()


@dag(
    dag_id="census_pep_ingest",
    description="Capture, replay, conform, and publish registered Census PEP releases",
    default_args=DEFAULT_ARGS,
    schedule="0 6 1 * *",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["census", "pep", "population"],
)
def pep_ingest():
    @task()
    def ensure_silver_schema() -> None:
        _ensure_silver_schema()

    @task()
    def validate_geography_prerequisites() -> dict[str, int]:
        """Require production-scale canonical geography before observations.

        The shared minimum -- one nation, fifty states, three thousand
        counties -- is the one every source waits on, and it lives in
        `data_ingestion_toolbox.silver_ref.geography_guard` (DAG-020). PEP
        serves place-level estimates, so it adds the grain only it needs
        rather than restating the three it shares.
        """
        with _get_postgres_hook().get_conn() as connection:
            return require_shared_geography_loaded(
                connection, additional_minimums={"place": 18000}
            )

    @task(pool=CONFIG.airflow_pool)
    def discover_registered_releases() -> list[dict[str, object]]:
        registry = PEPRegistry(CONFIG)
        releases = [
            registry.get_current_release(dataset_code)
            for dataset_code in sorted(registry.datasets)
        ]
        if any(release is None for release in releases):
            raise RuntimeError("A registered PEP dataset has no published release")
        return [
            {
                "dataset_code": release.dataset_code,
                "vintage_year": release.vintage_year,
                "product_code": release.product_code,
            }
            for release in releases
            if release is not None
        ]

    @task(pool=CONFIG.airflow_pool)
    def ingest_registered_releases() -> int:
        try:
            return ingest_census_pep()
        except Exception as exc:
            logger.error("PEP ingestion failed: %s", sanitize_error_message(exc))
            raise

    @task()
    def transform_to_silver() -> int:
        return transform_pep_to_silver()

    @task()
    def publish_gold() -> int:
        ensure_pep_gold_schema()
        return refresh_pep_elements()

    @task()
    def validate_publication() -> dict[str, int]:
        with (
            _get_postgres_hook().get_conn() as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT COUNT(*) FROM gold_pep.population_estimate_revision")
            revisions = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM gold_pep.population_estimate_latest")
            latest = cursor.fetchone()[0]
            if revisions == 0 or latest == 0 or latest > revisions:
                raise RuntimeError(
                    f"PEP publication reconciliation failed: revisions={revisions}, latest={latest}"
                )
        return {"revisions": revisions, "latest": latest}

    @task()
    def emit_pep_publisher_ready() -> None:
        emit_latest_publisher_ready(
            _get_postgres_hook().get_conn,
            publisher_schema="gold_pep",
        )

    silver = ensure_silver_schema()
    geography = validate_geography_prerequisites()
    releases = discover_registered_releases()
    captured = ingest_registered_releases()
    transformed = transform_to_silver()
    published = publish_gold()
    validated = validate_publication()
    emitted = emit_pep_publisher_ready()

    (
        silver
        >> geography
        >> releases
        >> captured
        >> transformed
        >> published
        >> validated
        >> emitted
    )


pep_ingest_dag = pep_ingest()
