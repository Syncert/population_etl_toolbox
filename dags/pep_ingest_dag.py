# dags/pep_ingest_dag.py
#
# DAG SCRIPT (TaskFlow API) — Census PEP (Population Estimates) Ingestion
# --------------------------------------------------------------------------
# What this DAG does:
# 1) Fetches PEP annual/international files for configured years
# 2) Persists raw JSON payloads to raw_capture.response_capture
# 3) Transforms raw data to silver_pep.fact_population
# 4) Refreshes gold_pep serving layer
#
# REQUIRED DB TABLES:
# - raw_capture.response_capture
# - silver_pep.observation_revision
# - silver_pep.fact_population
# - silver_ref.dim_time, silver_ref.dim_geography
# - gold_pep (PEP tables)
#
# REQUIRED AIRFLOW POOL:
# - Create a pool named "census_api" in Airflow UI (Admin -> Pools) and set
#   its size conservatively (start with 4).
#
# ASSUMPTIONS:
# - data_ingestion_toolbox.census_pep.config.CONFIG exists and has:
#     CONFIG.postgres_conn_id : Airflow connection id for Postgres
#     CONFIG.source_code       : source identifier (e.g. "CENSUS_PEP")
#     CONFIG.years             : range of years to ingest
#     CONFIG.file_types        : tuple of file types (e.g. ("ansfile", "intlfile"))
# - data_ingestion_toolbox.census_pep.ingest provides:
#     ingest_census_pep(years, file_types) -> int
# - data_ingestion_toolbox.census_pep.silver_pep.transform provides:
#     transform_pep_to_silver() -> int
# - data_ingestion_toolbox.census_pep.gold_pep.transform provides:
#     ensure_pep_gold_schema()
#     refresh_pep_elements() -> int
#
# NOTE: gold_pep DDL includes refresh_rpt_pep_observations and refresh_mv_pep_latest
#       procedures. The serving layer refresh uses the generic utility function.

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_ingestion_toolbox import census_pep as pep_package
from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.census_pep.ingest import ingest_census_pep
from data_ingestion_toolbox.census_pep.silver_pep.transform import transform_pep_to_silver
from data_ingestion_toolbox.census_pep.gold_pep.transform import (
    ensure_pep_gold_schema,
    refresh_pep_elements,
)
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.normalization import sanitize_error_message
from data_ingestion_toolbox.utility.gold_schema import (
    ServingRefreshChunkConfig,
    refresh_serving_layer_in_year_chunks,
)

logger = logging.getLogger(__name__)

# -----------------------------
# Airflow defaults & constants
# -----------------------------
DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=20),
}

# Pool-based throttling: create this in Airflow UI (Admin -> Pools)
CENSUS_API_POOL = "census_api"


def _get_postgres_hook() -> PostgresHook:
    """Centralized PostgresHook factory."""
    conn_id = CONFIG.postgres_conn_id.strip()
    if not conn_id:
        raise RuntimeError("PostgreSQL connection ID is not configured")
    return PostgresHook(postgres_conn_id=conn_id)


def _silver_ddl_path() -> Path:
    return Path(pep_package.__file__).resolve().parent / "DDL" / "silver_pep.sql"


def _ensure_silver_schema() -> None:
    """Ensure silver_pep schema and tables exist."""
    sql_path = _silver_ddl_path()
    sql = sql_path.read_text(encoding="utf-8")
    hook = _get_postgres_hook()
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


# -----------------------------
# DAG factory
# -----------------------------
@dag(
    dag_id="census_pep_ingest",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.schedule_interval if hasattr(CONFIG, 'schedule_interval') else "0 6 1 * *",  # monthly on the 1st at 06:00
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["census", "pep"],
)
def pep_ingest():
    """Build and wire the PEP ingestion DAG."""

    # -----------------------------
    # Task 1: Sync PEP dataset availability (metadata check)
    # -----------------------------
    @task()
    def check_pep_api() -> dict:
        """Lightweight check that PEP API is reachable for configured years."""
        hook = _get_postgres_hook()
        results = {}
        for year in CONFIG.years:
            for ft in CONFIG.file_types:
                url = f"https://api.census.gov/data/{year}/pep/{ft}.json"
                try:
                    import httpx

                    with httpx.Client(timeout=10.0) as client:
                        resp = client.head(url, follow_redirects=True)
                        results[f"{year}/{ft}"] = {"status": resp.status_code, "url": url}
                except Exception as exc:
                    results[f"{year}/{ft}"] = {"status": None, "url": url, "error": str(exc)}
        return results

    # -----------------------------
    # Task 2: Ingest raw PEP payloads
    # -----------------------------
    @task(pool=CENSUS_API_POOL)
    def ingest_raw_pep() -> int:
        """Fetch and capture Census PEP annual/international files."""
        try:
            return ingest_census_pep(
                years=CONFIG.years,
                file_types=CONFIG.file_types,
            )
        except Exception as exc:
            logger.error("PEP ingestion failed: %s", sanitize_error_message(exc))
            raise

    # -----------------------------
    # Task 3: Silver layer
    # -----------------------------
    @task(trigger_rule="none_failed")
    def ensure_silver_schema() -> None:
        """Ensure silver_pep schema and tables exist."""
        _ensure_silver_schema()

    @task(trigger_rule="none_failed")
    def transform_to_silver() -> int:
        """Transform ALL raw PEP data to silver (full load)."""
        try:
            return transform_pep_to_silver()
        except Exception as exc:
            logger.error("PEP silver transform failed: %s", sanitize_error_message(exc))
            raise

    # -----------------------------
    # Task 4: Gold layer
    # -----------------------------
    @task(trigger_rule="none_failed")
    def ensure_gold_pep_schema() -> None:
        """Apply the source-specific gold_pep DDL."""
        ensure_pep_gold_schema()

    @task(trigger_rule="none_failed")
    def refresh_gold_geography() -> None:
        """Synchronize the shared current-geography table in a short transaction."""
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SET lock_timeout = '30s'")
            cur.execute("SET statement_timeout = '10min'")
            conn.commit()

    @task(trigger_rule="none_failed")
    def refresh_gold_pep_elements() -> int:
        """Refresh PEP dimensions and metric mappings in gold_pep."""
        return refresh_pep_elements()

    @task(trigger_rule="none_failed")
    def refresh_gold_pep_serving_layer() -> dict[str, int]:
        """Refresh changed PEP years as independently committed annual chunks."""
        return refresh_serving_layer_in_year_chunks(
            hook=_get_postgres_hook(),
            config=ServingRefreshChunkConfig(
                source_code="CENSUS_PEP",
                log_label="PEP",
                report_table="gold_pep.rpt_pep_observations",
                report_date_column="observation_date",
                changed_chunks_sql="""
                    SELECT
                        MAKE_DATE(s.estimate_year, 1, 1) AS chunk_start,
                        MAKE_DATE(s.estimate_year, 12, 31) AS chunk_end,
                        MAX(s.ingested_at) AS target_watermark
                    FROM silver_pep.fact_population s
                    WHERE s.estimate_value IS NOT NULL
                      AND s.ingested_at > %s
                    GROUP BY s.estimate_year
                    ORDER BY s.estimate_year
                """,
                report_procedure="gold_pep.refresh_rpt_pep_observations",
                latest_procedure="gold_pep.refresh_mv_pep_latest",
                statement_timeout="90min",
            ),
            task_logger=logger,
        )

    @task(trigger_rule="none_failed")
    def emit_pep_publisher_ready() -> None:
        """Append a durable outbox event without waiting for glossary harvest."""
        hook = _get_postgres_hook()
        emit_latest_publisher_ready(hook.get_conn, publisher_schema="gold_pep")

    # -----------------------------
    # DAG wiring
    # -----------------------------
    api_check = check_pep_api()
    raw_ingest = ingest_raw_pep()
    silver_schema = ensure_silver_schema()
    silver_transform = transform_to_silver()
    gold_schema = ensure_gold_pep_schema()
    gold_geo = refresh_gold_geography()
    gold_elements = refresh_gold_pep_elements()
    gold_refresh = refresh_gold_pep_serving_layer()
    publisher_ready = emit_pep_publisher_ready()

    # Execution order:
    # API check -> raw ingest -> silver schema -> silver transform -> gold schema -> gold geography -> gold elements -> gold refresh -> publisher ready
    api_check >> raw_ingest >> silver_schema >> silver_transform >> gold_schema >> gold_geo >> gold_elements >> gold_refresh >> publisher_ready


# Instantiate DAG
pep_ingest_dag = pep_ingest()
