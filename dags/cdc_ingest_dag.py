# dags/cdc_ingest_dag.py
#
# DAG SCRIPT (TaskFlow API) — CDC Illness and Disease Data
# -------------------------------------------------------------------
# What this DAG does:
# 1) Syncs which CDC datasets are available (illness and disease indicators)
# 2) Selects every available year for each configured dataset
# 3) Builds a slice plan for US + State + County-by-State
# 4) Skips slices already completed *for the current variable set*
#    - If you change the curated variable list in config.py, variables_hash changes
#    - Any previously completed slices with an old hash become "stale" and are re-run
# 5) Uses a Pool ("cdc_api") to limit concurrency and respect CDC API limits
# 6) Tracks status/rows/errors in control.cdc_ingestion_slices
#
# REQUIRED DB TABLES:
# - raw_cdc.cdc_datasets          (filtered to illness and disease indicators only)
# - control.cdc_ingestion_slices  (ledger)
# - raw_capture.response_capture -> silver_cdc.observation_revision
#
# REQUIRED AIRFLOW POOLS:
# - Create a pool named "cdc_api" in Airflow UI and set its size conservatively (start with 4).
#   This is the macro-level rate limiter that prevents your executor from stampeding the CDC API.
# ASSUMPTIONS ABOUT YOUR CODEBASE:
# - data_ingestion_toolbox.cdc.config.CONFIG exists and has:
#     CONFIG.postgres_conn_id : Airflow connection id for Postgres
#     CONFIG.datasets         : list like ["cdc_illness_disease"]
# - data_ingestion_toolbox.cdc.metadata provides:
#     sync_cdc_dataset_table()
#     sync_variable_metadata_for_year(year: int, dataset: str)
# - data_ingestion_toolbox.cdc.client provides:
#     make_request(asset_id, page_size, page_cursor, app_token)
# - data_ingestion_toolbox.cdc.capture provides:
#     commit_capture(response_bytes, metadata, asset_id, release_version, capture_timestamp)
# - data_ingestion_toolbox.cdc.silver_cdc.transform provides:
#     transform_cdc_to_silver()
# - data_ingestion_toolbox.cdc.gold_cdc.publisher provides:
#     publish_glossary()
# - data_ingestion_toolbox.utility.gold_schema provides:
#     ServingRefreshChunkConfig,
#     refresh_serving_layer_in_year_chunks
#
# IMPORTANT NOTE ABOUT county_asset_id NULLABILITY:
# - This script uses ON CONFLICT (dataset, year, geo_level, county_asset_id).
# - That requires your cdc_ingestion_slices table to have a UNIQUE or PRIMARY KEY on those EXACT columns.
# - If you store county_asset_id as NULL for us/state slices, the ON CONFLICT target still works
#   ONLY if your PK/UNIQUE constraint is literally (dataset, year, geo_level, county_asset_id) and
#   your DDL does NOT attempt to use COALESCE() in the PK.
# - If you changed to county_asset_id NOT NULL DEFAULT '' for non-county, this still works.
#
# If your ledger differs, adjust the ON CONFLICT clause accordingly.

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_ingestion_toolbox import cdc as cdc_package
from data_ingestion_toolbox.cdc.config import CONFIG
from data_ingestion_toolbox.cdc.metadata import (
    sync_cdc_dataset_table,
    sync_variable_metadata_for_year,
)
from data_ingestion_toolbox.cdc.client import make_request
from data_ingestion_toolbox.cdc.capture import commit_capture
from data_ingestion_toolbox.cdc.silver_cdc.transform import (
    transform_cdc_to_silver,
)
from data_ingestion_toolbox.cdc.gold_cdc.publisher import (
    publish_glossary,
    publish_state,
)
from data_ingestion_toolbox.utility.gold_schema import (
    ServingRefreshChunkConfig,
    refresh_serving_layer_in_year_chunks,
)
from data_ingestion_toolbox.normalization import sanitize_error_message

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

# Pool-based throttling: create these in Airflow UI (Admin -> Pools)
# Start conservative (e.g., 4). Increase slowly while watching for HTTP 429s.
CDC_API_POOL = "cdc_api"


def _get_postgres_hook() -> PostgresHook:
    """
    Centralized PostgresHook factory.

    Keeps the conn_id in one place, so changing it in CONFIG affects all tasks.
    """
    conn_id = CONFIG.postgres_conn_id.strip()
    if not conn_id:
        raise RuntimeError("PostgreSQL connection ID is not configured")
    return PostgresHook(postgres_conn_id=conn_id)


def _silver_ddl_path() -> Path:
    return (
        Path(cdc_package.__file__).resolve().parent / "DDL" / "silver_cdc.sql"
    )


def _variables_fingerprint(year: int, dataset: str) -> tuple[str, int]:
    """
    Compute a stable fingerprint of the curated variable list for a (year, dataset).

    Why this matters:
    - Your plan/skip logic should not only say "did we ingest this slice once?"
      It should say "did we ingest this slice for the CURRENT variable set?"
    - If you add a variable to config.py (or change selection logic),
      the fingerprint changes and slices are automatically re-run to backfill.

    How it works:
    - Sort variable names
    - Join them with a delimiter
    - Hash the resulting string with SHA-256
    """
    vars_ = sync_variable_metadata_for_year(year, dataset) or []
    vars_sorted = sorted(vars_)
    payload = "|".join(vars_sorted).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return digest, len(vars_sorted)


def chunk_list(items: list, chunk_size: int) -> list[list]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _run_one_work_unit(work_unit: dict) -> int:
    """
    Plain Python function: does the DB ledger updates + calls make_request().
    Safe to call inside tasks (including inside a loop).
    """
    hook = _get_postgres_hook()

    dataset: str = work_unit["dataset"]
    year: int = int(work_unit["year"])
    geo_level: str = work_unit["geo_level"]
    county_asset_id: Optional[str] = work_unit.get("county_asset_id")

    variables_hash: Optional[str] = work_unit.get("variables_hash")
    variables_count: int = int(work_unit.get("variables_count", 0))

    started = datetime.now(timezone.utc)

    sql_running_update = """
        UPDATE control.cdc_ingestion_slices
        SET status = 'running',
            rows_loaded = 0,
            started_at = %s,
            finished_at = NULL,
            last_error = NULL,
            variables_hash = %s,
            variables_count = %s,
            variables_hash_seen_at = %s
        WHERE dataset = %s
        AND year = %s
        AND geo_level = %s
        AND county_asset_id IS NOT DISTINCT FROM %s;
    """

    sql_running_insert = """
        INSERT INTO control.cdc_ingestion_slices (
            dataset, year, geo_level, county_asset_id,
            status, rows_loaded,
            started_at, finished_at, last_error,
            variables_hash, variables_count, variables_hash_seen_at
        )
        VALUES (%s, %s, %s, %s,
                'running', 0,
                %s, NULL, NULL,
                %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql_running_update,
            (
                started,
                variables_hash,
                variables_count,
                started,  # variables_hash_seen_at
                dataset,
                year,
                geo_level,
                county_asset_id,
            ),
        )

        if cur.rowcount == 0:
            cur.execute(
            sql_running_insert,
            (
                dataset,
                year,
                geo_level,
                county_asset_id,
                started,
                variables_hash,
                variables_count,
                started,
            ),
            )

        conn.commit()

    try:
        response_bytes, metadata = make_request(
            asset_id=county_asset_id,
            page_size=1000,
            page_cursor=None,
            app_token=None
        )

        capture_lineage = commit_capture(
            response_bytes=response_bytes,
            metadata=metadata,
            asset_id=county_asset_id,
            release_version=str(year),
            capture_timestamp=datetime.now(timezone.utc)
        )

        rows_loaded = 1
    except Exception as e:
        rows_loaded = 0
        last_error = sanitize_error_message(e)

    sql_finished_update = """
        UPDATE control.cdc_ingestion_slices
        SET status = 'finished',
            rows_loaded = %s,
            finished_at = %s,
            last_error = %s
        WHERE dataset = %s
        AND year = %s
        AND geo_level = %s
        AND county_asset_id IS NOT DISTINCT FROM %s;
    """

    sql_finished_insert = """
        INSERT INTO control.cdc_ingestion_slices (
            dataset, year, geo_level, county_asset_id,
            status, rows_loaded,
            started_at, finished_at, last_error,
            variables_hash, variables_count, variables_hash_seen_at
        )
        VALUES (%s, %s, %s, %s,
                'finished', %s,
                %s, %s,
                %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql_finished_update,
            (
                rows_loaded,
                datetime.now(timezone.utc),
                last_error,
                dataset,
                year,
                geo_level,
                county_asset_id,
            ),
        )

        if cur.rowcount == 0:
            cur.execute(
            sql_finished_insert,
            (
                dataset,
                year,
                geo_level,
                county_asset_id,
                rows_loaded,
                datetime.now(timezone.utc),
                last_error,
                variables_hash,
                variables_count,
                started,
            ),
            )

        conn.commit()

    return rows_loaded


# -----------------------------
# Task 1: Sync dataset availability
# -----------------------------
@task(trigger_rule="none_failed")
def sync_datasets() -> None:
    """Sync CDC dataset availability table."""
    sync_cdc_dataset_table()


# -----------------------------
# Task 2: Determine all target years and build a variable-aware plan
# -----------------------------
@task(trigger_rule="none_failed")
def get_target_years() -> list[int]:
    """Determine all target years for configured datasets."""
    datasets = CONFIG.datasets
    years = []
    for dataset in datasets:
        # Fetch available years from metadata
        available_years = sync_variable_metadata_for_year(None, dataset) or []
        years.extend(available_years)
    return years


# -----------------------------
# Task 3: Build ingestion plan
# -----------------------------
@task(trigger_rule="none_failed")
def build_ingestion_plan(years: list[int]) -> list[list[dict]]:
    """Build a slice plan for US + State + County-by-State."""
    datasets = CONFIG.datasets
    geo_levels = ["us", "state", "county"]
    batches = []

    for dataset in datasets:
        for year in years:
            for geo_level in geo_levels:
                work_unit = {
                    "dataset": dataset,
                    "year": year,
                    "geo_level": geo_level,
                    "county_asset_id": None,
                    "variables_hash": None,
                    "variables_count": 0
                }
                batches.append(work_unit)

    return chunk_list(batches, chunk_size=10)


# -----------------------------
# Task 4: Mark slices planned for observability (optional but recommended)
# -----------------------------
@task(trigger_rule="none_failed")
def mark_slices_planned(batches: list[list[dict]]) -> None:
    """Mark slices planned for observability."""
    hook = _get_postgres_hook()
    now = datetime.now(timezone.utc)

    sql_planned_update = """
        UPDATE control.cdc_ingestion_slices
        SET status = 'planned',
            rows_loaded = 0,
            started_at = NULL,
            finished_at = NULL,
            last_error = NULL,
            variables_hash = %s,
            variables_count = %s,
            variables_hash_seen_at = %s
        WHERE dataset = %s
        AND year = %s
        AND geo_level = %s
        AND county_asset_id IS NOT DISTINCT FROM %s
        AND control.cdc_ingestion_slices.variables_hash = %s
        THEN control.cdc_ingestion_slices.rows_loaded
        ELSE 0
        END,
        variables_hash = %s,
        variables_count = %s,
        variables_hash_seen_at = %s,
        last_error = NULL
        WHERE dataset = %s
        AND year = %s
        AND geo_level = %s
        AND county_asset_id IS NOT DISTINCT FROM %s;
    """

    sql_planned_insert = """
        INSERT INTO control.cdc_ingestion_slices (
            dataset, year, geo_level, county_asset_id,
            status, rows_loaded,
            started_at, finished_at, last_error,
            variables_hash, variables_count, variables_hash_seen_at
        )
        VALUES (%s, %s, %s, %s,
                'planned', 0,
                NULL, NULL, NULL,
                %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        for batch in batches:
            for w in batch:
                dataset = w["dataset"]
                year = int(w["year"])
                geo_level = w["geo_level"]
                county_asset_id = w.get("county_asset_id")
                vhash = w.get("variables_hash")
                vcount = int(w.get("variables_count", 0))

                cur.execute(
                    sql_planned_update,
                    (
                        vhash,  # hash compare #1
                        vhash,  # hash compare #2
                        vhash,  # set variables_hash
                        vcount,
                        now,
                        dataset,
                        year,
                        geo_level,
                        county_asset_id,
                    ),
                )

                if cur.rowcount == 0:
                    cur.execute(
                    sql_planned_insert,
                    (
                        dataset,
                        year,
                        geo_level,
                        county_asset_id,
                        vhash,
                        vcount,
                        now,
                    ),
                    )

            conn.commit()


# -----------------------------
# Task 5: Ingest one slice (mapped), with ledger updates
# -----------------------------
@task(pool=CDC_API_POOL)
def ingest_batch(batch: list[dict]) -> int:
    """
    Ingest a batch of work units sequentially inside one mapped task.

    This keeps Airflow task-mapping under the 1024 cap AND avoids spawning
    thousands of task instances.
    """
    total = 0
    for work_unit in batch:
        total += _run_one_work_unit(work_unit)
    return total


# -----------------------------
# Task 6: Silver layer (full load)
# -----------------------------
@task(trigger_rule="none_failed")
def ensure_silver_schema() -> None:
    """Ensure silver_cdc schema and tables exist."""
    sql_path = _silver_ddl_path()
    sql = sql_path.read_text(encoding="utf-8")
    hook = _get_postgres_hook()
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


@task(trigger_rule="none_failed")
def transform_to_silver() -> int:
    """Transform ALL raw CDC data to silver (full load)."""
    return transform_cdc_to_silver()


# -----------------------------
# DAG wiring
# -----------------------------
# 1) Refresh dataset availability
sync = sync_datasets()

# 2) Determine all target years and build a variable-aware plan
targets = get_target_years()
batches = build_ingestion_plan(targets)

# Ensure ordering: dataset sync -> target selection -> plan build
sync >> batches

# 3) Mark slices planned for observability (optional but recommended)
planned = mark_slices_planned(batches)
batches >> planned

# 4) Execute mapped ingestion batches
raw_ingest = ingest_batch.expand(batch=batches)

silver_schema = ensure_silver_schema()
silver_transform = transform_to_silver()

raw_ingest >> silver_schema >> silver_transform

# -----------------------------
# gold_cdc serving layer
# -----------------------------
@task(trigger_rule="none_failed")
def ensure_gold_cdc_schema() -> None:
    """Apply the source-specific gold_cdc DDL."""
    from data_ingestion_toolbox.cdc.gold_cdc.publisher import (
        ensure_cdc_gold_schema,
    )

    ensure_cdc_gold_schema()


@task(trigger_rule="none_failed")
def refresh_gold_geography() -> None:
    """Synchronize the shared current-geography table in a short transaction."""
    hook = _get_postgres_hook()
    with hook.get_conn() as conn, conn.cursor() as cur:
    cur.execute("SET lock_timeout = '30s'")
    cur.execute("SET statement_timeout = '10min'")
    conn.commit()


@task(trigger_rule="none_failed")
def refresh_gold_cdc_elements() -> int:
    """Refresh CDC dimensions and metric mappings in gold_cdc."""
    from data_ingestion_toolbox.cdc.gold_cdc.publisher import (
    refresh_cdc_elements,
    )

    return refresh_cdc_elements()


@task(trigger_rule="none_failed")
def refresh_gold_cdc_serving_layer() -> dict[str, int]:
    """Refresh changed CDC vintages as independently committed annual chunks."""
    return refresh_serving_layer_in_year_chunks(
    hook=_get_postgres_hook(),
    config=ServingRefreshChunkConfig(
    source_code="CDC_DISEASE_ILLNESS",
    log_label="CDC",
    report_table="gold_cdc.rpt_cdc_observations",
    report_date_column="observation_date",
    changed_chunks_sql="""
    SELECT
    MAKE_DATE(s.estimate_year, 1, 1) AS chunk_start,
    MAKE_DATE(s.estimate_year, 12, 31) AS chunk_end,
    MAX(s.ingested_at) AS target_watermark
    FROM silver_cdc.fact_cdc_observations s
    WHERE s.estimate_value IS NOT NULL
    AND s.ingested_at > %s
    GROUP BY s.estimate_year
    ORDER BY s.estimate_year
    """,
    report_procedure="gold_cdc.refresh_rpt_cdc_observations",
    latest_procedure="gold_cdc.refresh_mv_cdc_latest",
    statement_timeout="90min",
    ),
    task_logger=logger,
    )


@task(trigger_rule="none_failed")
def emit_cdc_publisher_ready() -> None:
    """Append a durable outbox event without waiting for glossary harvest."""
    from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

    hook = _get_postgres_hook()
    emit_latest_publisher_ready(hook.get_conn, publisher_schema="gold_cdc")


gold_cdc_schema = ensure_gold_cdc_schema()
gold_geography = refresh_gold_geography()
gold_cdc_elements = refresh_gold_cdc_elements()
gold_cdc_refresh = refresh_gold_cdc_serving_layer()
publisher_ready = emit_cdc_publisher_ready()

(
    silver_transform
    >> gold_cdc_schema
    >> gold_geography
    >> gold_cdc_elements
    >> gold_cdc_refresh
    >> publisher_ready
    )


# Instantiate DAG
cdc_ingest_dag = cdc_ingest()
