# dags/acs_ingest_dag.py
#
# DAG SCRIPT (TaskFlow API) — ACS Detailed Tables (acs1 + acs5)
# -------------------------------------------------------------------
# What this DAG does:
# 1) Syncs which ACS Detailed Tables datasets are available (acs1/acs5 by year)
# 2) Picks the most recent available year per dataset (default "keep current" policy)
# 3) Builds a slice plan for US + State + County-by-State
# 4) Skips slices already completed *for the current variable set*
#    - If you change the curated variable list in config.py, variables_hash changes
#    - Any previously completed slices with an old hash become "stale" and are re-run
# 5) Uses a Pool ("census_api") to limit concurrency and respect Census API limits
# 6) Tracks status/rows/errors in control.acs_ingestion_slices
#
# REQUIRED DB TABLES:
# - raw_census.acs_datasets          (filtered to base Detailed Tables only)
# - control.acs_ingestion_slices  (ledger)
# - raw_capture.response_capture -> silver_census.observation_revision
#
# REQUIRED AIRFLOW POOLS:
# - Create a pool named "census_api" in Airflow UI and set its size conservatively (start with 4).
#   This is the macro-level rate limiter that prevents your executor from stampeding the Census API.
# ASSUMPTIONS ABOUT YOUR CODEBASE:
# - data_ingestion_toolbox.census_acs.config.CONFIG exists and has:
#     CONFIG.postgres_conn_id : Airflow connection id for Postgres
#     CONFIG.datasets         : list like ["acs1", "acs5"]
# - data_ingestion_toolbox.census_acs.metadata provides:
#     sync_acs_dataset_table()
#     sync_variable_metadata_for_year(year: int, dataset: str)
# - data_ingestion_toolbox.census_acs.ingest provides:
#     ingest_slice(year: int, dataset: str, geo_level: str, state_fips: Optional[str]) -> int
#     get_curated_variables(year: int, dataset: str) -> list[str]
# - ingest_slice returns 0 when there is nothing to load (perfect for ACS1 county coverage).
#
# IMPORTANT NOTE ABOUT state_fips NULLABILITY:
# - This script uses ON CONFLICT (dataset, year, geo_level, state_fips).
# - That requires your acs_ingestion_slices table to have a UNIQUE or PRIMARY KEY on those EXACT columns.
# - If you store state_fips as NULL for us/state slices, the ON CONFLICT target still works
#   ONLY if your PK/UNIQUE constraint is literally (dataset, year, geo_level, state_fips) and
#   your DDL does NOT attempt to use COALESCE() in the PK.
# - If you changed to state_fips NOT NULL DEFAULT '' for non-county, this still works.
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
from data_ingestion_toolbox import census_acs as census_acs_package
from data_ingestion_toolbox.census_acs.config import CONFIG
from data_ingestion_toolbox.census_acs.metadata import (
    sync_acs_dataset_table,
    sync_variable_metadata_for_year,
)
from data_ingestion_toolbox.census_acs.ingest import ingest_slice, get_curated_variables
from data_ingestion_toolbox.census_acs.geography import sync_geo_dim
from data_ingestion_toolbox.census_acs.silver_census.transform import (
    transform_census_to_silver,
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
CENSUS_API_POOL = "census_api"


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
        Path(census_acs_package.__file__).resolve().parent / "DDL" / "silver_census.sql"
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
    vars_ = get_curated_variables(year, dataset) or []
    vars_sorted = sorted(vars_)
    payload = "|".join(vars_sorted).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return digest, len(vars_sorted)


def chunk_list(items: list, chunk_size: int) -> list[list]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _run_one_work_unit(work_unit: dict) -> int:
    """
    Plain Python function: does the DB ledger updates + calls ingest_slice().
    Safe to call inside tasks (including inside a loop).
    """
    hook = _get_postgres_hook()

    dataset: str = work_unit["dataset"]
    year: int = int(work_unit["year"])
    geo_level: str = work_unit["geo_level"]
    state_fips: Optional[str] = work_unit.get("state_fips")

    variables_hash: Optional[str] = work_unit.get("variables_hash")
    variables_count: int = int(work_unit.get("variables_count", 0))

    started = datetime.now(timezone.utc)

    sql_running_update = """
        UPDATE control.acs_ingestion_slices
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
        AND state_fips IS NOT DISTINCT FROM %s;
    """

    sql_running_insert = """
        INSERT INTO control.acs_ingestion_slices (
            dataset, year, geo_level, state_fips,
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
                state_fips,
            ),
        )

        if cur.rowcount == 0:
            cur.execute(
                sql_running_insert,
                (
                    dataset,
                    year,
                    geo_level,
                    state_fips,
                    started,
                    variables_hash,
                    variables_count,
                    started,
                ),
            )

        conn.commit()

    try:
        rows_loaded = ingest_slice(
            year=year, dataset=dataset, geo_level=geo_level, state_fips=state_fips
        )

        finished = datetime.now(timezone.utc)
        final_status = "empty" if rows_loaded == 0 else "success"

        sql_done = """
            UPDATE control.acs_ingestion_slices
            SET status = %s,
                rows_loaded = %s,
                finished_at = %s,
                last_error = NULL,
                variables_hash = %s,
                variables_count = %s
            WHERE dataset = %s
              AND year = %s
              AND geo_level = %s
              AND state_fips IS NOT DISTINCT FROM %s;
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql_done,
                (
                    final_status,
                    int(rows_loaded),
                    finished,
                    variables_hash,
                    variables_count,
                    dataset,
                    year,
                    geo_level,
                    state_fips,
                ),
            )
            conn.commit()

        return int(rows_loaded)

    except Exception as e:
        finished = datetime.now(timezone.utc)
        err_txt = sanitize_error_message(e)

        sql_failed = """
            UPDATE control.acs_ingestion_slices
            SET status = 'failed',
                finished_at = %s,
                last_error = %s
            WHERE dataset = %s
              AND year = %s
              AND geo_level = %s
              AND state_fips IS NOT DISTINCT FROM %s;
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql_failed, (finished, err_txt, dataset, year, geo_level, state_fips)
            )
            conn.commit()

        raise


@dag(
    dag_id="acs_ingest",
    default_args=DEFAULT_ARGS,
    schedule="0 6 1 * *",  # monthly on the 1st at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["census", "acs"],
)
def acs_ingest():
    """
    ACS Detailed Tables ingestion DAG for raw_census.

    - Sync datasets available (acs1/acs5)
    - Determine target years (latest per dataset)
    - Build plan (us/state + county per state)
    - Skip completed slices *unless* variable set changed (hash mismatch)
    - Ingest remaining slices with dynamic task mapping and pool throttling
    - Record progress in control.acs_ingestion_slices
    """

    # -----------------------------
    # Task 1: Dataset availability sync
    # -----------------------------
    @task
    def sync_datasets() -> None:
        """
        Upsert the dataset/year entries for base Detailed Tables (acs1/acs5) into raw_census.acs_datasets.
        """
        sync_acs_dataset_table()

    @task
    def sync_geographies() -> None:
        # Auto-pick latest available Gazetteer year
        sync_geo_dim(source_year=None, min_year=2010)

    # -----------------------------
    # Task 2: Decide what year(s) to ingest
    # -----------------------------
    @task
    def get_target_years() -> list[dict]:
        """
        Current policy: ingest all available years per dataset.

        Output example:
        [
            {"dataset": "acs1", "year": 2020},
            {"dataset": "acs1", "year": 2021},
            {"dataset": "acs5", "year": 2022},
            {"dataset": "acs5", "year": 2023}
        ]
        """
        hook = _get_postgres_hook()

        sql = """
            SELECT dataset, year
            FROM raw_census.acs_datasets
            WHERE dataset = ANY(%s)
            AND is_available = TRUE
            ORDER BY dataset, year;
        """

        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (CONFIG.datasets,))
            rows = cur.fetchall()

        targets: list[dict] = []
        for dataset, year in rows:
            if year is None:
                # Defensive: skip if metadata has no year for a dataset
                continue
            targets.append({"dataset": str(dataset), "year": int(year)})

        return targets

    # -----------------------------
    # Task 3: Build ingestion plan (variable-aware skip)
    # -----------------------------
    @task
    def build_ingestion_plan(targets: list[dict]) -> list[list[dict]]:
        """
        Build a list of work units to ingest and exclude any slice already done
        FOR THE CURRENT VARIABLE SET (variables_hash).

        A slice is skipped only if:
          status IN ('success','empty') AND variables_hash == current_hash(dataset, year)

        If you add variables later:
          variables_hash changes -> previously done slices become stale -> re-run -> backfill.
        """
        hook = _get_postgres_hook()

        # County tasks are expanded by state FIPS.
        # Keep this simple to start; you can extend to territories later.
        state_fips_list = [f"{i:02d}" for i in range(1, 57) if i not in (3, 7, 14, 43)]

        # Compute current variable set hash/count for each target (dataset, year).
        varset_meta: dict[tuple[str, int], dict] = {}
        for t in targets:
            dataset = t["dataset"]
            year = int(t["year"])

            # Ensure your variable metadata table is synced/available for this year+dataset.
            # This is a good place for it since the plan depends on knowing the variable list.
            sync_variable_metadata_for_year(year, dataset)

            vhash, vcount = _variables_fingerprint(year, dataset)
            varset_meta[(dataset, year)] = {
                "variables_hash": vhash,
                "variables_count": vcount,
            }

        # Load all slices that are already done (success/empty) along with their variables_hash.
        # We'll only skip if the stored hash matches the current hash.
        completed: set[tuple[str, int, str, Optional[str], Optional[str]]] = set()

        sql_completed = """
            SELECT dataset, year, geo_level, state_fips, variables_hash
            FROM control.acs_ingestion_slices
            WHERE status IN ('success','empty');
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_completed)
            for dataset, year, geo_level, state_fips, variables_hash in cur.fetchall():
                completed.add(
                    (
                        str(dataset),
                        int(year),
                        str(geo_level),
                        state_fips if state_fips is not None else None,
                        variables_hash,
                    )
                )

        def is_done_for_current_varset(
            dataset: str, year: int, geo_level: str, state_fips: Optional[str]
        ) -> bool:
            """
            True if this slice is already completed for the current variable set.
            """
            current_hash = varset_meta[(dataset, year)]["variables_hash"]
            return (dataset, year, geo_level, state_fips, current_hash) in completed

        # Build the plan. Include anything not done for current hash.
        plan: list[dict] = []
        for (dataset, year), meta in varset_meta.items():
            # US slice
            if not is_done_for_current_varset(dataset, year, "us", None):
                plan.append(
                    {
                        "dataset": dataset,
                        "year": year,
                        "geo_level": "us",
                        "state_fips": None,
                        "variables_hash": meta["variables_hash"],
                        "variables_count": meta["variables_count"],
                    }
                )

            # STATE slice (all states in one call)
            if not is_done_for_current_varset(dataset, year, "state", None):
                plan.append(
                    {
                        "dataset": dataset,
                        "year": year,
                        "geo_level": "state",
                        "state_fips": None,
                        "variables_hash": meta["variables_hash"],
                        "variables_count": meta["variables_count"],
                    }
                )

            # COUNTY slices (by state)
            for sf in state_fips_list:
                if is_done_for_current_varset(dataset, year, "county", sf):
                    continue
                plan.append(
                    {
                        "dataset": dataset,
                        "year": year,
                        "geo_level": "county",
                        "state_fips": sf,
                        "variables_hash": meta["variables_hash"],
                        "variables_count": meta["variables_count"],
                    }
                )

        # Keep mapping sane: ~50–150 mapped tasks is a happy place.
        # IMPORTANT: return at least one batch so mapped-task retries remain stable.
        # Airflow can fail with "cannot expand field mapped to length 0" if a mapped
        # TI already exists and a retry re-renders this task against an empty list.
        if not plan:
            return [[]]

        batches = chunk_list(plan, chunk_size=25)  # 1836/25 ≈ 74 mapped tasks
        return batches

    # -----------------------------
    # Task 4: Mark slices planned (optional but recommended for observability)
    # -----------------------------
    @task
    def mark_slices_planned(batches: list[list[dict]]) -> None:
        """
        Upsert slice ledger rows as 'planned'.

        batches is a list of batches, each batch is a list of work_unit dicts.
        """
        if not batches:
            return

        hook = _get_postgres_hook()
        now = datetime.now(timezone.utc)

        sql_planned_update = """
            UPDATE control.acs_ingestion_slices
            SET status = CASE
                    WHEN control.acs_ingestion_slices.status IN ('success','empty')
                        AND control.acs_ingestion_slices.variables_hash = %s
                    THEN control.acs_ingestion_slices.status
                    ELSE 'planned'
                END,
                rows_loaded = CASE
                    WHEN control.acs_ingestion_slices.status IN ('success','empty')
                        AND control.acs_ingestion_slices.variables_hash = %s
                    THEN control.acs_ingestion_slices.rows_loaded
                    ELSE 0
                END,
                variables_hash = %s,
                variables_count = %s,
                variables_hash_seen_at = %s,
                last_error = NULL
            WHERE dataset = %s
            AND year = %s
            AND geo_level = %s
            AND state_fips IS NOT DISTINCT FROM %s;
        """

        sql_planned_insert = """
            INSERT INTO control.acs_ingestion_slices (
                dataset, year, geo_level, state_fips,
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
                    state_fips = w.get("state_fips")
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
                            state_fips,
                        ),
                    )

                    if cur.rowcount == 0:
                        cur.execute(
                            sql_planned_insert,
                            (
                                dataset,
                                year,
                                geo_level,
                                state_fips,
                                vhash,
                                vcount,
                                now,
                            ),
                        )

            conn.commit()

    # -----------------------------
    # Task 5: Ingest one slice (mapped), with ledger updates
    # -----------------------------

    @task(pool=CENSUS_API_POOL)
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
        """Ensure silver_census schema and tables exist."""
        sql_path = _silver_ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task(trigger_rule="none_failed")
    def transform_to_silver() -> int:
        """Transform ALL raw Census data to silver (full load)."""
        return transform_census_to_silver()

    # -----------------------------
    # DAG wiring
    # -----------------------------
    # 1) Refresh dataset availability
    sync = sync_datasets()
    sync_geo = sync_geographies()

    # 2) Determine target year(s) and build a variable-aware plan
    targets = get_target_years()
    batches = build_ingestion_plan(targets)

    # Ensure ordering: dataset sync -> target selection -> plan build
    sync >> sync_geo >> targets >> batches

    # 3) Mark slices planned for observability (optional but recommended)
    planned = mark_slices_planned(batches)
    batches >> planned

    # 4) Execute mapped ingestion batches
    raw_ingest = ingest_batch.expand(batch=batches)

    silver_schema = ensure_silver_schema()
    silver_transform = transform_to_silver()

    raw_ingest >> silver_schema >> silver_transform

    # -----------------------------
    # gold_census serving layer
    # -----------------------------
    @task(trigger_rule="none_failed")
    def ensure_gold_census_schema() -> None:
        """Apply the source-specific gold_census DDL."""
        from data_ingestion_toolbox.census_acs.gold_census.transform import (
            ensure_acs_gold_schema,
        )

        ensure_acs_gold_schema()

    @task(trigger_rule="none_failed")
    def refresh_gold_geography() -> None:
        """Synchronize the shared current-geography table in a short transaction."""
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SET lock_timeout = '30s'")
            cur.execute("SET statement_timeout = '10min'")
            conn.commit()

    @task(trigger_rule="none_failed")
    def refresh_gold_census_elements() -> int:
        """Refresh ACS dimensions and metric mappings in gold_census."""
        from data_ingestion_toolbox.census_acs.gold_census.transform import (
            refresh_acs_elements,
        )

        return refresh_acs_elements()

    @task(trigger_rule="none_failed")
    def refresh_gold_census_serving_layer() -> dict[str, int]:
        """Refresh changed ACS vintages as independently committed annual chunks."""
        return refresh_serving_layer_in_year_chunks(
            hook=_get_postgres_hook(),
            config=ServingRefreshChunkConfig(
                source_code="CENSUS_ACS",
                log_label="ACS",
                report_table="gold_census.rpt_acs_observations",
                report_date_column="observation_date",
                changed_chunks_sql="""
                    SELECT
                        MAKE_DATE(s.estimate_year, 1, 1) AS chunk_start,
                        MAKE_DATE(s.estimate_year, 12, 31) AS chunk_end,
                        MAX(s.ingested_at) AS target_watermark
                    FROM silver_census.fact_demographics s
                    WHERE s.estimate_value IS NOT NULL
                      AND s.ingested_at > %s
                    GROUP BY s.estimate_year
                    ORDER BY s.estimate_year
                """,
                report_procedure="gold_census.refresh_rpt_acs_observations",
                latest_procedure="gold_census.refresh_mv_acs_latest",
                statement_timeout="90min",
            ),
            task_logger=logger,
        )

    @task(trigger_rule="none_failed")
    def emit_census_publisher_ready() -> None:
        """Append a durable outbox event without waiting for glossary harvest."""
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        hook = _get_postgres_hook()
        emit_latest_publisher_ready(hook.get_conn, publisher_schema="gold_census")

    gold_census_schema = ensure_gold_census_schema()
    gold_geography = refresh_gold_geography()
    gold_census_elements = refresh_gold_census_elements()
    gold_census_refresh = refresh_gold_census_serving_layer()
    publisher_ready = emit_census_publisher_ready()

    (
        silver_transform
        >> gold_census_schema
        >> gold_geography
        >> gold_census_elements
        >> gold_census_refresh
        >> publisher_ready
    )


# Instantiate DAG
acs_ingest_dag = acs_ingest()
