# dags/fred_ingest_dag.py
#
# DROP-IN DAG SCRIPT (TaskFlow API) — FRED Economic Data Ingestion
# ------------------------------------------------------------------
# What this DAG does:
# 1) Syncs FRED series metadata from FRED API
# 2) Syncs FRED datasets table to track domain/series availability
# 3) Builds ingestion plan for configured domains and date ranges
# 4) Skips slices already completed for the current series set (hash-based)
# 5) Uses a Pool ("fred_api") to limit concurrency and respect FRED API limits
# 6) Tracks status/rows/errors in raw_fred.fred_ingestion_slices
#
# REQUIRED DB TABLES:
# - raw_fred.fred_datasets
# - raw_fred.fred_series
# - raw_fred.fred_ingestion_slices
# - raw_fred.fred_long
#
# REQUIRED AIRFLOW POOL:
# - Create a pool named "fred_api" in Airflow UI and set its size conservatively (start with 4).
#
# ASSUMPTIONS:
# - data_ingestion_toolbox.fred.config.CONFIG has postgres_conn_id, curated_series_ids, curated_by_domain
# - data_ingestion_toolbox.fred.metadata provides sync_fred_series_metadata(), sync_fred_datasets_table()
# - data_ingestion_toolbox.fred.ingest provides ingest_slice()
# - FRED_API_KEY environment variable is set

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_ingestion_toolbox import fred as fred_package
from data_ingestion_toolbox.fred.config import CONFIG
from data_ingestion_toolbox.fred.metadata import (
    sync_fred_series_metadata,
    sync_fred_datasets_table,
)
from data_ingestion_toolbox.fred.ingest import ingest_slice, get_curated_series_for_domain
from data_ingestion_toolbox.fred.silver_fred.transform import transform_fred_to_silver

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
FRED_API_POOL = "fred_api"


def _get_postgres_hook() -> PostgresHook:
    """Centralized PostgresHook factory."""
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _silver_ddl_path() -> Path:
    return Path(fred_package.__file__).resolve().parent / "DDL" / "silver_fred.sql"


def _series_fingerprint(domain: str) -> tuple[str, int]:
    """
    Compute a stable fingerprint of the curated series list for a domain.
    
    Returns: (hash_digest, series_count)
    """
    series_list = get_curated_series_for_domain(domain)
    if not series_list:
        return "", 0
    
    series_sorted = sorted(series_list)
    payload = "|".join(series_sorted).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return digest, len(series_sorted)


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split list into chunks."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _run_one_work_unit(work_unit: dict) -> int:
    """
    Execute one ingestion work unit with ledger updates.
    
    work_unit structure:
        {
            "domain": str,
            "date_start": str (YYYY-MM-DD),
            "date_end": str (YYYY-MM-DD),
            "series_hash": str,
            "series_count": int,
        }
    """
    hook = _get_postgres_hook()
    
    domain = work_unit["domain"]
    date_start = work_unit["date_start"]
    date_end = work_unit["date_end"]
    series_hash = work_unit.get("series_hash")
    series_count = int(work_unit.get("series_count", 0))
    
    started = datetime.now(timezone.utc)
    
    # Parse dates for ledger
    try:
        date_start_obj = datetime.fromisoformat(date_start).date()
        date_end_obj = datetime.fromisoformat(date_end).date()
    except ValueError:
        raise ValueError(f"Invalid date format: {date_start} or {date_end}")
    
    # Update ledger to 'running'
    sql_running_update = """
        UPDATE raw_fred.fred_ingestion_slices
        SET status = 'running',
            rows_loaded = 0,
            started_at = %s,
            finished_at = NULL,
            last_error = NULL,
            series_hash = %s,
            series_count = %s,
            series_hash_seen_at = %s
        WHERE domain = %s
          AND date_start = %s
          AND date_end = %s;
    """
    
    sql_running_insert = """
        INSERT INTO raw_fred.fred_ingestion_slices (
            domain, date_start, date_end,
            status, rows_loaded,
            started_at, finished_at, last_error,
            series_hash, series_count, series_hash_seen_at
        )
        VALUES (%s, %s, %s,
                'running', 0,
                %s, NULL, NULL,
                %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql_running_update,
            (started, series_hash, series_count, started,
             domain, date_start_obj, date_end_obj),
        )
        
        if cur.rowcount == 0:
            cur.execute(
                sql_running_insert,
                (domain, date_start_obj, date_end_obj,
                 started, series_hash, series_count, started),
            )
        
        conn.commit()
    
    try:
        # Call ingest_slice
        rows_loaded = ingest_slice(
            domain=domain,
            date_start=date_start,
            date_end=date_end,
        )
        
        finished = datetime.now(timezone.utc)
        final_status = "empty" if rows_loaded == 0 else "success"
        
        sql_done = """
            UPDATE raw_fred.fred_ingestion_slices
            SET status = %s,
                rows_loaded = %s,
                finished_at = %s,
                started_at = COALESCE(started_at, %s),
                last_error = NULL,
                series_hash = %s,
                series_count = %s
            WHERE domain = %s
              AND date_start = %s
              AND date_end = %s;
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql_done,
                (final_status, int(rows_loaded), finished, started, series_hash, series_count,
                 domain, date_start_obj, date_end_obj),
            )
            conn.commit()
        
        return int(rows_loaded)
    
    except Exception as e:
        finished = datetime.now(timezone.utc)
        err_txt = str(e)[:4000]
        
        sql_failed = """
            UPDATE raw_fred.fred_ingestion_slices
            SET status = 'failed',
                finished_at = %s,
                started_at = COALESCE(started_at, %s),
                last_error = %s
            WHERE domain = %s
              AND date_start = %s
              AND date_end = %s;
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql_failed,
                (finished, started, err_txt, domain, date_start_obj, date_end_obj),
            )
            conn.commit()
        
        raise


@dag(
    dag_id="fred_ingest",
    default_args=DEFAULT_ARGS,
    schedule="0 8 1 * *",  # monthly on the 1st at 08:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fred", "macro"],
)
def fred_ingest():
    """
    FRED raw data ingestion DAG for raw_fred.
    
    - Sync metadata (series + datasets)
    - Build ingestion plan for each domain
    - Skip completed slices unless series set changed (hash mismatch)
    - Track progress in raw_fred.fred_ingestion_slices
    """
    
    # -----------------------------
    # Task 1: Metadata sync
    # -----------------------------
    @task
    def sync_datasets() -> None:
        """Sync fred_datasets table."""
        sync_fred_datasets_table()
    
    @task
    def sync_metadata() -> int:
        """Sync series metadata for all curated series."""
        count = sync_fred_series_metadata()
        return count
    
    # -----------------------------
    # Task 2: Build ingestion plan
    # -----------------------------
    @task
    def build_ingestion_plan(metadata_count: int) -> list[list[dict]]:
        """
        Build work units for each domain.

        Year-range strategy
        -------------------
        Slices are split into two date bands:

        1. Historical band  (1970-01-01 → current_year-3-12-31)
           Treated as immutable once status='success' for the current series hash.
           Re-runs only when the curated series list changes.

        2. Rolling window   (current_year-2-01-01 → yesterday)
           Always re-ingested unconditionally, regardless of prior status.
           FRED revises series values continuously (benchmark revisions,
           seasonal adjustment updates, late-filed data).  Two years back
           ensures all common revision windows are covered automatically
           on every monthly DAG run.

        Skip logic
        ----------
        Historical slices: skipped when (domain, date_start, date_end,
            series_hash) is in the completed set.
        Rolling slices:    never skipped — always included in the plan.
        """
        hook = _get_postgres_hook()

        today = datetime.now(timezone.utc)
        current_year = today.year

        # Historical band: lock in once complete
        hist_start = "1970-01-01"
        hist_end   = f"{current_year - 3}-12-31"   # e.g. in 2026 → 2023-12-31

        # Rolling window: always re-ingest for revisions / backfills
        roll_start = f"{current_year - 2}-01-01"   # e.g. in 2026 → 2024-01-01
        roll_end   = (today - timedelta(days=1)).strftime("%Y-%m-%d")  # yesterday

        # Compute series fingerprints for each domain
        domain_meta = {}
        for domain in CONFIG.domains:
            shash, scount = _series_fingerprint(domain)
            if scount > 0:
                domain_meta[domain] = {"series_hash": shash, "series_count": scount}

        # Also add a slice for all series combined (domain=None)
        shash_all, scount_all = _series_fingerprint(None)
        if scount_all > 0:
            domain_meta["all"] = {"series_hash": shash_all, "series_count": scount_all}

        # Load completed slices so we can skip historical ones
        completed = set()
        sql_completed = """
            SELECT domain, date_start, date_end, series_hash
            FROM raw_fred.fred_ingestion_slices
            WHERE status IN ('success', 'empty');
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_completed)
            for domain, ds, de, series_hash in cur.fetchall():
                completed.add((
                    str(domain),
                    ds.isoformat() if ds else None,
                    de.isoformat() if de else None,
                    series_hash,
                ))

        def hist_is_done(domain: str, ds: str, de: str) -> bool:
            """True if the historical slice is complete for the current series hash."""
            current_hash = domain_meta.get(domain, {}).get("series_hash")
            if not current_hash:
                return True
            return (domain, ds, de, current_hash) in completed

        def add_slice(
            plan: list[dict],
            domain: str,
            ds: str,
            de: str,
            force: bool,
        ) -> None:
            """Append a work unit if it should run (forced or not yet done)."""
            meta = domain_meta[domain]
            if force or not hist_is_done(domain, ds, de):
                plan.append({
                    "domain": domain,
                    "date_start": ds,
                    "date_end": de,
                    "series_hash": meta["series_hash"],
                    "series_count": meta["series_count"],
                })

        # Build plan across both bands
        plan: list[dict] = []

        for domain in domain_meta:
            # Historical band — skip if already done
            add_slice(plan, domain, hist_start, hist_end, force=False)
            # Rolling window — always re-ingest to catch revisions
            add_slice(plan, domain, roll_start, roll_end, force=True)

        # IMPORTANT: return at least one batch so mapped task retries remain stable.
        if not plan:
            return [[]]

        batches = chunk_list(plan, chunk_size=5)
        return batches
    
    # -----------------------------
    # Task 3: Mark slices planned
    # -----------------------------
    @task
    def mark_slices_planned(batches: list[list[dict]]) -> None:
        """Upsert slice ledger rows as 'planned'."""
        if not batches:
            return
        
        hook = _get_postgres_hook()
        now = datetime.now(timezone.utc)
        
        sql_planned_update = """
            UPDATE raw_fred.fred_ingestion_slices
            SET status = CASE
                    WHEN status IN ('success','empty')
                        AND series_hash = %s
                    THEN status
                    ELSE 'planned'
                END,
                rows_loaded = CASE
                    WHEN status IN ('success','empty')
                        AND series_hash = %s
                    THEN rows_loaded
                    ELSE 0
                END,
                series_hash = %s,
                series_count = %s,
                series_hash_seen_at = %s,
                last_error = NULL
            WHERE domain = %s
              AND date_start = %s
              AND date_end = %s;
        """
        
        sql_planned_insert = """
            INSERT INTO raw_fred.fred_ingestion_slices (
                domain, date_start, date_end,
                status, rows_loaded,
                started_at, finished_at, last_error,
                series_hash, series_count, series_hash_seen_at
            )
            VALUES (%s, %s, %s,
                    'planned', 0,
                    NULL, NULL, NULL,
                    %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            for batch in batches:
                for w in batch:
                    domain = w["domain"]
                    date_start = datetime.fromisoformat(w["date_start"]).date()
                    date_end = datetime.fromisoformat(w["date_end"]).date()
                    shash = w.get("series_hash")
                    scount = int(w.get("series_count", 0))
                    
                    cur.execute(
                        sql_planned_update,
                        (shash, shash, shash, scount, now,
                         domain, date_start, date_end),
                    )
                    
                    if cur.rowcount == 0:
                        cur.execute(
                            sql_planned_insert,
                            (domain, date_start, date_end,
                             shash, scount, now),
                        )
            
            conn.commit()
    
    # -----------------------------
    # Task 4: Ingest batch (mapped)
    # -----------------------------
    @task(pool=FRED_API_POOL)
    def ingest_batch(batch: list[dict]) -> int:
        """Ingest a batch of work units sequentially."""
        total = 0
        for work_unit in batch:
            total += _run_one_work_unit(work_unit)
        return total

    # -----------------------------
    # Task 5: Silver layer (full load)
    # -----------------------------
    @task(trigger_rule='none_failed')
    def ensure_silver_schema() -> None:
        """Ensure silver_fred schema and tables exist."""
        sql_path = _silver_ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task(trigger_rule='none_failed', max_active_tis_per_dag=CONFIG.silver_max_active_tis)
    def transform_to_silver_by_domain(domain: str) -> int:
        """Transform ALL raw FRED data to silver for one domain (full load)."""
        return transform_fred_to_silver(domain=domain)
    
    # -----------------------------
    # DAG wiring
    # -----------------------------
    sync_ds = sync_datasets()
    sync_meta = sync_metadata()
    
    plan = build_ingestion_plan(sync_meta)
    
    sync_ds >> sync_meta >> plan
    
    planned = mark_slices_planned(plan)
    plan >> planned
    
    raw_ingest = ingest_batch.expand(batch=plan)

    silver_schema = ensure_silver_schema()
    silver_transforms = transform_to_silver_by_domain.expand(
        domain=["labor_cycle", "housing", "prices", "rates", "macro"]
    )

    raw_ingest >> silver_schema >> silver_transforms

    # -----------------------------
    # gold_fred serving layer
    # -----------------------------
    @task(trigger_rule="none_failed")
    def ensure_gold_fred_schema() -> None:
        """Apply the source-specific gold_fred DDL."""
        from data_ingestion_toolbox.fred.gold_fred.transform import (
            ensure_fred_gold_schema,
        )

        ensure_fred_gold_schema()

    @task(trigger_rule="none_failed")
    def refresh_gold_fred_elements() -> int:
        """Refresh FRED dimensions and metric mappings in gold_fred."""
        from data_ingestion_toolbox.fred.gold_fred.transform import (
            refresh_fred_elements,
        )

        return refresh_fred_elements()

    @task(trigger_rule="none_failed")
    def get_gold_fred_refresh_window() -> dict[str, str] | None:
        """Return the complete FRED date range currently available in silver."""
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT MIN(observation_date)::date, MAX(observation_date)::date
                FROM silver_fred.fact_economic_indicators
                WHERE is_missing = FALSE
                """
            )
            row = cur.fetchone()

        if not row or row[0] is None:
            logger.warning("[gold_fred] No silver data available; skipping serving refresh.")
            return None

        return {"start_date": row[0].isoformat(), "end_date": row[1].isoformat()}

    @task(trigger_rule="none_failed")
    def refresh_gold_fred_serving_layer(
        refresh_window: dict[str, str] | None,
    ) -> None:
        """Refresh persisted FRED serving tables in gold_fred."""
        if refresh_window is None:
            return

        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(
                "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s)",
                (refresh_window["start_date"], refresh_window["end_date"]),
            )
            conn.commit()

    gold_fred_schema = ensure_gold_fred_schema()
    gold_fred_elements = refresh_gold_fred_elements()
    gold_fred_window = get_gold_fred_refresh_window()
    gold_fred_refresh = refresh_gold_fred_serving_layer(gold_fred_window)

    (
        silver_transforms
        >> gold_fred_schema
        >> gold_fred_elements
        >> gold_fred_window
        >> gold_fred_refresh
    )

# Instantiate DAG
fred_ingest_dag = fred_ingest()
