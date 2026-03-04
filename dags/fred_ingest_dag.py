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
# - fred.config.CONFIG has postgres_conn_id, curated_series_ids, curated_by_domain
# - fred.metadata provides sync_fred_series_metadata(), sync_fred_datasets_table()
# - fred.ingest provides ingest_slice()
# - FRED_API_KEY environment variable is set

from __future__ import annotations

import hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from fred.config import CONFIG
from fred.metadata import sync_fred_series_metadata, sync_fred_datasets_table
from fred.ingest import ingest_slice, get_curated_series_for_domain
from fred.silver_fred.transform import transform_fred_to_silver

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
    return Path(__file__).resolve().parents[1] / "fred" / "DDL" / "silver_fred.sql"


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
        
        Skip slices already completed for current series hash.
        
        We'll create one slice per domain covering the full historical period.
        For incremental updates, you could modify this to create rolling windows.
        """
        hook = _get_postgres_hook()
        
        # Define date range
        # Start from 1970-01-01 for comprehensive historical data
        date_start = "1970-01-01"
        # End at yesterday (avoid today to ensure data is complete)
        date_end = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Compute series fingerprints for each domain
        domain_meta = {}
        for domain in CONFIG.domains:
            shash, scount = _series_fingerprint(domain)
            if scount > 0:  # Only include domains with series
                domain_meta[domain] = {"series_hash": shash, "series_count": scount}
        
        # Also add a slice for all series combined (domain=None)
        shash_all, scount_all = _series_fingerprint(None)
        if scount_all > 0:
            domain_meta["all"] = {"series_hash": shash_all, "series_count": scount_all}
        
        # Load completed slices
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
                    series_hash
                ))
        
        def is_done(domain: str, ds: str, de: str) -> bool:
            """Check if slice is already done for current series set."""
            current_hash = domain_meta.get(domain, {}).get("series_hash")
            if not current_hash:
                return True  # Skip if no series for this domain
            return (domain, ds, de, current_hash) in completed
        
        # Build plan
        plan: list[dict] = []
        
        for domain, meta in domain_meta.items():
            if not is_done(domain, date_start, date_end):
                plan.append({
                    "domain": domain,
                    "date_start": date_start,
                    "date_end": date_end,
                    "series_hash": meta["series_hash"],
                    "series_count": meta["series_count"],
                })
        
        # For FRED, we have relatively few slices, so no batching needed
        # But we'll still return as list of lists for consistency.
        # IMPORTANT: return at least one batch so mapped task retries remain stable.
        # Airflow can raise "cannot expand field mapped to length 0" when a mapped TI
        # already exists (map_index=0) and a retry re-renders against an empty list.
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

    @task(trigger_rule='none_failed')
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
    # Gold update terminal tasks
    # -----------------------------
    @task(trigger_rule='none_failed')
    def gold_ensure_schema() -> None:
        """Ensure gold schema exists."""
        from gold.transform import ensure_gold_schema
        ensure_gold_schema()

    @task(trigger_rule='none_failed')
    def gold_refresh_elements() -> None:
        """Refresh gold element dictionary from silver sources."""
        from fred.gold_fred.transform import refresh_fred_elements
        refresh_fred_elements()

    @task(trigger_rule='none_failed')
    def gold_compute_shards() -> list[str]:
        """Compute gold update shard list for FRED window (~3 months back)."""
        from datetime import date, timedelta
        from gold.transform import build_shard_list
        today = date.today()
        window_start = date(today.year, today.month, 1) - timedelta(days=90)
        window_end = today
        window_start = date(window_start.year, window_start.month, 1)
        return build_shard_list(window_start, window_end)

    @task(trigger_rule='none_failed')
    def gold_merge_shard(month_start: str) -> dict:
        """Merge one gold month shard."""
        from fred.gold_fred.transform import merge_fred_shard
        return merge_fred_shard({"month_start": month_start})

    @task(trigger_rule='none_failed')
    def gold_quality_check(shard_results: list[dict]) -> None:
        """Run quality checks on merged gold shards."""
        from datetime import date
        from gold.quality import run_quality_checks
        for result in (shard_results or []):
            if result and result.get("output_rows", 0) > 0:
                run_quality_checks(date.fromisoformat(result["month_start"]))

    gold_schema = gold_ensure_schema()
    gold_elements = gold_refresh_elements()
    gold_shards = gold_compute_shards()
    gold_merged = gold_merge_shard.expand(month_start=gold_shards)
    gold_qa = gold_quality_check(gold_merged)

    silver_transforms >> gold_schema >> gold_elements >> gold_shards >> gold_merged >> gold_qa


# Instantiate DAG
fred_ingest_dag = fred_ingest()
