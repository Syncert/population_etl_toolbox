# dags/bls_raw_ingest_dag.py
#
# DROP-IN DAG SCRIPT (TaskFlow API) — BLS Data Ingestion
# -------------------------------------------------------
# What this DAG does:
# 1) Syncs BLS series metadata from download.bls.gov
# 2) Syncs BLS datasets table to track program availability
# 3) Builds ingestion plan for configured programs and years
#    - For LAUS (program='la'): expands by geography (us/state/county)
#    - For other programs (CES/CPI/JOLTS): ingests national series
# 4) Skips slices already completed for the current series set (hash-based)
# 5) Uses a Pool ("bls_api") to limit concurrency and respect BLS API limits
# 6) Tracks status/rows/errors in raw_bls.bls_ingestion_slices
#
# REQUIRED DB TABLES:
# - raw_bls.bls_datasets
# - raw_bls.bls_series
# - raw_bls.bls_ingestion_slices
# - raw_bls.bls_long
#
# REQUIRED AIRFLOW POOL:
# - Create a pool named "bls_api" in Airflow UI and set its size conservatively (start with 4).
#
# ASSUMPTIONS:
# - bls.config.CONFIG has postgres_conn_id, programs, curated_by_program
# - bls.metadata provides sync_bls_series_metadata(), sync_bls_datasets_table()
# - bls.ingest provides ingest_slice()
# - BLS_API_KEY environment variable is set

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from bls.config import CONFIG
from bls.metadata import sync_bls_series_metadata, sync_bls_datasets_table
from bls.ingest import ingest_slice, get_curated_series_for_program, BlsRetryableHTTP

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
BLS_API_POOL = "bls_api"


def _get_postgres_hook() -> PostgresHook:
    """Centralized PostgresHook factory."""
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _series_fingerprint(program: str) -> tuple[str, int]:
    """
    Compute a stable fingerprint of the curated series/measure codes for a program.
    
    For LAUS: fingerprints measure codes (which expand to many series).
    For others: fingerprints full series IDs.
    
    Returns: (hash_digest, series_count)
    """
    series_list = get_curated_series_for_program(program)
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
            "program": str,
            "start_year": int,
            "end_year": int,
            "geo_level": str (for LAUS) or None,
            "state_fips": str (for LAUS county) or None,
            "series_hash": str,
            "series_count": int,
        }
    """
    hook = _get_postgres_hook()
    
    program = work_unit["program"]
    start_year = int(work_unit["start_year"])
    end_year = int(work_unit["end_year"])
    geo_level = work_unit.get("geo_level")
    state_fips = work_unit.get("state_fips")
    series_hash = work_unit.get("series_hash")
    series_count = int(work_unit.get("series_count", 0))
    
    started = datetime.now(timezone.utc)
    
    # Update ledger to 'running'
    sql_running_update = """
        UPDATE raw_bls.bls_ingestion_slices
        SET status = 'running',
            rows_loaded = 0,
            started_at = %s,
            finished_at = NULL,
            last_error = NULL,
            series_hash = %s,
            series_count = %s,
            series_hash_seen_at = %s
        WHERE program = %s
          AND year_start = %s
          AND year_end = %s
          AND geo_level IS NOT DISTINCT FROM %s
          AND state_fips IS NOT DISTINCT FROM %s;
    """
    
    sql_running_insert = """
        INSERT INTO raw_bls.bls_ingestion_slices (
            program, year_start, year_end, geo_level, state_fips,
            status, rows_loaded,
            started_at, finished_at, last_error,
            series_hash, series_count, series_hash_seen_at
        )
        VALUES (%s, %s, %s, %s, %s,
                'running', 0,
                %s, NULL, NULL,
                %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql_running_update,
            (started, series_hash, series_count, started,
             program, start_year, end_year, geo_level, state_fips),
        )
        
        if cur.rowcount == 0:
            cur.execute(
                sql_running_insert,
                (program, start_year, end_year, geo_level, state_fips,
                 started, series_hash, series_count, started),
            )
        
        conn.commit()
    
    try:
        # Call ingest_slice
        rows_loaded = ingest_slice(
            program=program,
            start_year=start_year,
            end_year=end_year,
            geo_level=geo_level,
            state_fips=state_fips,
        )
        
        finished = datetime.now(timezone.utc)
        final_status = "empty" if rows_loaded == 0 else "success"
        
        sql_done = """
            UPDATE raw_bls.bls_ingestion_slices
            SET status = %s,
                rows_loaded = %s,
                started_at = COALESCE(started_at, %s),
                finished_at = %s,
                last_error = NULL,
                series_hash = %s,
                series_count = %s
            WHERE program = %s
              AND year_start = %s
              AND year_end = %s
              AND geo_level IS NOT DISTINCT FROM %s
              AND state_fips IS NOT DISTINCT FROM %s;
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql_done,
                (final_status, int(rows_loaded), started, finished, series_hash, series_count,
                 program, start_year, end_year, geo_level, state_fips),
            )
            conn.commit()
        
        return int(rows_loaded)
    
    except Exception as e:
        finished = datetime.now(timezone.utc)
        err_txt = str(e)[:4000]
        
        # Special handling for rate limits and retryable BLS API errors
        # These should be retried, not permanently failed
        is_retryable = isinstance(e, BlsRetryableHTTP)
        
        if is_retryable:
            # Mark as 'planned' so it retries on next manual trigger
            # This covers rate limits (REQUEST_NOT_PROCESSED) and other transient API errors
            sql_planned = """
                UPDATE raw_bls.bls_ingestion_slices
                SET status = 'planned',
                    started_at = COALESCE(started_at, %s),
                    finished_at = %s,
                    last_error = %s
                WHERE program = %s
                  AND year_start = %s
                  AND year_end = %s
                  AND geo_level IS NOT DISTINCT FROM %s
                  AND state_fips IS NOT DISTINCT FROM %s;
            """
            
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    sql_planned,
                    (started, finished, err_txt, program, start_year, end_year, geo_level, state_fips),
                )
                conn.commit()
            
            # Don't raise; let task succeed so DAG completes
            print(f"[Retryable Error] {program} {start_year}-{end_year} (geo={geo_level}, state={state_fips}): {err_txt}")
            return 0
        
        else:
            # Other errors: mark as 'failed' and propagate
            sql_failed = """
                UPDATE raw_bls.bls_ingestion_slices
                SET status = 'failed',
                    started_at = COALESCE(started_at, %s),
                    finished_at = %s,
                    last_error = %s
                WHERE program = %s
                  AND year_start = %s
                  AND year_end = %s
                  AND geo_level IS NOT DISTINCT FROM %s
                  AND state_fips IS NOT DISTINCT FROM %s;
            """
            
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    sql_failed,
                    (started, finished, err_txt, program, start_year, end_year, geo_level, state_fips),
                )
                conn.commit()
            
            raise


@dag(
    dag_id="bls_raw_ingest",
    default_args=DEFAULT_ARGS,
    schedule="0 7 1 * *",  # monthly on the 1st at 07:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bls", "labor", "raw_bls"],
)
def bls_raw_ingest():
    """
    BLS raw data ingestion DAG for raw_bls.
    
    - Sync metadata (series + datasets)
    - Build ingestion plan for each program
    - For LAUS: expand by geography (us/state/county)
    - For others: ingest national series
    - Skip completed slices unless series set changed (hash mismatch)
    - Track progress in raw_bls.bls_ingestion_slices
    """
    
    # -----------------------------
    # Task 1: Metadata sync
    # -----------------------------
    @task
    def sync_datasets() -> None:
        """Sync bls_datasets table."""
        sync_bls_datasets_table()
    
    @task
    def sync_metadata() -> list[str]:
        """Sync series metadata for all configured programs."""
        synced_programs = []
        for program in CONFIG.programs:
            try:
                sync_bls_series_metadata(program)
                synced_programs.append(program)
            except Exception as e:
                # Log but don't fail entire DAG
                print(f"Warning: failed to sync metadata for {program}: {e}")
        return synced_programs
    
    # -----------------------------
    # Task 2: Build ingestion plan
    # -----------------------------
    @task
    def build_ingestion_plan(synced_programs: list[str]) -> list[list[dict]]:
        """
        Build work units for each program.
        
        Skip slices already completed for current series hash.
        """
        hook = _get_postgres_hook()
        
        # Define year range (can make this dynamic)
        current_year = datetime.now(timezone.utc).year
        start_year = 1990  # Start from 1990
        end_year = current_year - 1  # Previous year (avoid incomplete data)
        
        # For LAUS county-level, we need state FIPS codes
        state_fips_list = [
            f"{i:02d}" for i in range(1, 57)
            if i not in (3, 7, 14, 43)  # Skip territories
        ]
        
        # Compute series fingerprints for each program
        series_meta = {}
        for program in synced_programs:
            shash, scount = _series_fingerprint(program)
            series_meta[program] = {"series_hash": shash, "series_count": scount}
        
        # Load completed slices (skip these)
        completed = set()
        sql_completed = """
            SELECT program, year_start, year_end, geo_level, state_fips, series_hash
            FROM raw_bls.bls_ingestion_slices
            WHERE status IN ('success', 'empty');
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_completed)
            for program, sy, ey, geo_level, state_fips, series_hash in cur.fetchall():
                completed.add((
                    str(program), int(sy), int(ey),
                    geo_level if geo_level is not None else None,
                    state_fips if state_fips is not None else None,
                    series_hash
                ))
        
        # Load planned slices (retry these)
        planned_to_retry = set()
        sql_planned = """
            SELECT program, year_start, year_end, geo_level, state_fips
            FROM raw_bls.bls_ingestion_slices
            WHERE status = 'planned';
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_planned)
            for program, sy, ey, geo_level, state_fips in cur.fetchall():
                planned_to_retry.add((
                    str(program), int(sy), int(ey),
                    geo_level if geo_level is not None else None,
                    state_fips if state_fips is not None else None,
                ))
        
        def is_done(program: str, sy: int, ey: int, geo_level: Optional[str], state_fips: Optional[str]) -> bool:
            """Check if slice is already done for current series set."""
            current_hash = series_meta[program]["series_hash"]
            return (program, sy, ey, geo_level, state_fips, current_hash) in completed
        
        def needs_retry(program: str, sy: int, ey: int, geo_level: Optional[str], state_fips: Optional[str]) -> bool:
            """Check if slice is marked as planned (from previous 404)."""
            return (program, sy, ey, geo_level, state_fips) in planned_to_retry
        
        # Build plan
        plan: list[dict] = []
        
        for program in synced_programs:
            meta = series_meta[program]
            
            if program == "la":
                # LAUS: expand by geography
                # US level
                if not is_done(program, start_year, end_year, "us", None) or needs_retry(program, start_year, end_year, "us", None):
                    plan.append({
                        "program": program,
                        "start_year": start_year,
                        "end_year": end_year,
                        "geo_level": "us",
                        "state_fips": None,
                        "series_hash": meta["series_hash"],
                        "series_count": meta["series_count"],
                    })
                
                # State level
                if not is_done(program, start_year, end_year, "state", None) or needs_retry(program, start_year, end_year, "state", None):
                    plan.append({
                        "program": program,
                        "start_year": start_year,
                        "end_year": end_year,
                        "geo_level": "state",
                        "state_fips": None,
                        "series_hash": meta["series_hash"],
                        "series_count": meta["series_count"],
                    })
                
                # County level (by state)
                for sf in state_fips_list:
                    if not is_done(program, start_year, end_year, "county", sf) or needs_retry(program, start_year, end_year, "county", sf):
                        plan.append({
                            "program": program,
                            "start_year": start_year,
                            "end_year": end_year,
                            "geo_level": "county",
                            "state_fips": sf,
                            "series_hash": meta["series_hash"],
                            "series_count": meta["series_count"],
                        })
            
            else:
                # Other programs: national series only
                if not is_done(program, start_year, end_year, None, None) or needs_retry(program, start_year, end_year, None, None):
                    plan.append({
                        "program": program,
                        "start_year": start_year,
                        "end_year": end_year,
                        "geo_level": None,
                        "state_fips": None,
                        "series_hash": meta["series_hash"],
                        "series_count": meta["series_count"],
                    })
        
        # Batch for mapping
        batches = chunk_list(plan, chunk_size=20)
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
            UPDATE raw_bls.bls_ingestion_slices
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
            WHERE program = %s
              AND year_start = %s
              AND year_end = %s
              AND geo_level IS NOT DISTINCT FROM %s
              AND state_fips IS NOT DISTINCT FROM %s;
        """
        
        sql_planned_insert = """
            INSERT INTO raw_bls.bls_ingestion_slices (
                program, year_start, year_end, geo_level, state_fips,
                status, rows_loaded,
                started_at, finished_at, last_error,
                series_hash, series_count, series_hash_seen_at
            )
            VALUES (%s, %s, %s, %s, %s,
                    'planned', 0,
                    NULL, NULL, NULL,
                    %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            for batch in batches:
                for w in batch:
                    program = w["program"]
                    start_year = int(w["start_year"])
                    end_year = int(w["end_year"])
                    geo_level = w.get("geo_level")
                    state_fips = w.get("state_fips")
                    shash = w.get("series_hash")
                    scount = int(w.get("series_count", 0))
                    
                    cur.execute(
                        sql_planned_update,
                        (shash, shash, shash, scount, now,
                         program, start_year, end_year, geo_level, state_fips),
                    )
                    
                    if cur.rowcount == 0:
                        cur.execute(
                            sql_planned_insert,
                            (program, start_year, end_year, geo_level, state_fips,
                             shash, scount, now),
                        )
            
            conn.commit()
    
    # -----------------------------
    # Task 4: Ingest batch (mapped)
    # -----------------------------
    @task(pool=BLS_API_POOL)
    def ingest_batch(batch: list[dict]) -> int:
        """Ingest a batch of work units sequentially."""
        total = 0
        for work_unit in batch:
            total += _run_one_work_unit(work_unit)
        return total
    
    # -----------------------------
    # DAG wiring
    # -----------------------------
    sync_ds = sync_datasets()
    sync_meta = sync_metadata()
    
    plan = build_ingestion_plan(sync_meta)
    
    sync_ds >> sync_meta >> plan
    
    planned = mark_slices_planned(plan)
    plan >> planned
    
    _ = ingest_batch.expand(batch=plan)


# Instantiate DAG
bls_raw_ingest_dag = bls_raw_ingest()
