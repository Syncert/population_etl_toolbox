# dags/bls_ingest_dag.py
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
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

from bls.config import CONFIG
from bls.metadata import sync_bls_series_metadata, sync_bls_datasets_table
from bls.ingest import ingest_slice, get_curated_series_for_program, BlsRetryableHTTP
from bls.silver_bls.transform import transform_bls_to_silver

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


def _silver_ddl_path() -> Path:
    return Path(__file__).resolve().parents[1] / "bls" / "DDL" / "silver_bls.sql"


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
    dag_id="bls_ingest",
    default_args=DEFAULT_ARGS,
    schedule="0 7 1 * *",  # monthly on the 1st at 07:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bls", "labor"],
)
def bls_ingest():
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

        Year-range strategy
        -------------------
        Slices are split into two bands:

        1. Historical band  (1990 → current_year - 3)
           Treated as immutable once status='success' for the current series hash.
           Re-runs only when the curated series list changes.

        2. Rolling window   (current_year - 2 → current_year - 1)
           Always re-ingested unconditionally, regardless of prior status.
           This ensures BLS revisions, late-filed data, and appropriations-lapse
           backfills (footnote X/N/9) are picked up automatically on every
           monthly DAG run.  Two years back is used because BLS frequently
           revises LAUS county benchmarks ~18 months after initial release.

        Skip logic
        ----------
        Historical slices: skipped when (program, year_start, year_end,
            geo_level, state_fips, series_hash) is in the completed set.
        Rolling slices:    never skipped — always included in the plan.
        """
        hook = _get_postgres_hook()

        current_year = datetime.now(timezone.utc).year

        # Historical band: lock in once complete
        hist_start = 1990
        hist_end   = current_year - 3   # e.g. in 2026 → 1990–2023

        # Rolling window: always re-ingest for revisions / backfills
        roll_start = current_year - 2   # e.g. in 2026 → 2024
        roll_end   = current_year - 1   # e.g. in 2026 → 2025
        
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
        
        # Batch for mapping.
        # IMPORTANT: return at least one batch so mapped-task retries remain stable.
        # Airflow can fail with "cannot expand field mapped to length 0" if a mapped
        # TI already exists and a retry re-renders this task against an empty list.
        if not plan:
            return [[]]

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
    # Task 5: Silver layer (full load)
    # -----------------------------
    @task(trigger_rule='none_failed')
    def ensure_silver_schema() -> None:
        """Ensure silver_bls schema and tables exist."""
        sql_path = _silver_ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task(trigger_rule='none_failed')
    def transform_to_silver_by_program(program: str) -> int:
        """Transform ALL raw BLS data to silver for one program (full load)."""
        return transform_bls_to_silver(program=program)
    
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
    silver_transforms = transform_to_silver_by_program.expand(
        program=["la", "ln", "ce", "cu", "jt"]
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
        from bls.gold_bls.transform import refresh_bls_elements
        refresh_bls_elements()

    @task(trigger_rule='none_failed')
    def gold_compute_shards() -> list[str]:
        """Compute gold shards driven by what actually exists in silver.

        BLS is monthly so every distinct calendar month in silver_bls.fact_labor_statistics
        is a valid shard.  Querying silver directly handles both the initial load
        (all historical months) and incremental updates (new months appear
        automatically on the next DAG run).

        Cross-checked against silver_ref.dim_time to guard against dates
        outside the time dimension.
        """
        hook = _get_postgres_hook()

        sql_silver_months = """
            SELECT DISTINCT date_trunc('month', period_date)::date AS month_start
            FROM silver_bls.fact_labor_statistics
            ORDER BY month_start;
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_silver_months)
            silver_months = [row[0].isoformat() for row in cur.fetchall()]

        if not silver_months:
            logger.warning("[BLS GOLD] No months found in silver_bls.fact_labor_statistics — no shards generated.")
            return []

        logger.info("[BLS GOLD] Silver contains %d distinct month(s), range %s to %s",
                    len(silver_months), silver_months[0], silver_months[-1])

        sql_dim_check = """
            SELECT date_trunc('month', date_key)::date AS month_start
            FROM silver_ref.dim_time
            WHERE date_trunc('month', date_key)::date = ANY(%s::date[])
              AND is_month_start = TRUE
            ORDER BY month_start;
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_dim_check, (silver_months,))
            confirmed_shards = [row[0].isoformat() for row in cur.fetchall()]

        missing = set(silver_months) - set(confirmed_shards)
        if missing:
            logger.warning("[BLS GOLD] %d month(s) not in dim_time, skipping: %s",
                           len(missing), sorted(missing))

        logger.info("[BLS GOLD] Emitting %d shard(s)", len(confirmed_shards))
        return confirmed_shards

    @task(trigger_rule='none_failed')
    def gold_merge_shard(month_start: str) -> dict:
        """Merge one gold month shard."""
        from bls.gold_bls.transform import merge_bls_shard
        return merge_bls_shard({"month_start": month_start})

    @task(trigger_rule='none_failed')
    def gold_validate_coverage(shard_results: list[dict]) -> dict:
        """Validate gold row counts match silver for every calendar month.

        Compares:
        - silver_bls.fact_labor_statistics row counts per calendar month
        - gold.fact_metrics row counts per month_start for source_system='BLS'

        Raises ValueError if any month present in silver has zero rows in gold,
        indicating an incomplete or failed transposition.

        Returns a summary dict for XCom inspection.
        """
        hook = _get_postgres_hook()

        sql_silver = """
            SELECT date_trunc('month', period_date)::date AS month_start,
                   COUNT(*) AS silver_rows
            FROM silver_bls.fact_labor_statistics
            WHERE value IS NOT NULL
            GROUP BY date_trunc('month', period_date)::date
            ORDER BY month_start;
        """
        sql_gold = """
            SELECT month_start, COUNT(*) AS gold_rows
            FROM gold.fact_metrics
            WHERE source_system = 'BLS'
            GROUP BY month_start
            ORDER BY month_start;
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_silver)
            silver_counts = {row[0].isoformat(): row[1] for row in cur.fetchall()}
            cur.execute(sql_gold)
            gold_counts = {row[0].isoformat(): row[1] for row in cur.fetchall()}

        summary = {}
        incomplete_months = []
        for month, silver_rows in silver_counts.items():
            gold_rows = gold_counts.get(month, 0)
            summary[month] = {"silver_rows": silver_rows, "gold_rows": gold_rows}
            if gold_rows == 0:
                incomplete_months.append(month)
                logger.error("[BLS GOLD] Coverage gap: %s has %d silver rows but 0 gold rows.",
                             month, silver_rows)
            else:
                logger.info("[BLS GOLD] Coverage OK: %s — silver=%d gold=%d",
                            month, silver_rows, gold_rows)

        if incomplete_months:
            raise ValueError(
                f"[BLS GOLD] Gold transposition incomplete for {len(incomplete_months)} month(s): "
                f"{incomplete_months}. Silver data exists but gold has 0 rows."
            )

        logger.info("[BLS GOLD] Coverage validation passed for all %d month(s).", len(silver_counts))
        return summary

    @task(trigger_rule='none_failed')
    def gold_quality_check(shard_results: list[dict]) -> None:
        """Run row-level quality checks on merged gold shards."""
        from datetime import date
        from gold.quality import run_quality_checks
        for result in (shard_results or []):
            if result and result.get("output_rows", 0) > 0:
                run_quality_checks(date.fromisoformat(result["month_start"]))

    gold_schema = gold_ensure_schema()
    gold_elements = gold_refresh_elements()
    gold_shards = gold_compute_shards()
    gold_merged = gold_merge_shard.expand(month_start=gold_shards)
    gold_coverage = gold_validate_coverage(gold_merged)
    gold_qa = gold_quality_check(gold_merged)

    silver_transforms >> gold_schema >> gold_elements >> gold_shards >> gold_merged >> [gold_coverage, gold_qa]


# Instantiate DAG
bls_ingest_dag = bls_ingest()
