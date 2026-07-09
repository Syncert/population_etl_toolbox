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
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

try:
    from data_ingestion_toolbox.bls.config import CONFIG
    from data_ingestion_toolbox.bls.metadata import sync_bls_series_metadata, sync_bls_datasets_table
    from data_ingestion_toolbox.bls.ingest import ingest_slice, get_curated_series_for_program, BlsRetryableHTTP, BlsDailyThresholdExceeded
    from data_ingestion_toolbox.bls.silver_bls.transform import transform_bls_to_silver
except ImportError:
    # Backward-compatible fallback for legacy Airflow layouts that copy
    # sibling folders (silver_ref/, bls/, census_acs/, fred/) next to dags/.
    from bls.config import CONFIG
    from bls.metadata import sync_bls_series_metadata, sync_bls_datasets_table
    from bls.ingest import ingest_slice, get_curated_series_for_program, BlsRetryableHTTP, BlsDailyThresholdExceeded
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
    root = Path(__file__).resolve().parents[1]
    candidates = [
        root / "src" / "data_ingestion_toolbox" / "bls" / "DDL" / "silver_bls.sql",
        root / "bls" / "DDL" / "silver_bls.sql",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"BLS silver DDL not found. Checked: {candidates}")


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


_EST = ZoneInfo("America/New_York")


def _compute_delay_until_1am_est() -> timedelta:
    """
    Return the timedelta from now until 1:00am US/Eastern on the next calendar day.

    Using the next calendar day (not +24h) means the retry always lands at 01:00
    regardless of when within the current day the threshold was hit.
    """
    now_est = datetime.now(_EST)
    next_day = (now_est + timedelta(days=1)).date()
    target = datetime(next_day.year, next_day.month, next_day.day, 1, 0, 0, tzinfo=_EST)
    return target - now_est


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
    
    except BlsDailyThresholdExceeded as e:
        # BLS daily API quota exhausted.  Mark slice 'planned' so the Airflow
        # retry picks it up, then re-raise so Airflow records the task as
        # up_for_retry rather than success.
        #
        # Before re-raising, write the computed delay onto ti.task.retry_delay.
        # Airflow reads this attribute when scheduling the retry; overriding it
        # here targets 1:00am US/Eastern on the next calendar day regardless of
        # when within the current day the threshold was hit.  The task runs in
        # an isolated worker process, so mutating the task object is safe.
        # ingest_batch has retries=10, covering up to 10 consecutive days of
        # quota exhaustion — adequate for extensive backfills.
        finished = datetime.now(timezone.utc)
        err_txt = str(e)[:4000]
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

        retry_delay = _compute_delay_until_1am_est()
        retry_at_str = (datetime.now(_EST) + retry_delay).strftime("%Y-%m-%d %H:%M %Z")
        try:
            ctx = get_current_context()
            ctx["ti"].task.retry_delay = retry_delay
        except Exception:
            pass  # Outside Airflow (unit tests): fall back to decorator default.

        logger.warning(
            "[BLS] Daily API threshold exceeded for %s %s-%s (geo=%s, state=%s) — "
            "Airflow will retry at %s.",
            program, start_year, end_year, geo_level, state_fips, retry_at_str,
        )
        raise

    except Exception as e:
        finished = datetime.now(timezone.utc)
        err_txt = str(e)[:4000]

        # Transient HTTP errors (429 / 5xx): mark 'planned', suppress so the
        # rest of the batch continues.  These will be retried on the next run.
        if isinstance(e, BlsRetryableHTTP):
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
            logger.warning(
                "[Retryable Error] %s %s-%s (geo=%s, state=%s): %s",
                program, start_year, end_year, geo_level, state_fips, err_txt,
            )
            return 0

        else:
            # Unexpected errors: mark as 'failed' and propagate.
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


def _is_work_unit_done_for_current_hash(work_unit: dict) -> bool:
    """
    Return True when a slice is already success/empty for this work unit's series hash.

    This is used only during mapped-task retries to avoid re-calling the BLS API
    for work units that completed successfully in an earlier attempt.
    """
    hook = _get_postgres_hook()

    program = work_unit["program"]
    start_year = int(work_unit["start_year"])
    end_year = int(work_unit["end_year"])
    geo_level = work_unit.get("geo_level")
    state_fips = work_unit.get("state_fips")
    series_hash = work_unit.get("series_hash")

    sql = """
        SELECT status, series_hash
        FROM raw_bls.bls_ingestion_slices
        WHERE program = %s
          AND year_start = %s
          AND year_end = %s
          AND geo_level IS NOT DISTINCT FROM %s
          AND state_fips IS NOT DISTINCT FROM %s;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (program, start_year, end_year, geo_level, state_fips))
        row = cur.fetchone()

    if not row:
        return False

    status, existing_hash = row
    return status in ("success", "empty") and existing_hash == series_hash


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

        2. Rolling window   (current_year - 2 → current_year)
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
        roll_end   = current_year   # e.g. in 2026 → 2026
        
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

        # Load completed and planned slices in one round-trip.
        completed = set()
        planned_to_retry = set()
        sql_slice_status = """
            SELECT program, year_start, year_end, geo_level, state_fips, series_hash, status
            FROM raw_bls.bls_ingestion_slices
            WHERE status IN ('success', 'empty', 'planned');
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_slice_status)
            for program, sy, ey, geo_level, state_fips, series_hash, status in cur.fetchall():
                key = (
                    str(program), int(sy), int(ey),
                    geo_level if geo_level is not None else None,
                    state_fips if state_fips is not None else None,
                )
                if status in ('success', 'empty'):
                    completed.add((*key, series_hash))
                else:  # 'planned'
                    planned_to_retry.add(key)

        def hist_is_done(
            program: str, sy: int, ey: int,
            geo_level: Optional[str], state_fips: Optional[str],
        ) -> bool:
            """True if the historical slice is complete for the current series hash."""
            current_hash = series_meta[program]["series_hash"]
            return (program, sy, ey, geo_level, state_fips, current_hash) in completed

        def needs_retry(
            program: str, sy: int, ey: int,
            geo_level: Optional[str], state_fips: Optional[str],
        ) -> bool:
            """True if a previously-planned slice needs a retry."""
            return (program, sy, ey, geo_level, state_fips) in planned_to_retry

        def add_slices(
            plan: list[dict],
            program: str,
            sy: int,
            ey: int,
            geo_level: Optional[str],
            state_fips: Optional[str],
            force: bool,
        ) -> None:
            """Append a work unit if it should run (forced or not yet done)."""
            meta = series_meta[program]
            if force or not hist_is_done(program, sy, ey, geo_level, state_fips) \
                    or needs_retry(program, sy, ey, geo_level, state_fips):
                plan.append({
                    "program": program,
                    "start_year": sy,
                    "end_year": ey,
                    "geo_level": geo_level,
                    "state_fips": state_fips,
                    "series_hash": meta["series_hash"],
                    "series_count": meta["series_count"],
                })

        # Build plan across both bands
        plan: list[dict] = []

        for program in synced_programs:
            if program == "la":
                geos: list[tuple[str, Optional[str]]] = (
                    [("us", None), ("state", None)]
                    + [("county", sf) for sf in state_fips_list]
                )
                for geo_level, state_fips in geos:
                    # Historical band — skip if already done
                    if hist_end >= hist_start:
                        add_slices(plan, program, hist_start, hist_end,
                                   geo_level, state_fips, force=False)
                    # Rolling window — always re-ingest to catch revisions
                    add_slices(plan, program, roll_start, roll_end,
                               geo_level, state_fips, force=True)
            else:
                # Non-LAUS: national series only
                if hist_end >= hist_start:
                    add_slices(plan, program, hist_start, hist_end,
                               None, None, force=False)
                add_slices(plan, program, roll_start, roll_end,
                           None, None, force=True)

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
        
        sql_planned_upsert = """
            INSERT INTO raw_bls.bls_ingestion_slices (
                program, year_start, year_end, geo_level, state_fips,
                status, rows_loaded,
                started_at, finished_at, last_error,
                series_hash, series_count, series_hash_seen_at
            ) VALUES %s
            ON CONFLICT (program, year_start, year_end, COALESCE(geo_level, ''), COALESCE(state_fips, ''))
            DO UPDATE SET
                status = CASE
                        WHEN raw_bls.bls_ingestion_slices.status IN ('success', 'empty')
                             AND raw_bls.bls_ingestion_slices.series_hash = EXCLUDED.series_hash
                        THEN raw_bls.bls_ingestion_slices.status
                        ELSE 'planned'
                    END,
                rows_loaded = CASE
                        WHEN raw_bls.bls_ingestion_slices.status IN ('success', 'empty')
                             AND raw_bls.bls_ingestion_slices.series_hash = EXCLUDED.series_hash
                        THEN raw_bls.bls_ingestion_slices.rows_loaded
                        ELSE 0
                    END,
                series_hash = EXCLUDED.series_hash,
                series_count = EXCLUDED.series_count,
                series_hash_seen_at = EXCLUDED.series_hash_seen_at,
                last_error = NULL;
        """
        
        rows = [
            (
                w["program"], int(w["start_year"]), int(w["end_year"]),
                w.get("geo_level"), w.get("state_fips"),
                'planned', 0, None, None, None,
                w.get("series_hash"), int(w.get("series_count", 0)), now,
            )
            for batch in batches for w in batch
        ]
        
        if rows:
            with hook.get_conn() as conn, conn.cursor() as cur:
                execute_values(cur, sql_planned_upsert, rows, page_size=500)
                conn.commit()
    
    # -----------------------------
    # Task 4: Ingest batch (mapped)
    # -----------------------------
    @task(
        pool=BLS_API_POOL,
        # retries=10 covers up to 10 consecutive days of BLS daily-quota exhaustion
        # (REQUEST_NOT_PROCESSED), adequate for extensive backfills.  retry_delay is
        # a static fallback only — the except BlsDailyThresholdExceeded block overrides
        # it dynamically to target 1:00am US/Eastern the next calendar day.
        retries=10,
        retry_delay=timedelta(hours=23),  # fallback; normally overridden dynamically
    )
    def ingest_batch(batch: list[dict]) -> int:
        """Ingest a batch of work units sequentially."""
        try:
            ctx = get_current_context()
            try_number = int(ctx["ti"].try_number)
        except Exception:
            try_number = 1

        total = 0
        for work_unit in batch:
            if try_number > 1 and _is_work_unit_done_for_current_hash(work_unit):
                logger.info(
                    "[Retry Skip] Skipping already-successful slice on retry: "
                    "program=%s years=%s-%s geo=%s state=%s",
                    work_unit["program"],
                    work_unit["start_year"],
                    work_unit["end_year"],
                    work_unit.get("geo_level"),
                    work_unit.get("state_fips"),
                )
                continue
            total += _run_one_work_unit(work_unit)
        return total

    # -----------------------------
    # Task 5: Silver layer (full load)
    # -----------------------------
    @task(trigger_rule='all_success')
    def ensure_silver_schema() -> None:
        """Ensure silver_bls schema and tables exist."""
        sql_path = _silver_ddl_path()
        sql = sql_path.read_text(encoding="utf-8")
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task(trigger_rule='all_success', max_active_tis_per_dag=CONFIG.silver_max_active_tis)
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
    @task(trigger_rule='all_success')
    def gold_ensure_schema() -> None:
        """Ensure gold schema exists."""
        try:
            from data_ingestion_toolbox.bls.gold_bls.transform import ensure_bls_gold_schema
        except ImportError:
            from bls.gold_bls.transform import ensure_bls_gold_schema
        ensure_bls_gold_schema()

    @task(trigger_rule='all_success')
    def gold_refresh_elements() -> None:
        """Refresh gold element dictionary from silver sources."""
        try:
            from data_ingestion_toolbox.bls.gold_bls.transform import refresh_bls_elements
        except ImportError:
            from bls.gold_bls.transform import refresh_bls_elements
        refresh_bls_elements()

    @task(trigger_rule='all_success')
    def gold_compute_shards() -> list[str]:
        """Compute the rolling date window covered by the current silver data.

        Returns a single-element list [month_start_iso] representing the
        earliest month in the two-year rolling window, used only for
        downstream compatibility; the actual refresh range is computed
        in gold_refresh_window from silver directly.
        """
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(period_date)::date
                FROM silver_bls.fact_labor_statistics
                WHERE period_date >= (CURRENT_DATE - INTERVAL '2 years')
                  AND value IS NOT NULL
            """)
            row = cur.fetchone()
        if not row or row[0] is None:
            logger.warning("[BLS GOLD] No data in silver rolling window; skipping gold refresh.")
            return []
        return [row[0].isoformat()]

    @task(trigger_rule='all_success')
    def gold_refresh_window(shard_results: list[str]) -> dict[str, str] | None:
        """Compute min/max date window for the serving-layer refresh from silver."""
        if not shard_results:
            return None
        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(period_date)::date, MAX(period_date)::date
                FROM silver_bls.fact_labor_statistics
                WHERE period_date >= (CURRENT_DATE - INTERVAL '2 years')
                  AND value IS NOT NULL
            """)
            row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return {"start_date": row[0].isoformat(), "end_date": row[1].isoformat()}

    @task(trigger_rule='all_success')
    def refresh_dashboard_serving_layer(refresh_window: dict[str, str] | None) -> None:
        """Refresh BLS persisted serving tables and latest snapshots."""
        started_at = datetime.now(timezone.utc)
        window_start = refresh_window["start_date"] if refresh_window else None
        window_end = refresh_window["end_date"] if refresh_window else None

        hook = _get_postgres_hook()
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0;")
            cur.execute("SET application_name = %s;", ("airflow:bls:refresh_dashboard_serving_layer",))
            cur.execute("SELECT pg_backend_pid();")
            backend_pid = cur.fetchone()[0]

            logger.info(
                "[BLS GOLD] Starting dashboard serving refresh: backend_pid=%s window_start=%s window_end=%s",
                backend_pid,
                window_start,
                window_end,
            )

            conn.notices.clear()
            if refresh_window is None:
                cur.execute("CALL gold.refresh_dashboard_serving_layer_bls(NULL, NULL);")
            else:
                cur.execute(
                    "CALL gold.refresh_dashboard_serving_layer_bls(%s, %s);",
                    (refresh_window["start_date"], refresh_window["end_date"]),
                )

            for notice in conn.notices:
                logger.info("[BLS GOLD] [DB NOTICE] %s", notice.strip())

            conn.commit()

        elapsed_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info(
            "[BLS GOLD] Completed dashboard serving refresh in %.2f seconds: backend_pid=%s window_start=%s window_end=%s",
            elapsed_seconds,
            backend_pid,
            window_start,
            window_end,
        )

    gold_schema = gold_ensure_schema()
    gold_elements = gold_refresh_elements()
    gold_shards = gold_compute_shards()
    refresh_window = gold_refresh_window(gold_shards)
    dashboard_refresh = refresh_dashboard_serving_layer(refresh_window)

    silver_transforms >> gold_schema >> gold_elements >> gold_shards >> refresh_window >> dashboard_refresh


# Instantiate DAG
bls_ingest_dag = bls_ingest()
