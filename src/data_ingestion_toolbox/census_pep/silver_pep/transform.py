"""
Census PEP silver transform — pivots ``silver_pep.observation_revision``
into ``silver_pep.fact_population`` with shared dimension keys.

The transform operates year-by-year in memory-safe chunks, mapping state
FIPS codes to ``silver_ref.dim_geography`` keys and calendar years to
``silver_ref.dim_time`` keys.  It also extracts column metadata into
``silver_pep.pep_column_metadata`` for API discovery.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import polars as pl
import psycopg2
from psycopg2.extras import execute_values

from data_ingestion_toolbox.census_pep.config import CONFIG

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch / retry tuning
# ---------------------------------------------------------------------------

_LARGE_DATASET_ROW_THRESHOLD = 500_000
_INSERT_SUB_BATCH_SIZE = 500_000
_UPSERT_SUB_BATCH_SIZE = 100_000
_INSERT_MAX_RETRIES = 3
_INSERT_RETRY_BASE_DELAY = 5  # seconds


# ---------------------------------------------------------------------------
# Metrics tracking
# ---------------------------------------------------------------------------

@dataclass
class PepTransformMetrics:
    """Track and log PEP silver transform metrics."""

    dataset_name: str = "CENSUS_PEP"

    # Pre-transform
    raw_rows_by_year: dict[int, int] = field(default_factory=dict)
    column_issues: list[str] = field(default_factory=list)

    # Per-chunk
    chunk_input_rows: int = 0
    chunk_output_rows: int = 0
    rows_missing_geo: int = 0
    rows_missing_time: int = 0
    rows_deduplicated: int = 0
    time_dim_hits: int = 0
    time_dim_misses: int = 0
    geo_dim_hits: int = 0
    geo_dim_misses: int = 0

    # Insert
    insert_duration_sec: float = 0.0
    insert_total: int = 0

    # Upsert
    upsert_duration_sec: float = 0.0
    upsert_inserted: int = 0
    upsert_updated: int = 0
    upsert_total: int = 0

    # Post-transform
    total_processed: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    errors_encountered: list[str] = field(default_factory=list)
    columns_extracted: int = 0

    def log_pre_transform(self) -> None:
        if self.raw_rows_by_year:
            years_summary = "; ".join(
                f"year={y}:{count:,} rows"
                for y, count in sorted(self.raw_rows_by_year.items())
            )
            logger.info(
                "[CENSUS_PEP] PRE-TRANSFORM] Raw row count by year: %s (total: %d)",
                years_summary,
                sum(self.raw_rows_by_year.values()),
            )
        if self.column_issues:
            logger.warning(
                "[CENSUS_PEP] PRE-TRANSFORM] Column metadata issues: %s",
                "; ".join(self.column_issues),
            )

    def log_chunk_start(self, year: int, input_rows: int) -> None:
        self.chunk_input_rows = input_rows
        logger.info(
            "[CENSUS_PEP] CHUNK] Processing year=%d with %d raw rows",
            year,
            input_rows,
        )

    def log_chunk_complete(self, year: int) -> None:
        logger.info(
            "[CENSUS_PEP] CHUNK] year=%d : %s input → %s output, geo_dim_misses=%s, time_dim_misses=%s",
            year,
            f"{self.chunk_input_rows:,}",
            f"{self.chunk_output_rows:,}",
            f"{self.geo_dim_misses:,}",
            f"{self.time_dim_misses:,}",
        )
        if self.rows_missing_time or self.rows_missing_geo:
            logger.warning(
                "[CENSUS_PEP] CHUNK] Rows filtered: missing_time=%s, missing_geo=%s",
                f"{self.rows_missing_time:,}",
                f"{self.rows_missing_geo:,}",
            )

    def log_insert_complete(self, inserted: int, duration_sec: float) -> None:
        self.insert_total += inserted
        self.insert_duration_sec += duration_sec
        rate = inserted / duration_sec if duration_sec > 0 else 0
        logger.info(
            "[CENSUS_PEP] INSERT] Completed in %.2f sec: %s rows inserted (%.0f rows/sec)",
            duration_sec,
            f"{inserted:,}",
            rate,
        )

    def log_transform_summary(self) -> None:
        logger.info(
            "[CENSUS_PEP] SUMMARY] Transform complete: %s rows processed, "
            "%s net-new inserted, columns_extracted=%d, errors=%d",
            f"{self.total_processed:,}",
            f"{self.total_inserted:,}",
            self.columns_extracted,
            len(self.errors_encountered),
        )
        if self.errors_encountered:
            for err in self.errors_encountered:
                logger.error("[CENSUS_PEP] SUMMARY] Error: %s", err)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _load_time_dim(
    hook: PostgresHook,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """Pre-load the shared time dimension for the requested date range."""
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT time_sk, date_key FROM silver_ref.dim_time WHERE date_key BETWEEN %s AND %s ORDER BY date_key",
            (str(start_date), str(end_date)),
        )
        rows = cur.fetchall()
    if rows:
        return pl.DataFrame(rows, schema=["time_sk", "date_key"], orient="row")
    return pl.DataFrame(schema={"time_sk": pl.Int64, "date_key": pl.Date})


def _load_geo_dim(hook: PostgresHook) -> pl.DataFrame:
    """Pre-load state-level geography dimension (PEP uses state FIPS as primary key)."""
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT geo_sk, state_fips FROM silver_ref.dim_geography WHERE state_fips IS NOT NULL"
        )
        rows = cur.fetchall()
    if rows:
        return pl.DataFrame(rows, schema=["geo_sk", "state_fips"], orient="row")
    return pl.DataFrame(schema={"geo_sk": pl.Int64, "state_fips": pl.Variant})


def _get_approx_row_count(hook: PostgresHook, table: str = "silver_pep.observation_revision") -> int:
    """Estimate row count via pg_class (fast, approximate)."""
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s",
            (table,),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] else 0


# ---------------------------------------------------------------------------
# Column metadata extraction
# ---------------------------------------------------------------------------

def _extract_column_metadata(
    hook: PostgresHook,
    year: int,
) -> int:
    """Extract distinct column definitions from observation_revision into pep_column_metadata.

    Returns the number of columns extracted.
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        # Truncate and rebuild column metadata for this year's data
        cur.execute("TRUNCATE silver_pep.pep_column_metadata")

        cur.execute(
            """
            INSERT INTO silver_pep.pep_column_metadata (
                variable_code, variable_label, concept, universe,
                data_type, is_numeric, is_geometry, source_year
            )
            SELECT DISTINCT
                variable_name AS variable_code,
                variable_name AS variable_label,
                CASE
                    WHEN variable_name = 'POP' THEN 'Population Total'
                    WHEN variable_name LIKE 'NPOP%' THEN 'Population Estimate'
                    WHEN variable_name LIKE 'MAR%' THEN 'Marriage'
                    WHEN variable_name LIKE 'DIV%' THEN 'Divorce'
                    WHEN variable_name LIKE 'BIRTH%' THEN 'Birth'
                    WHEN variable_name LIKE 'DEATH%' THEN 'Death'
                    ELSE variable_name
                END AS concept,
                CASE
                    WHEN variable_name = 'POP' THEN 'Total population'
                    WHEN variable_name LIKE 'NPOP%' THEN 'Noninstitutional population'
                    ELSE 'See concept'
                END AS universe,
                'numeric' AS data_type,
                CASE WHEN variable_name ~ '^[0-9]+$' THEN false ELSE true END AS is_numeric,
                false AS is_geometry,
                %s AS source_year
            FROM silver_pep.observation_revision
            WHERE year = %s
              AND value_status = 'valid'
              AND value IS NOT NULL
            ORDER BY variable_name
            """,
            (year, year),
        )
        conn.commit()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM silver_pep.pep_column_metadata WHERE source_year = %s",
            (year,),
        )
        count = cur.fetchone()[0]
    logger.info("[CENSUS_PEP] Extracted %d column metadata entries for year %d", count, year)
    return count


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------

def _upsert_silver_rows(
    hook: PostgresHook,
    df: pl.DataFrame,
    load_batch_id: uuid.UUID,
    ingested_at: datetime,
) -> int:
    """Upsert transformed PEP rows into ``silver_pep.fact_population``.

    Uses an upsert strategy because PEP data is subject to revision when
    Census Bureau releases updated population estimates.
    """
    upsert_cols = [
        "dataset", "geo_id", "variable_code", "geo_id", "estimate_year",
        "time_sk", "geo_sk", "duration_start", "duration_end",
        "estimate_value", "margin_of_error", "margin_of_error_pct",
        "variable_label", "variable_concept", "universe",
        "source_system", "load_batch_id", "ingested_at",
    ]
    suffix = "_pep"

    create_temp_sql = """
        CREATE TEMP TABLE temp_census_upsert ON COMMIT DROP AS
        SELECT * FROM (VALUES %s) AS t (
            dataset TEXT, table_id TEXT, variable_code TEXT,
            geo_id TEXT, estimate_year INT,
            time_sk BIGINT, geo_sk BIGINT,
            duration_start DATE, duration_end DATE,
            estimate_value BIGINT, margin_of_error BIGINT,
            margin_of_error_pct FLOAT,
            variable_label TEXT, variable_concept TEXT, universe TEXT,
            source_system TEXT, load_batch_id TEXT, ingested_at TIMESTAMPTZ
        );
    """

    insert_temp_sql = """
        INSERT INTO temp_census_upsert (
            dataset, table_id, variable_code, geo_id, estimate_year,
            time_sk, geo_sk, duration_start, duration_end,
            estimate_value, margin_of_error, margin_of_error_pct,
            variable_label, variable_concept, universe,
            source_system, load_batch_id, ingested_at
        ) VALUES %s
    """

    merge_sql = """
        INSERT INTO silver_pep.fact_population (
            dataset, table_id, variable_code, geo_id, estimate_year,
            time_sk, geo_sk, duration_start, duration_end,
            estimate_value, margin_of_error, margin_of_error_pct,
            variable_label, variable_concept, universe,
            source_system, load_batch_id, ingested_at
        )
        SELECT
            dataset, table_id, variable_code, geo_id, estimate_year,
            time_sk, geo_sk, duration_start, duration_end,
            estimate_value, margin_of_error, margin_of_error_pct,
            variable_label, variable_concept, universe,
            source_system, load_batch_id, ingested_at
        FROM temp_census_upsert
        ON CONFLICT (dataset, geo_id, variable_code, estimate_year)
        DO UPDATE SET
            time_sk = EXCLUDED.time_sk,
            geo_sk = EXCLUDED.geo_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            estimate_value = EXCLUDED.estimate_value,
            margin_of_error = EXCLUDED.margin_of_error,
            margin_of_error_pct = EXCLUDED.margin_of_error_pct,
            variable_label = EXCLUDED.variable_label,
            variable_concept = EXCLUDED.variable_concept,
            universe = EXCLUDED.universe,
            source_system = EXCLUDED.source_system,
            load_batch_id = EXCLUDED.load_batch_id,
            ingested_at = EXCLUDED.ingested_at
        WHERE (
            silver_pep.fact_population.time_sk,
            silver_pep.fact_population.geo_sk,
            silver_pep.fact_population.duration_start,
            silver_pep.fact_population.duration_end,
            silver_pep.fact_population.estimate_value,
            silver_pep.fact_population.margin_of_error,
            silver_pep.fact_population.margin_of_error_pct,
            silver_pep.fact_population.variable_label,
            silver_pep.fact_population.variable_concept,
            silver_pep.fact_population.universe,
            silver_pep.fact_population.source_system
        ) IS DISTINCT FROM (
            EXCLUDED.time_sk,
            EXCLUDED.geo_sk,
            EXCLUDED.duration_start,
            EXCLUDED.duration_end,
            EXCLUDED.estimate_value,
            EXCLUDED.margin_of_error,
            EXCLUDED.margin_of_error_pct,
            EXCLUDED.variable_label,
            EXCLUDED.variable_concept,
            EXCLUDED.universe,
            EXCLUDED.source_system
        );
    """

    affected_rows = 0
    num_batches = (df.height + _UPSERT_SUB_BATCH_SIZE - 1) // _UPSERT_SUB_BATCH_SIZE

    try:
        with hook.get_conn() as conn:
            for batch_idx in range(num_batches):
                offset = batch_idx * _UPSERT_SUB_BATCH_SIZE
                batch_df = df.slice(offset, _UPSERT_SUB_BATCH_SIZE)
                records = [row + suffix for row in batch_df.select(upsert_cols).rows()]

                with conn.cursor() as cur:
                    if batch_idx == 0:
                        cur.execute(create_temp_sql)
                    else:
                        cur.execute("TRUNCATE temp_census_upsert;")
                    execute_values(cur, insert_temp_sql, records, page_size=10000)
                    cur.execute(merge_sql)
                    changed_now = (
                        cur.rowcount
                        if cur.rowcount is not None and cur.rowcount >= 0
                        else 0
                    )
                conn.commit()
                affected_rows += changed_now
                logger.info(
                    "[CENSUS_PEP UPSERT] Completed sub-batch %d/%d: input=%d, changed=%d",
                    batch_idx + 1,
                    num_batches,
                    len(records),
                    changed_now,
                )
    except Exception:
        logger.exception("Failed to upsert Census PEP silver rows")
        raise

    return affected_rows


# ---------------------------------------------------------------------------
# Public transform function
# ---------------------------------------------------------------------------

def transform_pep_to_silver() -> int:
    """Transform all PEP raw observations into silver layer.

    Processes captured observations in memory-safe year chunks and upserts
    ``silver_pep.fact_population``. PEP errata on an existing geography
    propagate, while equivalent replays report zero changed rows.

    Returns
    -------
    int
        Number of rows inserted (changed) in the silver table.
    """
    hook = _get_hook()
    metrics = PepTransformMetrics(dataset_name="CENSUS_PEP")

    logger.info("[CENSUS_PEP] Starting silver transform — checking dataset size...")
    approx_rows = _get_approx_row_count(hook)
    logger.info("[CENSUS_PEP] Approximate row count: %d", approx_rows)

    if approx_rows == 0:
        logger.info("[CENSUS_PEP] No PEP rows found for silver transform")
        return 0

    load_batch_id = uuid.uuid4()
    ingested_at = datetime.now(timezone.utc)

    # ── year-level counts ─────────────────────────────────────────────
    logger.info("[CENSUS_PEP] Gathering per-year row counts...")
    sql = """
        SELECT year, COUNT(*)
        FROM silver_pep.observation_revision
        GROUP BY year ORDER BY year;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            metrics.raw_rows_by_year[int(row[0])] = int(row[1])

    total_rows = sum(metrics.raw_rows_by_year.values())
    logger.info(
        "[CENSUS_PEP] Exact row count: %d across %d years",
        total_rows,
        len(metrics.raw_rows_by_year),
    )

    if total_rows == 0:
        return 0

    years = sorted(metrics.raw_rows_by_year.keys())
    metrics.log_pre_transform()

    # ── pre-load shared dimensions ────────────────────────────────────
    logger.info("[CENSUS_PEP] Pre-loading geography dimension...")
    geo_df = _load_geo_dim(hook)
    logger.info("[CENSUS_PEP] Loaded %d geography rows", geo_df.height)

    earliest_year = min(years)
    latest_year = max(years)
    logger.info(
        "[CENSUS_PEP] Pre-loading time dimension (%d..%d)...",
        earliest_year,
        latest_year,
    )
    time_df = _load_time_dim(hook, date(earliest_year, 1, 1), date(latest_year, 12, 31))
    logger.info("[CENSUS_PEP] Loaded %d time dimension rows", time_df.height)

    # ── process each year ─────────────────────────────────────────────
    inserted_total = 0

    for y in years:
        # Fetch raw observation_revision rows for this year
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT capture_id, year, file_type,
                       state_fips_source, county_fips_source,
                       place_fips_source, name_source, us_source,
                       variable_name, value_source, value, value_status
                FROM silver_pep.observation_revision
                WHERE year = %s
                ORDER BY capture_id, source_row_index, source_column_index
                """,
                (y,),
            )
            rows = cur.fetchall()

        if not rows:
            continue

        metrics.log_chunk_start(y, len(rows))

        # Build Polars DataFrame for vectorized dimension mapping
        df_raw = pl.DataFrame(
            rows,
            schema=[
                "capture_id", "year", "file_type",
                "state_fips_source", "county_fips_source",
                "place_fips_source", "name_source", "us_source",
                "variable_name", "value_source", "value", "value_status",
            ],
            orient="row",
        )

        # Filter to valid values only
        df_valid = df_raw.filter(
            (pl.col("value_status") == "valid") & (pl.col("value").is_not_null())
        )

        if df_valid.is_empty():
            metrics.rows_missing_time = len(df_raw)
            metrics.rows_missing_geo = len(df_raw)
            metrics.total_processed += len(df_raw)
            continue

        # Map geography dimension
        geo_map = {}
        for geo_sk, state_fips in zip(geo_df["geo_sk"].to_list(), geo_df["state_fips"].to_list()):
            if state_fips is not None:
                geo_map[str(state_fips)] = geo_sk

        # Map time dimension
        time_map = {}
        for time_sk, date_key in zip(time_df["time_sk"].to_list(), time_df["date_key"].to_list()):
            if date_key is not None:
                yr = date_key.year
                time_map[yr] = time_sk

        # Build silver rows
        silver_rows = []
        for row in df_valid.rows():
            capture_id, row_year, file_type = row[0], row[1], row[2]
            state_fips = str(row[3]) if row[3] else None
            variable_name = row[8]
            value = row[10]

            geo_sk = geo_map.get(state_fips) if state_fips else None
            time_sk = time_map.get(row_year)

            if geo_sk is None:
                metrics.geo_dim_misses += 1
                metrics.rows_missing_geo += 1
                continue
            if time_sk is None:
                metrics.time_dim_misses += 1
                metrics.rows_missing_time += 1
                continue

            metrics.geo_dim_hits += 1
            metrics.time_dim_hits += 1

            # Determine geo_id from geography level
            geo_id = f"FIPS:{state_fips}"

            # Compute margin_of_error_pct (PEP doesn't provide MoE; estimate 2%)
            moe_pct = 2.0 if value is not None and value != 0 else 0.0
            moe = int(abs(value) * moe_pct / 100) if value is not None else None

            silver_rows.append((
                "pep",               # dataset
                variable_name[:10],  # table_id (first 10 chars of variable)
                variable_name,        # variable_code
                geo_id,              # geo_id
                row_year,            # estimate_year
                time_sk,             # time_sk
                geo_sk,              # geo_sk
                date(row_year, 1, 1),  # duration_start
                date(row_year, 12, 31), # duration_end
                int(value) if value is not None else None,  # estimate_value
                moe,                 # margin_of_error
                moe_pct,            # margin_of_error_pct
                variable_name,       # variable_label
                variable_name,       # variable_concept
                "Total population estimate",  # universe
                "CENSUS_PEP",       # source_system
                str(load_batch_id), # load_batch_id
                ingested_at,        # ingested_at
            ))

        if not silver_rows:
            metrics.total_processed += len(df_raw)
            continue

        df_silver = pl.DataFrame(
            silver_rows,
            schema=[
                "dataset", "table_id", "variable_code", "geo_id", "estimate_year",
                "time_sk", "geo_sk", "duration_start", "duration_end",
                "estimate_value", "margin_of_error", "margin_of_error_pct",
                "variable_label", "variable_concept", "universe",
                "source_system", "load_batch_id", "ingested_at",
            ],
            orient="row",
        )

        transformed_count = df_silver.height

        # Upsert into silver
        insert_start = datetime.now(timezone.utc)
        changed = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
        insert_duration = (datetime.now(timezone.utc) - insert_start).total_seconds()

        metrics.rows_net_new = changed
        metrics.rows_already_existed = transformed_count - changed
        metrics.chunk_output_rows = transformed_count
        metrics.log_chunk_complete(y)
        metrics.log_insert_complete(changed, insert_duration)
        inserted_total += changed
        metrics.total_inserted += changed
        metrics.total_processed += len(df_raw)

        # Extract column metadata for this year
        metrics.columns_extracted += _extract_column_metadata(hook, y)

    metrics.log_transform_summary()
    logger.info("[CENSUS_PEP] Silver transform complete: %d rows inserted total", inserted_total)
    return inserted_total
