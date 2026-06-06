from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, date
from dataclasses import dataclass, field

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from data_ingestion_toolbox.bls.config import CONFIG as RAW_CONFIG, LAUS_MEASURE_META
from .geography_parser import parse_bls_geography
from .time_utils import parse_bls_period_to_date

logger = logging.getLogger(__name__)

BLS_API_DOC = "https://www.bls.gov/developers/api_signature_v2.htm"


LARGE_PROGRAM_ROW_THRESHOLD = 500_000


@dataclass
class TransformMetrics:
    """Track and log BLS silver transform metrics."""
    dataset_name: str
    
    # Pre-transform
    raw_rows_by_program: dict[str, int] = field(default_factory=dict)
    schema_issues: list[str] = field(default_factory=list)
    
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
    null_counts: dict[str, int] = field(default_factory=dict)
    
    # Upsert
    upsert_duration_sec: float = 0.0
    upsert_inserted: int = 0
    upsert_total: int = 0
    
    # Post-transform
    total_processed: int = 0
    total_inserted: int = 0
    errors_encountered: list[str] = field(default_factory=list)
    
    def log_pre_transform(self) -> None:
        """Log pre-transform diagnostics."""
        if self.raw_rows_by_program:
            programs_summary = "; ".join(
                f"program={p}:{count:,} rows"
                for p, count in sorted(self.raw_rows_by_program.items())
            )
            logger.info(
                "[%s PRE-TRANSFORM] Raw row count by program: %s (total: %s)",
                self.dataset_name,
                programs_summary,
                sum(self.raw_rows_by_program.values()),
            )
        
        if self.schema_issues:
            logger.warning(
                "[%s PRE-TRANSFORM] Schema validation issues: %s",
                self.dataset_name,
                "; ".join(self.schema_issues),
            )
    
    def log_chunk_start(self, program: str, input_rows: int) -> None:
        """Log start of chunk processing."""
        self.chunk_input_rows = input_rows
        logger.info(
            "[%s CHUNK] Processing program=%s with %s raw rows",
            self.dataset_name,
            program,
            input_rows,
        )
    
    def log_chunk_complete(self, program: str) -> None:
        """Log chunk processing results."""
        pct_output = (
            (self.chunk_output_rows / self.chunk_input_rows * 100)
            if self.chunk_input_rows > 0
            else 0
        )
        logger.info(
            "[%s CHUNK] Program=%s: %s input → %s output (%.1f%% retained)",
            self.dataset_name,
            program,
            self.chunk_input_rows,
            self.chunk_output_rows,
            pct_output,
        )
        
        if self.rows_missing_time or self.rows_missing_geo:
            logger.warning(
                "[%s CHUNK] Rows filtered: missing_time=%s, missing_geo=%s",
                self.dataset_name,
                self.rows_missing_time,
                self.rows_missing_geo,
            )
        
        if self.rows_deduplicated:
            logger.info(
                "[%s CHUNK] Deduplicated %s rows",
                self.dataset_name,
                self.rows_deduplicated,
            )
        
        if self.null_counts:
            null_summary = "; ".join(
                f"{col}={count:,}"
                for col, count in sorted(self.null_counts.items())
            )
            logger.info(
                "[%s CHUNK] Null counts by column: %s",
                self.dataset_name,
                null_summary,
            )
    
    def log_upsert_complete(self, upserted: int, duration_sec: float) -> None:
        """Log upsert results."""
        self.upsert_total += upserted
        self.upsert_duration_sec += duration_sec
        logger.info(
            "[%s UPSERT] Completed in %.2f sec: %s rows upserted",
            self.dataset_name,
            duration_sec,
            upserted,
        )
    
    def log_transform_summary(self) -> None:
        """Log final transform summary."""
        logger.info(
            "[%s SUMMARY] Transform complete: %s rows processed, %s upserted, errors=%s",
            self.dataset_name,
            self.total_processed,
            self.total_inserted,
            len(self.errors_encountered),
        )
        
        if self.errors_encountered:
            for err in self.errors_encountered:
                logger.error("[%s SUMMARY] Error: %s", self.dataset_name, err)



def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=RAW_CONFIG.postgres_conn_id)


def _load_time_dim(hook: PostgresHook, start_date: date, end_date: date) -> pl.DataFrame:
    sql = """
        SELECT time_sk, date_key
        from data_ingestion_toolbox.silver_ref.dim_time
        WHERE date_key BETWEEN %s AND %s;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()

    return pl.DataFrame(rows, orient="row", schema=["time_sk", "date_key"]) if rows else pl.DataFrame(
        schema=["time_sk", "date_key"]
    )


def _load_geo_dim(hook: PostgresHook) -> pl.DataFrame:
    sql = """
        SELECT geo_sk, geo_level, geo_id
        from data_ingestion_toolbox.silver_ref.dim_geo;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return pl.DataFrame(rows, orient="row", schema=["geo_sk", "geo_level", "geo_id"]) if rows else pl.DataFrame(
        schema=["geo_sk", "geo_level", "geo_id"]
    )


def _load_geo_dim_for_list(hook: PostgresHook, geo_df: pl.DataFrame) -> pl.DataFrame:
    """
    Load only geographic records that exist in the provided dataframe.
    This avoids loading entire dim_geo into memory when dealing with large datasets.
    """
    if geo_df.is_empty():
        return pl.DataFrame(schema=["geo_sk", "geo_level", "geo_id"])

    unique_geos = geo_df.select(["geo_level", "geo_id"]).unique()
    if unique_geos.is_empty():
        return pl.DataFrame(schema=["geo_sk", "geo_level", "geo_id"])

    geo_tuples = list(unique_geos.iter_rows())
    if not geo_tuples:
        return pl.DataFrame(schema=["geo_sk", "geo_level", "geo_id"])

    sql = """
        WITH needed(geo_level, geo_id) AS (VALUES %s)
        SELECT g.geo_sk, g.geo_level, g.geo_id
        from data_ingestion_toolbox.silver_ref.dim_geo g
        JOIN needed n
          ON g.geo_level = n.geo_level
         AND g.geo_id = n.geo_id;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, geo_tuples, page_size=5000)
        rows = cur.fetchall()

    return pl.DataFrame(rows, orient="row", schema=["geo_sk", "geo_level", "geo_id"]) if rows else pl.DataFrame(
        schema=["geo_sk", "geo_level", "geo_id"]
    )


def _extract_measure_code(series_id: str, program: str, fallback: str | None) -> str | None:
    if (program or "").lower() == "la" and series_id and series_id[-2:].isdigit():
        return series_id[-2:]
    return fallback


def _get_program_row_count(hook: PostgresHook, program: str) -> int:
    """Approximate row count using pg_class.reltuples to avoid a full table scan.

    reltuples is updated by ANALYZE / autovacuum and is accurate enough for
    the large-dataset threshold check.  The total table count is used as a
    conservative upper bound (may trigger year-chunking for smaller programs,
    but that is safe).
    """
    sql = """
        SELECT COALESCE(c.reltuples, 0)::bigint
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'raw_bls'
          AND c.relname = 'bls_long';
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _get_program_years(hook: PostgresHook, program: str) -> list[int]:
    sql = "SELECT DISTINCT year FROM raw_bls.bls_long WHERE program = %s ORDER BY year;"
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (program,))
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def _fetch_raw_rows(hook: PostgresHook, program: str, year: int | None = None) -> list[tuple]:
    sql = """
        SELECT
            bl.series_id,
            bl.program,
            bl.year,
            bl.period,
            bl.period_name,
            bl.value,
            bs.measure AS measure_code,
            bs.seasonal AS seasonal_adjustment,
            bs.title
        FROM raw_bls.bls_long bl
        LEFT JOIN raw_bls.bls_series bs
            ON bl.series_id = bs.series_id
            AND bl.program = bs.program
        WHERE bl.program = %s
    """
    params: list[object] = [program]
    if year is not None:
        sql += " AND bl.year = %s"
        params.append(int(year))
    sql += " ORDER BY bl.series_id, bl.year, bl.period;"

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def _upsert_silver_rows(hook: PostgresHook, df: pl.DataFrame, load_batch_id: uuid.UUID, ingested_at: datetime) -> int:
    if df.is_empty():
        return 0

    records = []
    for r in df.iter_rows(named=True):
        records.append(
            (
                r["time_sk"],
                r["geo_sk"],
                r["duration_start"],
                r["duration_end"],
                r["period_date"],
                r["series_id"],
                r["program"],
                r["geo_level"],
                r["geo_id"],
                r["state_fips"],
                r["county_fips"],
                r["value"],
                r["year"],
                r["period"],
                r["period_name"],
                r["measure_code"],
                r.get("measure_name"),
                r["seasonal_adjustment"] or "U",
                "BLS",
                load_batch_id,
                ingested_at,
            )
        )

    insert_sql = """
        INSERT INTO silver_bls.fact_labor_statistics (
            time_sk, geo_sk, duration_start, duration_end,
            period_date, series_id, program,
            geo_level, geo_id, state_fips, county_fips,
            value, year, period, period_name,
            measure_code, measure_name, seasonal_adjustment,
            source_system, load_batch_id, ingested_at
        ) VALUES %s
        ON CONFLICT (series_id, period_date)
        DO UPDATE SET
            time_sk = EXCLUDED.time_sk,
            geo_sk = EXCLUDED.geo_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            program = EXCLUDED.program,
            geo_level = EXCLUDED.geo_level,
            geo_id = EXCLUDED.geo_id,
            state_fips = EXCLUDED.state_fips,
            county_fips = EXCLUDED.county_fips,
            value = EXCLUDED.value,
            year = EXCLUDED.year,
            period = EXCLUDED.period,
            period_name = EXCLUDED.period_name,
            measure_code = EXCLUDED.measure_code,
            measure_name = EXCLUDED.measure_name,
            seasonal_adjustment = EXCLUDED.seasonal_adjustment,
            source_system = EXCLUDED.source_system,
            load_batch_id = EXCLUDED.load_batch_id,
            ingested_at = EXCLUDED.ingested_at;
    """

    try:
        with hook.get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, insert_sql, records, page_size=5000)
            conn.commit()
    except Exception:
        logger.exception("Failed to upsert BLS silver rows")
        raise

    return len(records)


def _transform_rows_to_silver_df(hook: PostgresHook, rows: list[tuple], metrics: TransformMetrics | None = None) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(
        rows,
        orient="row",
        schema=[
            "series_id",
            "program",
            "year",
            "period",
            "period_name",
            "value",
            "measure_code",
            "seasonal_adjustment",
            "title",
        ],
    )

    years = df.get_column("year").to_list()
    periods = df.get_column("period").to_list()
    series_ids = df.get_column("series_id").to_list()
    program = str(df.get_column("program")[0]) if df.height else ""

    period_results = [parse_bls_period_to_date(int(y), p) for y, p in zip(years, periods)]
    period_date = [p[0] for p in period_results]
    duration_start = [p[1] for p in period_results]
    duration_end = [p[2] for p in period_results]

    geo_results = [parse_bls_geography(sid, program) for sid in series_ids]
    geo_level = [g["geo_level"] for g in geo_results]
    geo_id = [g["geo_id"] for g in geo_results]
    state_fips = [g["state_fips"] for g in geo_results]
    county_fips = [g["county_fips"] for g in geo_results]

    if program.lower() == "la":
        measure_code = [sid[-2:] if (sid and sid[-2:].isdigit()) else None for sid in series_ids]
    else:
        measure_fallback = df.get_column("measure_code").to_list()
        measure_code = [
            _extract_measure_code(sid, program, fb)
            for sid, fb in zip(series_ids, measure_fallback)
        ]

    # Derive measure_name from LAUS measure-code lookup or bls_series title
    if program.lower() == "la":
        measure_name = [
            LAUS_MEASURE_META.get(mc, {}).get("name") if mc else None
            for mc in measure_code
        ]
    else:
        titles = df.get_column("title").to_list()
        measure_name = [t if t else None for t in titles]

    df = df.with_columns(
        [
            pl.Series("period_date", period_date),
            pl.Series("duration_start", duration_start),
            pl.Series("duration_end", duration_end),
            pl.Series("geo_level", geo_level),
            pl.Series("geo_id", geo_id),
            pl.Series("state_fips", state_fips),
            pl.Series("county_fips", county_fips),
            pl.Series("measure_code", measure_code),
            pl.Series("measure_name", measure_name),
        ]
    )

    min_date = min(duration_start)
    max_date = max(duration_start)

    time_df = _load_time_dim(hook, min_date, max_date)
    unique_geos = df.select(["geo_level", "geo_id"]).unique()
    geo_df = _load_geo_dim_for_list(hook, unique_geos)

    df = df.join(time_df, left_on="duration_start", right_on="date_key", how="left")
    df = df.join(geo_df, on=["geo_level", "geo_id"], how="left")

    df_before_filter = df.clone()
    missing_time = df.filter(pl.col("time_sk").is_null()).height
    if missing_time:
        logger.warning(
            "Dropped %s BLS rows with missing time_sk. Ensure silver_ref.dim_time covers %s..%s.",
            missing_time,
            min_date,
            max_date,
        )
        if metrics:
            metrics.time_dim_misses = missing_time
            metrics.time_dim_hits = df_before_filter.height - missing_time
            metrics.rows_missing_time = missing_time

    missing_geo = df.filter(pl.col("geo_sk").is_null()).height
    if missing_geo:
        logger.warning(
            "Dropped %s BLS rows with missing geo_sk. Ensure silver_ref.dim_geo is synced.",
            missing_geo,
        )
        if metrics:
            metrics.geo_dim_misses = missing_geo
            metrics.geo_dim_hits = df_before_filter.height - missing_geo
            metrics.rows_missing_geo = missing_geo

    df = df.filter(pl.col("time_sk").is_not_null() & pl.col("geo_sk").is_not_null())
    if df.is_empty():
        return pl.DataFrame()

    initial_rows = df.height
    df = df.unique(subset=["series_id", "period_date"], keep="last")
    if initial_rows > df.height:
        dedup_count = initial_rows - df.height
        logger.warning(
            "Deduplicated %s duplicate BLS rows",
            dedup_count,
        )
        if metrics:
            metrics.rows_deduplicated = dedup_count

    # Collect null counts
    if metrics:
        for col in ["value", "series_id", "measure_code", "seasonal_adjustment"]:
            if col in df.columns:
                null_count = df.filter(pl.col(col).is_null()).height
                if null_count > 0:
                    metrics.null_counts[col] = null_count

    return df


def transform_bls_to_silver(program: str) -> int:
    """
    Transform ALL BLS raw data to silver layer for specified program.
    Processes entire raw_bls.bls_long table for this program.
    """
    hook = _get_hook()
    metrics = TransformMetrics(dataset_name=f"BLS_{program.upper()}")

    total_rows = _get_program_row_count(hook, program)
    if total_rows == 0:
        logger.info("No BLS rows found for program=%s", program)
        return 0

    load_batch_id = uuid.uuid4()
    ingested_at = datetime.now(timezone.utc)

    years: list[int] | None = None
    if total_rows >= LARGE_PROGRAM_ROW_THRESHOLD:
        years = _get_program_years(hook, program)
        logger.info(
            "Program=%s has %s raw rows; processing in %s year chunks",
            program,
            total_rows,
            len(years),
        )
        # Pre-transform diagnostics
        for y in years:
            sql = "SELECT COUNT(*) FROM raw_bls.bls_long WHERE program = %s AND year = %s;"
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, (program, y))
                row = cur.fetchone()
                metrics.raw_rows_by_program[f"{program}_{y}"] = int(row[0]) if row else 0
        metrics.log_pre_transform()

    upserted_total = 0

    if years:
        for y in years:
            rows = _fetch_raw_rows(hook, program, year=y)
            if not rows:
                continue
            
            metrics.log_chunk_start(f"{program}_year={y}", len(rows))
            
            df_silver = _transform_rows_to_silver_df(hook, rows, metrics)
            if not df_silver.is_empty():
                metrics.chunk_output_rows = df_silver.height
                metrics.log_chunk_complete(f"{program}_year={y}")
                
                upsert_start = datetime.now(timezone.utc)
                upserted = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
                upsert_duration = (datetime.now(timezone.utc) - upsert_start).total_seconds()
                
                metrics.log_upsert_complete(upserted, upsert_duration)
                upserted_total += upserted
                metrics.total_processed += len(rows)
                metrics.total_inserted += upserted
    else:
        rows = _fetch_raw_rows(hook, program)
        if rows:
            metrics.log_chunk_start(program, len(rows))
            df_silver = _transform_rows_to_silver_df(hook, rows, metrics)
            if not df_silver.is_empty():
                metrics.chunk_output_rows = df_silver.height
                metrics.log_chunk_complete(program)
                
                upsert_start = datetime.now(timezone.utc)
                upserted_total = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
                upsert_duration = (datetime.now(timezone.utc) - upsert_start).total_seconds()
                
                metrics.log_upsert_complete(upserted_total, upsert_duration)
                metrics.total_processed = len(rows)
                metrics.total_inserted = upserted_total

    metrics.log_transform_summary()
    logger.info("Upserted %s BLS silver rows for program=%s", upserted_total, program)
    return upserted_total
