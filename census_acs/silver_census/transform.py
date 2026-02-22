from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, date
from dataclasses import dataclass, field

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from census_acs.config import CONFIG as RAW_CONFIG

logger = logging.getLogger(__name__)

CENSUS_DATA_DOC = "https://www.census.gov/data/developers/data-sets.html"


LARGE_DATASET_ROW_THRESHOLD = 500_000


@dataclass
class TransformMetrics:
    """Track and log Census silver transform metrics."""
    dataset_name: str
    
    # Pre-transform
    raw_rows_by_year: dict[int, int] = field(default_factory=dict)
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
    upsert_updated: int = 0
    upsert_total: int = 0
    
    # Post-transform
    total_processed: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    errors_encountered: list[str] = field(default_factory=list)
    
    def log_pre_transform(self) -> None:
        """Log pre-transform diagnostics."""
        if self.raw_rows_by_year:
            years_summary = "; ".join(
                f"year={y}:{count:,} rows"
                for y, count in sorted(self.raw_rows_by_year.items())
            )
            logger.info(
                "[%s PRE-TRANSFORM] Raw row count by year: %s (total: %s)",
                self.dataset_name,
                years_summary,
                sum(self.raw_rows_by_year.values()),
            )
        
        if self.schema_issues:
            logger.warning(
                "[%s PRE-TRANSFORM] Schema validation issues: %s",
                self.dataset_name,
                "; ".join(self.schema_issues),
            )
    
    def log_chunk_start(self, year: int, input_rows: int) -> None:
        """Log start of chunk processing."""
        self.chunk_input_rows = input_rows
        logger.info(
            "[%s CHUNK] Processing year=%s with %s raw rows",
            self.dataset_name,
            year,
            input_rows,
        )
    
    def log_chunk_complete(self, year: int) -> None:
        """Log chunk processing results."""
        pct_output = (
            (self.chunk_output_rows / self.chunk_input_rows * 100)
            if self.chunk_input_rows > 0
            else 0
        )
        logger.info(
            "[%s CHUNK] Year=%s: %s input → %s output (%.1f%% retained)",
            self.dataset_name,
            year,
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
        
        if self.time_dim_misses or self.geo_dim_misses:
            logger.info(
                "[%s CHUNK] Dimension coverage: time_sk=%s hits/%s misses (%.1f%%), geo_sk=%s hits/%s misses (%.1f%%)",
                self.dataset_name,
                self.time_dim_hits,
                self.time_dim_misses,
                (self.time_dim_misses / (self.time_dim_hits + self.time_dim_misses) * 100)
                if (self.time_dim_hits + self.time_dim_misses) > 0 else 0,
                self.geo_dim_hits,
                self.geo_dim_misses,
                (self.geo_dim_misses / (self.geo_dim_hits + self.geo_dim_misses) * 100)
                if (self.geo_dim_hits + self.geo_dim_misses) > 0 else 0,
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
        FROM silver_ref.dim_time
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
        FROM silver_ref.dim_geo;
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
        FROM silver_ref.dim_geo g
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


def _count_unpadded_state_geo_ids(hook: PostgresHook) -> int:
    sql = """
        SELECT COUNT(*)
        FROM silver_ref.dim_geo
        WHERE geo_level = 'state'
          AND geo_id ~ '^state:[0-9]$';
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _load_variable_metadata(hook: PostgresHook) -> pl.DataFrame:
    sql = """
        SELECT dataset, year, variable_name, label, concept, predicate_type
        FROM raw_census.acs_variables;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return pl.DataFrame(
        rows,
        orient="row",
        schema=[
            "dataset",
            "year",
            "variable_name",
            "variable_label",
            "variable_concept",
            "universe",
        ],
    ) if rows else pl.DataFrame(
        schema=[
            "dataset",
            "year",
            "variable_name",
            "variable_label",
            "variable_concept",
            "universe",
        ]
    )


def _get_dataset_row_count(hook: PostgresHook) -> int:
    sql = "SELECT COUNT(*) FROM raw_census.acs_long;"
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _get_dataset_years(hook: PostgresHook) -> list[int]:
    sql = "SELECT DISTINCT year FROM raw_census.acs_long ORDER BY year;"
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def _fetch_raw_rows(hook: PostgresHook, year: int | None = None) -> list[tuple]:
    sql = """
        SELECT
            dataset,
            year,
            geo_level,
            state_fips,
            county_fips,
            table_id,
            variable_name,
            measure_type,
            value
        FROM raw_census.acs_long
    """
    params: list[object] = []
    if year is not None:
        sql += " WHERE year = %s"
        params.append(int(year))
    sql += ";"

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def _transform_rows_to_silver_df(
    hook: PostgresHook,
    rows: list[tuple],
    metrics: TransformMetrics | None = None,
    meta_df: pl.DataFrame | None = None,
    time_df: pl.DataFrame | None = None,
    geo_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Transform raw ACS rows to silver fact DataFrame.

    Parameters
    ----------
    meta_df, time_df, geo_df : optional pre-loaded dimension DataFrames.
        When supplied the function skips per-chunk DB round-trips.
    """
    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(
        rows,
        orient="row",
        schema=[
            "dataset",
            "estimate_year",
            "geo_level",
            "state_fips",
            "county_fips",
            "table_id",
            "variable_name",
            "measure_type",
            "value",
        ],
    )

    df = df.with_columns([
        pl.col("variable_name").str.head(-1).alias("variable_code"),
    ])

    grouped = df.group_by([
        "dataset",
        "estimate_year",
        "geo_level",
        "state_fips",
        "county_fips",
        "table_id",
        "variable_code",
    ]).agg([
        pl.when(pl.col("measure_type") == "E").then(pl.col("value")).max().alias("estimate_value"),
        pl.when(pl.col("measure_type") == "M").then(pl.col("value")).max().alias("margin_of_error"),
    ])

    grouped = grouped.with_columns([
        pl.when(pl.col("geo_level") == "us")
        .then(pl.lit("us:1"))
        .when(pl.col("geo_level") == "state")
        .then(pl.lit("state:") + pl.col("state_fips"))
        .when(pl.col("geo_level") == "county")
        .then(pl.lit("state:") + pl.col("state_fips") + pl.lit("|county:") + pl.col("county_fips"))
        .otherwise(pl.lit(None))
        .alias("geo_id"),
    ])

    if meta_df is None:
        meta_df = _load_variable_metadata(hook)
    if not meta_df.is_empty():
        _meta = meta_df.filter(pl.col("variable_name").str.ends_with("E"))
        _meta = _meta.with_columns([
            pl.col("variable_name").str.head(-1).alias("variable_code"),
        ])
        _meta = _meta.select([
            "dataset",
            pl.col("year").alias("estimate_year"),
            "variable_code",
            "variable_label",
            "variable_concept",
            "universe",
        ])
        grouped = grouped.join(
            _meta,
            on=["dataset", "estimate_year", "variable_code"],
            how="left",
        )
    else:
        grouped = grouped.with_columns([
            pl.lit(None, dtype=pl.Utf8).alias("variable_label"),
            pl.lit(None, dtype=pl.Utf8).alias("variable_concept"),
            pl.lit(None, dtype=pl.Utf8).alias("universe"),
        ])

    grouped = grouped.with_columns([
        pl.when(pl.col("dataset").str.to_lowercase() == "acs5")
        .then(
            pl.concat_str([
                (pl.col("estimate_year").cast(pl.Int32) - 4).cast(pl.Utf8),
                pl.lit("-01-01"),
            ]).str.to_date("%Y-%m-%d")
        )
        .otherwise(
            pl.concat_str([
                pl.col("estimate_year").cast(pl.Utf8),
                pl.lit("-01-01"),
            ]).str.to_date("%Y-%m-%d")
        )
        .alias("duration_start"),
        pl.concat_str([
            pl.col("estimate_year").cast(pl.Utf8),
            pl.lit("-12-31"),
        ]).str.to_date("%Y-%m-%d")
        .alias("duration_end"),
    ])

    grouped = grouped.with_columns([
        (
            pl.col("margin_of_error")
            /
            pl.when(
                pl.col("estimate_value").is_null() | (pl.col("estimate_value") == 0)
            )
            .then(None)
            .otherwise(pl.col("estimate_value"))
            * 100
        ).alias("margin_of_error_pct")
    ])

    if time_df is None:
        min_date = grouped["duration_start"].min()
        max_date = grouped["duration_start"].max()
        time_df = _load_time_dim(hook, min_date, max_date)
    if geo_df is None:
        unique_geos = grouped.select(["geo_level", "geo_id"]).unique()
        geo_df = _load_geo_dim_for_list(hook, unique_geos)

    pre_join_height = grouped.height
    grouped = grouped.join(time_df, left_on="duration_start", right_on="date_key", how="left")
    grouped = grouped.join(geo_df, on=["geo_level", "geo_id"], how="left")

    missing_time_rows = grouped.filter(pl.col("time_sk").is_null()).height
    if missing_time_rows:
        _min_d = grouped["duration_start"].min()
        _max_d = grouped["duration_start"].max()
        logger.warning(
            "Dropped %s Census rows with missing time_sk. Ensure silver_ref.dim_time covers %s..%s.",
            missing_time_rows,
            _min_d,
            _max_d,
        )
        if metrics:
            metrics.time_dim_misses = missing_time_rows
            metrics.time_dim_hits = pre_join_height - missing_time_rows

    missing_geo_rows = grouped.filter(pl.col("geo_sk").is_null()).height
    if missing_geo_rows:
        missing_geo_ids = grouped.filter(pl.col("geo_sk").is_null()).select([
            "geo_level",
            "geo_id",
            "state_fips",
            "county_fips",
        ]).unique()
        by_geo_level_df = (
            grouped.filter(pl.col("geo_sk").is_null())
            .group_by("geo_level")
            .len()
            .sort("geo_level")
        )
        by_geo_level = ", ".join(
            f"{r['geo_level']}={r['len']}"
            for r in by_geo_level_df.iter_rows(named=True)
        )
        missing_geo_examples_df = (
            missing_geo_ids
            .sort(["geo_level", "geo_id"])
            .head(25)
        )
        missing_geo_examples = "; ".join(
            f"{r['geo_level']}:{r['geo_id']}"
            for r in missing_geo_examples_df.iter_rows(named=True)
        )

        logger.warning(
            "Dropped %s Census rows with missing geo_sk (distinct_missing_geo_ids=%s; by_geo_level_rows={%s}). Ensure silver_ref.dim_geo is synced.",
            missing_geo_rows,
            missing_geo_ids.height,
            by_geo_level,
        )
        logger.warning("Missing geo_id examples (max 25): %s", missing_geo_examples)

        unpadded_states = _count_unpadded_state_geo_ids(hook)
        if unpadded_states:
            logger.warning(
                "silver_ref.dim_geo has %s unpadded state geo_id values (e.g., state:1). This can break joins against Census geo_id format state:01.",
                unpadded_states,
            )
        
        if metrics:
            metrics.geo_dim_misses = missing_geo_rows
            metrics.rows_missing_geo = missing_geo_rows
            metrics.geo_dim_hits = pre_join_height - missing_geo_rows

    grouped = grouped.filter(pl.col("time_sk").is_not_null() & pl.col("geo_sk").is_not_null())
    if grouped.is_empty():
        return pl.DataFrame()

    # Deduplicate by unique constraint columns - keep last record
    initial_rows = grouped.height
    grouped = grouped.unique(
        subset=["dataset", "table_id", "variable_code", "geo_id", "estimate_year"],
        keep="last"
    )
    if initial_rows > grouped.height:
        dedup_count = initial_rows - grouped.height
        logger.warning(
            "Deduplicated %s duplicate Census rows",
            dedup_count,
        )
        if metrics:
            metrics.rows_deduplicated = dedup_count

    # Collect null counts
    if metrics:
        null_check_cols = [c for c in ["estimate_value", "margin_of_error", "variable_label", "variable_concept", "universe"] if c in grouped.columns]
        if null_check_cols:
            null_row = grouped.select([pl.col(c).null_count().alias(c) for c in null_check_cols]).row(0, named=True)
            for col, count in null_row.items():
                if count > 0:
                    metrics.null_counts[col] = count

    return grouped


def _upsert_silver_rows(hook: PostgresHook, df: pl.DataFrame, load_batch_id: uuid.UUID, ingested_at: datetime) -> int:
    """Upsert Census silver rows to fact table using efficient TEMP table strategy."""
    if df.is_empty():
        return 0

    upsert_cols = [
        "time_sk", "geo_sk", "duration_start", "duration_end",
        "estimate_year", "dataset", "table_id", "variable_code",
        "geo_level", "geo_id", "state_fips", "county_fips",
        "estimate_value", "margin_of_error", "margin_of_error_pct",
        "variable_label", "variable_concept", "universe",
    ]
    suffix = ("CENSUS_ACS", load_batch_id, ingested_at)
    records = [row + suffix for row in df.select(upsert_cols).rows()]

    # Use TEMP table strategy for better performance on large upserts
    create_temp_sql = """
        CREATE TEMP TABLE temp_census_upsert (
            time_sk INTEGER,
            geo_sk INTEGER,
            duration_start DATE,
            duration_end DATE,
            estimate_year INTEGER,
            dataset VARCHAR(50),
            table_id VARCHAR(50),
            variable_code VARCHAR(100),
            geo_level VARCHAR(50),
            geo_id VARCHAR(255),
            state_fips VARCHAR(2),
            county_fips VARCHAR(3),
            estimate_value NUMERIC,
            margin_of_error NUMERIC,
            margin_of_error_pct NUMERIC,
            variable_label TEXT,
            variable_concept TEXT,
            universe TEXT,
            source_system VARCHAR(50),
            load_batch_id UUID,
            ingested_at TIMESTAMPTZ
        ) ON COMMIT DROP;
    """

    insert_temp_sql = """
        INSERT INTO temp_census_upsert VALUES %s;
    """

    merge_sql = """
        INSERT INTO silver_census.fact_demographics (
            time_sk,
            geo_sk,
            duration_start,
            duration_end,
            estimate_year,
            dataset,
            table_id,
            variable_code,
            geo_level,
            geo_id,
            state_fips,
            county_fips,
            estimate_value,
            margin_of_error,
            margin_of_error_pct,
            variable_label,
            variable_concept,
            universe,
            source_system,
            load_batch_id,
            ingested_at
        )
        SELECT 
            time_sk,
            geo_sk,
            duration_start,
            duration_end,
            estimate_year,
            dataset,
            table_id,
            variable_code,
            geo_level,
            geo_id,
            state_fips,
            county_fips,
            estimate_value,
            margin_of_error,
            margin_of_error_pct,
            variable_label,
            variable_concept,
            universe,
            source_system,
            load_batch_id,
            ingested_at
        FROM temp_census_upsert
        ON CONFLICT (dataset, table_id, variable_code, geo_id, estimate_year)
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
            ingested_at = EXCLUDED.ingested_at;
    """

    try:
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(create_temp_sql)
            execute_values(cur, insert_temp_sql, records, page_size=10000)
            cur.execute(merge_sql)
            conn.commit()
    except Exception:
        logger.exception("Failed to upsert Census silver rows")
        raise

    return len(records)


def transform_census_to_silver() -> int:
    """
    Transform ALL Census ACS raw data to silver layer.
    Processes entire raw_census.acs_long table in memory-safe year chunks.
    """
    hook = _get_hook()
    metrics = TransformMetrics(dataset_name="CENSUS_ACS")

    logger.info("[CENSUS_ACS] Starting silver transform — counting raw rows...")
    total_rows = _get_dataset_row_count(hook)
    logger.info("[CENSUS_ACS] Raw row count: %s", f"{total_rows:,}")
    if total_rows == 0:
        logger.info("No Census ACS rows found for silver transform")
        return 0

    load_batch_id = uuid.uuid4()
    ingested_at = datetime.now(timezone.utc)

    years: list[int] | None = None
    if total_rows >= LARGE_DATASET_ROW_THRESHOLD:
        years = _get_dataset_years(hook)
        logger.info(
            "Census ACS dataset has %s raw rows; processing in %s year chunks",
            total_rows,
            len(years),
        )
        # Pre-transform diagnostics — single query instead of per-year
        logger.info("[CENSUS_ACS] Gathering per-year row counts...")
        sql = "SELECT year, COUNT(*) FROM raw_census.acs_long GROUP BY year ORDER BY year;"
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                metrics.raw_rows_by_year[int(row[0])] = int(row[1])
        metrics.log_pre_transform()

    upserted_total = 0

    # Pre-load dimensions once to avoid repeated DB round-trips per year chunk
    logger.info("[CENSUS_ACS] Pre-loading variable metadata...")
    meta_df = _load_variable_metadata(hook)
    logger.info("[CENSUS_ACS] Loaded %s variable metadata rows", f"{meta_df.height:,}")

    logger.info("[CENSUS_ACS] Pre-loading geo dimension...")
    geo_df = _load_geo_dim(hook)
    logger.info("[CENSUS_ACS] Loaded %s geo dimension rows", f"{geo_df.height:,}")

    # Compute full time range across all possible ACS windows
    if years:
        earliest_year = min(years) - 4  # acs5 looks back 4 years
    else:
        earliest_year = 2000
    latest_year = max(years) if years else 2030
    logger.info("[CENSUS_ACS] Pre-loading time dimension (%s..%s)...", earliest_year, latest_year)
    time_df = _load_time_dim(hook, date(earliest_year, 1, 1), date(latest_year, 12, 31))
    logger.info("[CENSUS_ACS] Loaded %s time dimension rows", f"{time_df.height:,}")

    if years:
        for y in years:
            rows = _fetch_raw_rows(hook, year=y)
            if not rows:
                continue
            
            metrics.log_chunk_start(y, len(rows))
            
            df_silver = _transform_rows_to_silver_df(hook, rows, metrics, meta_df=meta_df, time_df=time_df, geo_df=geo_df)
            if not df_silver.is_empty():
                metrics.chunk_output_rows = df_silver.height
                metrics.log_chunk_complete(y)
                
                upsert_start = datetime.now(timezone.utc)
                upserted = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
                upsert_duration = (datetime.now(timezone.utc) - upsert_start).total_seconds()
                
                metrics.log_upsert_complete(upserted, upsert_duration)
                upserted_total += upserted
                metrics.total_processed += len(rows)
                metrics.total_inserted += upserted
    else:
        rows = _fetch_raw_rows(hook)
        if rows:
            metrics.log_chunk_start(None, len(rows))
            df_silver = _transform_rows_to_silver_df(hook, rows, metrics, meta_df=meta_df, time_df=time_df, geo_df=geo_df)
            if not df_silver.is_empty():
                metrics.chunk_output_rows = df_silver.height
                metrics.log_chunk_complete(None)
                
                upsert_start = datetime.now(timezone.utc)
                upserted_total = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
                upsert_duration = (datetime.now(timezone.utc) - upsert_start).total_seconds()
                
                metrics.log_upsert_complete(upserted_total, upsert_duration)
                metrics.total_processed = len(rows)
                metrics.total_inserted = upserted_total

    metrics.log_transform_summary()
    logger.info("Upserted %s Census silver rows total", upserted_total)
    return upserted_total
