from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, date
from dataclasses import dataclass, field

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from data_ingestion_toolbox.fred.config import CONFIG as RAW_CONFIG
from .time_utils import compute_fred_duration

logger = logging.getLogger(__name__)

FRED_OBS_DOC = "https://fred.stlouisfed.org/docs/api/fred/series_observations.html"


@dataclass
class TransformMetrics:
    """Track and log FRED silver transform metrics."""

    dataset_name: str

    # Pre-transform
    raw_rows_by_domain: dict[str, int] = field(default_factory=dict)
    schema_issues: list[str] = field(default_factory=list)

    # Per-chunk
    chunk_input_rows: int = 0
    chunk_output_rows: int = 0
    rows_missing_time: int = 0
    rows_deduplicated: int = 0
    time_dim_hits: int = 0
    time_dim_misses: int = 0
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
        if self.raw_rows_by_domain:
            domains_summary = "; ".join(
                f"domain={d}:{count:,} rows"
                for d, count in sorted(self.raw_rows_by_domain.items())
            )
            logger.info(
                "[%s PRE-TRANSFORM] Raw row count by domain: %s (total: %s)",
                self.dataset_name,
                domains_summary,
                sum(self.raw_rows_by_domain.values()),
            )

        if self.schema_issues:
            logger.warning(
                "[%s PRE-TRANSFORM] Schema validation issues: %s",
                self.dataset_name,
                "; ".join(self.schema_issues),
            )

    def log_chunk_start(self, domain: str, input_rows: int) -> None:
        """Log start of chunk processing."""
        self.chunk_input_rows = input_rows
        logger.info(
            "[%s CHUNK] Processing domain=%s with %s raw rows",
            self.dataset_name,
            domain,
            input_rows,
        )

    def log_chunk_complete(self, domain: str) -> None:
        """Log chunk processing results."""
        pct_output = (
            (self.chunk_output_rows / self.chunk_input_rows * 100)
            if self.chunk_input_rows > 0
            else 0
        )
        logger.info(
            "[%s CHUNK] Domain=%s: %s input → %s output (%.1f%% retained)",
            self.dataset_name,
            domain,
            self.chunk_input_rows,
            self.chunk_output_rows,
            pct_output,
        )

        if self.rows_missing_time:
            logger.warning(
                "[%s CHUNK] Rows filtered: missing_time=%s",
                self.dataset_name,
                self.rows_missing_time,
            )

        if self.rows_deduplicated:
            logger.info(
                "[%s CHUNK] Deduplicated %s rows",
                self.dataset_name,
                self.rows_deduplicated,
            )

        if self.time_dim_misses:
            logger.info(
                "[%s CHUNK] Time dimension coverage: %s hits, %s misses (%.1f%%)",
                self.dataset_name,
                self.time_dim_hits,
                self.time_dim_misses,
                (
                    self.time_dim_misses
                    / (self.time_dim_hits + self.time_dim_misses)
                    * 100
                )
                if (self.time_dim_hits + self.time_dim_misses) > 0
                else 0,
            )

        if self.null_counts:
            null_summary = "; ".join(
                f"{col}={count:,}" for col, count in sorted(self.null_counts.items())
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


def _load_time_dim(
    hook: PostgresHook, start_date: date, end_date: date
) -> pl.DataFrame:
    sql = """
        SELECT time_sk, date_key
        FROM silver_ref.dim_time
        WHERE date_key BETWEEN %s AND %s;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()

    return (
        pl.DataFrame(rows, schema=["time_sk", "date_key"])
        if rows
        else pl.DataFrame(schema=["time_sk", "date_key"])
    )


def transform_fred_to_silver(domain: str) -> int:
    """
    Transform ALL FRED raw data to silver layer for specified domain.
    Processes entire raw_fred.fred_long table for this domain.

    Reference: https://fred.stlouisfed.org/docs/api/fred/series_observations.html
    """
    hook = _get_hook()
    metrics = TransformMetrics(dataset_name=f"FRED_{domain}")

    sql = """
        WITH latest_revisions AS (
            SELECT
                series_id,
                obs_date,
                value,
                is_missing,
                domain,
                ROW_NUMBER() OVER (
                    PARTITION BY series_id, obs_date
                    ORDER BY realtime_start DESC, ingested_at DESC
                ) as rn
            FROM raw_fred.fred_long
            WHERE domain = %s
              AND is_missing = FALSE
        )
        SELECT
            lr.series_id,
            lr.obs_date,
            lr.value,
            lr.is_missing,
            lr.domain,
            fs.title AS series_title,
            fs.units AS unit_of_measure,
            fs.frequency,
            fs.seasonal_adjustment
        FROM latest_revisions lr
        LEFT JOIN raw_fred.fred_series fs ON lr.series_id = fs.series_id
        WHERE lr.rn = 1
        ORDER BY lr.series_id, lr.obs_date;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (domain,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No FRED rows found for domain=%s", domain)
        return 0

    metrics.log_chunk_start(domain, len(rows))

    df = pl.DataFrame(
        rows,
        orient="row",
        schema=[
            "series_id",
            "observation_date",
            "value",
            "is_missing",
            "domain",
            "series_title",
            "unit_of_measure",
            "frequency",
            "seasonal_adjustment",
        ],
    )

    durations = [
        compute_fred_duration(r["observation_date"], r["frequency"])
        for r in df.iter_rows(named=True)
    ]
    duration_start = [d[0] for d in durations]
    duration_end = [d[1] for d in durations]

    df = df.with_columns(
        [
            pl.Series("duration_start", duration_start),
            pl.Series("duration_end", duration_end),
        ]
    )

    min_date = min(duration_start)
    max_date = max(duration_start)
    time_df = _load_time_dim(hook, min_date, max_date)

    df_before_filter = df.clone()
    df = df.join(time_df, left_on="duration_start", right_on="date_key", how="left")

    missing_time = df.filter(pl.col("time_sk").is_null()).height
    if missing_time:
        logger.warning(
            "Dropped %s FRED rows with missing time_sk. Ensure silver_ref.dim_time covers %s..%s.",
            missing_time,
            min_date,
            max_date,
        )
        metrics.time_dim_misses = missing_time
        metrics.time_dim_hits = df_before_filter.height - missing_time
        metrics.rows_missing_time = missing_time

    df = df.filter(pl.col("time_sk").is_not_null())
    if df.is_empty():
        return 0

    # Deduplicate by (series_id, observation_date) - keep last record
    # This handles cases where raw data has duplicates
    initial_rows = len(df)
    df = df.unique(subset=["series_id", "observation_date"], keep="last")
    deduped_rows = len(df)
    if initial_rows > deduped_rows:
        dedup_count = initial_rows - deduped_rows
        logger.warning(
            "Deduplicated %s duplicate FRED rows for domain=%s",
            dedup_count,
            domain,
        )
        metrics.rows_deduplicated = dedup_count

    metrics.chunk_output_rows = len(df)
    metrics.log_chunk_complete(domain)

    load_batch_id = uuid.uuid4()
    ingested_at = datetime.now(timezone.utc)

    records = []
    for r in df.iter_rows(named=True):
        records.append(
            (
                r["time_sk"],
                r["duration_start"],
                r["duration_end"],
                r["observation_date"],
                r["series_id"],
                r["domain"],
                r["value"],
                r["is_missing"],
                r["series_title"],
                r["unit_of_measure"],
                r["frequency"],
                r["seasonal_adjustment"],
                "FRED",
                load_batch_id,
                ingested_at,
            )
        )

    insert_sql = """
        INSERT INTO silver_fred.fact_economic_indicators (
            time_sk, duration_start, duration_end,
            observation_date, series_id, domain,
            value, is_missing, series_title,
            unit_of_measure, frequency, seasonal_adjustment,
            source_system, load_batch_id, ingested_at
        ) VALUES %s
        ON CONFLICT (series_id, observation_date)
        DO UPDATE SET
            time_sk = EXCLUDED.time_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            domain = EXCLUDED.domain,
            value = EXCLUDED.value,
            is_missing = EXCLUDED.is_missing,
            series_title = EXCLUDED.series_title,
            unit_of_measure = EXCLUDED.unit_of_measure,
            frequency = EXCLUDED.frequency,
            seasonal_adjustment = EXCLUDED.seasonal_adjustment,
            source_system = EXCLUDED.source_system,
            load_batch_id = EXCLUDED.load_batch_id,
            ingested_at = EXCLUDED.ingested_at
        WHERE (
            silver_fred.fact_economic_indicators.time_sk,
            silver_fred.fact_economic_indicators.duration_start,
            silver_fred.fact_economic_indicators.duration_end,
            silver_fred.fact_economic_indicators.domain,
            silver_fred.fact_economic_indicators.value,
            silver_fred.fact_economic_indicators.is_missing,
            silver_fred.fact_economic_indicators.series_title,
            silver_fred.fact_economic_indicators.unit_of_measure,
            silver_fred.fact_economic_indicators.frequency,
            silver_fred.fact_economic_indicators.seasonal_adjustment,
            silver_fred.fact_economic_indicators.source_system
        ) IS DISTINCT FROM (
            EXCLUDED.time_sk,
            EXCLUDED.duration_start,
            EXCLUDED.duration_end,
            EXCLUDED.domain,
            EXCLUDED.value,
            EXCLUDED.is_missing,
            EXCLUDED.series_title,
            EXCLUDED.unit_of_measure,
            EXCLUDED.frequency,
            EXCLUDED.seasonal_adjustment,
            EXCLUDED.source_system
        );
    """

    try:
        upsert_start = datetime.now(timezone.utc)
        with hook.get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, insert_sql, records, page_size=1000)
            conn.commit()
        upsert_duration = (datetime.now(timezone.utc) - upsert_start).total_seconds()
        metrics.log_upsert_complete(len(records), upsert_duration)
    except Exception:
        logger.exception("Failed to upsert FRED silver rows for domain=%s", domain)
        metrics.errors_encountered.append(f"Upsert failed for domain={domain}")
        raise

    metrics.total_processed = len(rows)
    metrics.total_inserted = len(records)
    metrics.log_transform_summary()
    logger.info("Upserted %s FRED silver rows for domain=%s", len(records), domain)
    return len(records)
