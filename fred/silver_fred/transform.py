from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, date

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from fred.config import CONFIG as RAW_CONFIG
from .time_utils import compute_fred_duration

logger = logging.getLogger(__name__)

FRED_OBS_DOC = "https://fred.stlouisfed.org/docs/api/fred/series_observations.html"


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

    return pl.DataFrame(rows, schema=["time_sk", "date_key"]) if rows else pl.DataFrame(
        schema=["time_sk", "date_key"]
    )


def transform_fred_to_silver(domain: str) -> int:
    """
    Transform ALL FRED raw data to silver layer for specified domain.
    Processes entire raw_fred.fred_long table for this domain.

    Reference: https://fred.stlouisfed.org/docs/api/fred/series_observations.html
    """
    hook = _get_hook()

    sql = """
        SELECT
            fl.series_id,
            fl.obs_date,
            fl.value,
            fl.is_missing,
            fl.domain,
            fs.title AS series_title,
            fs.units AS unit_of_measure,
            fs.frequency,
            fs.seasonal_adjustment
        FROM raw_fred.fred_long fl
        LEFT JOIN raw_fred.fred_series fs ON fl.series_id = fs.series_id
        WHERE fl.domain = %s
          AND fl.is_missing = FALSE
        ORDER BY fl.series_id, fl.obs_date;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (domain,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No FRED rows found for domain=%s", domain)
        return 0

    df = pl.DataFrame(
        rows,
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

    df = df.with_columns([
        pl.Series("duration_start", duration_start),
        pl.Series("duration_end", duration_end),
    ])

    min_date = min(duration_start)
    max_date = max(duration_start)
    time_df = _load_time_dim(hook, min_date, max_date)

    df = df.join(time_df, left_on="duration_start", right_on="date_key", how="left")

    missing_time = df.filter(pl.col("time_sk").is_null()).height
    if missing_time:
        logger.warning(
            "Dropped %s FRED rows with missing time_sk. Ensure silver_ref.dim_time covers %s..%s.",
            missing_time,
            min_date,
            max_date,
        )

    df = df.filter(pl.col("time_sk").is_not_null())
    if df.is_empty():
        return 0

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
            ingested_at = EXCLUDED.ingested_at;
    """

    try:
        with hook.get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, insert_sql, records, page_size=1000)
            conn.commit()
    except Exception:
        logger.exception("Failed to upsert FRED silver rows for domain=%s", domain)
        raise

    logger.info("Upserted %s FRED silver rows for domain=%s", len(records), domain)
    return len(records)
