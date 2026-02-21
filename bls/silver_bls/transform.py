from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, date

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from bls.config import CONFIG as RAW_CONFIG
from .geography_parser import parse_bls_geography
from .time_utils import parse_bls_period_to_date

logger = logging.getLogger(__name__)

BLS_API_DOC = "https://www.bls.gov/developers/api_signature_v2.htm"


LARGE_PROGRAM_ROW_THRESHOLD = 500_000


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


def _load_geo_dim(hook: PostgresHook) -> pl.DataFrame:
    sql = """
        SELECT geo_sk, geo_level, geo_id
        FROM silver_ref.dim_geo;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return pl.DataFrame(rows, schema=["geo_sk", "geo_level", "geo_id"]) if rows else pl.DataFrame(
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

    return pl.DataFrame(rows, schema=["geo_sk", "geo_level", "geo_id"]) if rows else pl.DataFrame(
        schema=["geo_sk", "geo_level", "geo_id"]
    )


def _extract_measure_code(series_id: str, program: str, fallback: str | None) -> str | None:
    if (program or "").lower() == "la" and series_id and series_id[-2:].isdigit():
        return series_id[-2:]
    return fallback


def _get_program_row_count(hook: PostgresHook, program: str) -> int:
    sql = "SELECT COUNT(*) FROM raw_bls.bls_long WHERE program = %s;"
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (program,))
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
            bs.seasonal AS seasonal_adjustment
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
                None,
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


def _transform_rows_to_silver_df(hook: PostgresHook, rows: list[tuple]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(
        rows,
        schema=[
            "series_id",
            "program",
            "year",
            "period",
            "period_name",
            "value",
            "measure_code",
            "seasonal_adjustment",
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
        ]
    )

    min_date = min(duration_start)
    max_date = max(duration_start)

    time_df = _load_time_dim(hook, min_date, max_date)
    unique_geos = df.select(["geo_level", "geo_id"]).unique()
    geo_df = _load_geo_dim_for_list(hook, unique_geos)

    df = df.join(time_df, left_on="duration_start", right_on="date_key", how="left")
    df = df.join(geo_df, on=["geo_level", "geo_id"], how="left")

    missing_time = df.filter(pl.col("time_sk").is_null()).height
    if missing_time:
        logger.warning(
            "Dropped %s BLS rows with missing time_sk. Ensure silver_ref.dim_time covers %s..%s.",
            missing_time,
            min_date,
            max_date,
        )

    missing_geo = df.filter(pl.col("geo_sk").is_null()).height
    if missing_geo:
        logger.warning(
            "Dropped %s BLS rows with missing geo_sk. Ensure silver_ref.dim_geo is synced.",
            missing_geo,
        )

    df = df.filter(pl.col("time_sk").is_not_null() & pl.col("geo_sk").is_not_null())
    if df.is_empty():
        return pl.DataFrame()

    initial_rows = df.height
    df = df.unique(subset=["series_id", "period_date"], keep="last")
    if initial_rows > df.height:
        logger.warning(
            "Deduplicated %s duplicate BLS rows",
            initial_rows - df.height,
        )

    return df


def transform_bls_to_silver(program: str) -> int:
    """
    Transform ALL BLS raw data to silver layer for specified program.
    Processes entire raw_bls.bls_long table for this program.
    """
    hook = _get_hook()

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

    upserted_total = 0

    if years:
        for y in years:
            rows = _fetch_raw_rows(hook, program, year=y)
            if not rows:
                continue
            df_silver = _transform_rows_to_silver_df(hook, rows)
            upserted = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
            upserted_total += upserted
            logger.info("Upserted %s BLS silver rows for program=%s year=%s", upserted, program, y)
    else:
        rows = _fetch_raw_rows(hook, program)
        df_silver = _transform_rows_to_silver_df(hook, rows)
        upserted_total = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)

    logger.info("Upserted %s BLS silver rows for program=%s", upserted_total, program)
    return upserted_total
