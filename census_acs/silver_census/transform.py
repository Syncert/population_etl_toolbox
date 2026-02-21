from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, date

import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from census_acs.config import CONFIG as RAW_CONFIG
from .geography_mapper import map_census_geography
from .time_utils import compute_acs_duration

logger = logging.getLogger(__name__)

CENSUS_DATA_DOC = "https://www.census.gov/data/developers/data-sets.html"


LARGE_DATASET_ROW_THRESHOLD = 500_000


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
    sql += " ORDER BY geo_level, state_fips, county_fips, table_id, variable_name;"

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def _transform_rows_to_silver_df(hook: PostgresHook, rows: list[tuple]) -> pl.DataFrame:
    """Transform raw ACS rows to silver fact DataFrame."""
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
        pl.col("variable_name").map_elements(lambda x: x[:-1], return_dtype=pl.Utf8).alias("variable_code"),
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

    geo_ids = [
        map_census_geography(r["geo_level"], r["state_fips"], r["county_fips"])
        for r in grouped.iter_rows(named=True)
    ]

    grouped = grouped.with_columns([
        pl.Series("geo_id", geo_ids),
    ])

    meta_df = _load_variable_metadata(hook)
    if not meta_df.is_empty():
        meta_df = meta_df.filter(pl.col("variable_name").str.ends_with("E"))
        meta_df = meta_df.with_columns([
            pl.col("variable_name").map_elements(lambda x: x[:-1], return_dtype=pl.Utf8).alias("variable_code"),
        ])
        meta_df = meta_df.select([
            "dataset",
            pl.col("year").alias("estimate_year"),
            "variable_code",
            "variable_label",
            "variable_concept",
            "universe",
        ])
        grouped = grouped.join(
            meta_df,
            on=["dataset", "estimate_year", "variable_code"],
            how="left",
        )
    else:
        grouped = grouped.with_columns([
            pl.lit(None, dtype=pl.Utf8).alias("variable_label"),
            pl.lit(None, dtype=pl.Utf8).alias("variable_concept"),
            pl.lit(None, dtype=pl.Utf8).alias("universe"),
        ])

    durations = [
        compute_acs_duration(r["dataset"], int(r["estimate_year"]))
        for r in grouped.iter_rows(named=True)
    ]
    duration_start = [d[0] for d in durations]
    duration_end = [d[1] for d in durations]

    grouped = grouped.with_columns([
        pl.Series("duration_start", duration_start),
        pl.Series("duration_end", duration_end),
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

    min_date = min(duration_start)
    max_date = max(duration_start)
    time_df = _load_time_dim(hook, min_date, max_date)
    
    # Get unique geo combinations from data, then load only those geographies
    unique_geos = grouped.select(["geo_level", "geo_id"]).unique()
    geo_df = _load_geo_dim_for_list(hook, unique_geos)

    grouped = grouped.join(time_df, left_on="duration_start", right_on="date_key", how="left")
    grouped = grouped.join(geo_df, on=["geo_level", "geo_id"], how="left")

    missing_time = grouped.filter(pl.col("time_sk").is_null()).height
    if missing_time:
        logger.warning(
            "Dropped %s Census rows with missing time_sk. Ensure silver_ref.dim_time covers %s..%s.",
            missing_time,
            min_date,
            max_date,
        )

    missing_geo_rows = grouped.filter(pl.col("geo_sk").is_null())
    missing_geo = missing_geo_rows.height
    if missing_geo:
        missing_geo_ids = missing_geo_rows.select([
            "geo_level",
            "geo_id",
            "state_fips",
            "county_fips",
        ]).unique()
        by_geo_level_df = (
            missing_geo_rows
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
            missing_geo,
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
        logger.warning(
            "Deduplicated %s duplicate Census rows",
            initial_rows - grouped.height,
        )

    return grouped


def _upsert_silver_rows(hook: PostgresHook, df: pl.DataFrame, load_batch_id: uuid.UUID, ingested_at: datetime) -> int:
    """Upsert Census silver rows to fact table using efficient TEMP table strategy."""
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
                r["estimate_year"],
                r["dataset"],
                r["table_id"],
                r["variable_code"],
                r["geo_level"],
                r["geo_id"],
                r["state_fips"],
                r["county_fips"],
                r["estimate_value"],
                r["margin_of_error"],
                r["margin_of_error_pct"],
                r["variable_label"],
                r["variable_concept"],
                r["universe"],
                "CENSUS_ACS",
                load_batch_id,
                ingested_at,
            )
        )

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

    total_rows = _get_dataset_row_count(hook)
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

    upserted_total = 0

    if years:
        for y in years:
            rows = _fetch_raw_rows(hook, year=y)
            if not rows:
                continue
            df_silver = _transform_rows_to_silver_df(hook, rows)
            upserted = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)
            upserted_total += upserted
            logger.info("Upserted %s Census silver rows for year=%s", upserted, y)
    else:
        rows = _fetch_raw_rows(hook)
        df_silver = _transform_rows_to_silver_df(hook, rows)
        upserted_total = _upsert_silver_rows(hook, df_silver, load_batch_id, ingested_at)

    logger.info("Upserted %s Census silver rows total", upserted_total)
    return upserted_total
