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


def transform_census_to_silver() -> int:
    """
    Transform ALL Census ACS raw data to silver layer.
    Processes entire raw_census.acs_long table.
    """
    hook = _get_hook()

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
        ORDER BY geo_level, state_fips, county_fips, table_id, variable_name;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        logger.info("No Census ACS rows found for silver transform")
        return 0

    df = pl.DataFrame(
        rows,
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
        pl.col("variable_name").str.slice(0, -1).alias("variable_code"),
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
        pl.max(pl.when(pl.col("measure_type") == "E").then(pl.col("value"))).alias("estimate_value"),
        pl.max(pl.when(pl.col("measure_type") == "M").then(pl.col("value"))).alias("margin_of_error"),
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
            pl.col("variable_name").str.slice(0, -1).alias("variable_code"),
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
        pl.when(
            pl.col("estimate_value").is_not_null()
            & (pl.col("estimate_value") != 0)
            & pl.col("margin_of_error").is_not_null()
        )
        .then((pl.col("margin_of_error") / pl.col("estimate_value")) * 100)
        .otherwise(None)
        .alias("margin_of_error_pct")
    ])

    min_date = min(duration_start)
    max_date = max(duration_start)
    time_df = _load_time_dim(hook, min_date, max_date)
    geo_df = _load_geo_dim(hook)

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

    missing_geo = grouped.filter(pl.col("geo_sk").is_null()).height
    if missing_geo:
        logger.warning(
            "Dropped %s Census rows with missing geo_sk. Ensure silver_ref.dim_geo is synced.",
            missing_geo,
        )

    grouped = grouped.filter(pl.col("time_sk").is_not_null() & pl.col("geo_sk").is_not_null())
    if grouped.is_empty():
        return 0

    # Deduplicate by unique constraint columns - keep last record
    # This handles cases where raw data has duplicates
    initial_rows = len(grouped)
    grouped = grouped.unique(
        subset=["dataset", "table_id", "variable_code", "geo_id", "estimate_year"],
        keep="last"
    )
    deduped_rows = len(grouped)
    if initial_rows > deduped_rows:
        logger.warning(
            "Deduplicated %s duplicate Census rows",
            initial_rows - deduped_rows,
        )

    load_batch_id = uuid.uuid4()
    ingested_at = datetime.now(timezone.utc)

    records = []
    for r in grouped.iter_rows(named=True):
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

    insert_sql = """
        INSERT INTO silver_census.fact_demographics (
            time_sk, geo_sk, duration_start, duration_end,
            estimate_year, dataset, table_id, variable_code,
            geo_level, geo_id, state_fips, county_fips,
            estimate_value, margin_of_error, margin_of_error_pct,
            variable_label, variable_concept, universe,
            source_system, load_batch_id, ingested_at
        ) VALUES %s
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
            execute_values(cur, insert_sql, records, page_size=1000)
            conn.commit()
    except Exception:
        logger.exception("Failed to upsert Census silver rows")
        raise

    logger.info("Upserted %s Census silver rows", len(records))
    return len(records)
