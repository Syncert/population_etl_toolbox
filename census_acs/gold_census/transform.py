"""
Gold analytics layer — ACS/Census subject transform.

Handles fetching ACS silver data for a given month and upserting
into the shared gold.fact_metrics table.

ACS data is annual; month_start must be January 1st to yield rows.
ACS 5-year estimates (acs5) take precedence over 1-year (acs1).
"""
from __future__ import annotations

import logging
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG
from gold.transform import (
    ensure_gold_schema,
    build_shard_list,
)

logger = logging.getLogger(__name__)


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _seed_acs_metric_catalog(cur) -> int:
    """Populate dim_metric_catalog and bridge_metric_acs_variable from dim_acs_variable."""
    cur.execute(
        """
        INSERT INTO gold.dim_metric_catalog (
            metric_code,
            metric_display_name,
            source_code,
            source_object_type,
            business_definition,
            caveats,
            valid_geo_grains,
            valid_time_grains,
            dashboard_suitability,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team,
            is_active
        )
        SELECT
            'ACS:' || v.dataset_code || ':' || v.variable_code AS metric_code,
            COALESCE(NULLIF(v.variable_label, ''), v.variable_code) AS metric_display_name,
            'CENSUS_ACS' AS source_code,
            'ACS_VARIABLE' AS source_object_type,
            CONCAT_WS(
                ' ',
                COALESCE(NULLIF(v.concept, ''), NULLIF(t.table_title, ''), 'ACS curated variable.'),
                CASE
                    WHEN NULLIF(v.universe, '') IS NOT NULL THEN 'Universe: ' || v.universe || '.'
                    ELSE NULL
                END,
                'Published from the ' || UPPER(v.dataset_code) || ' dataset.'
            ) AS business_definition,
            CASE
                WHEN v.dataset_code = 'acs1' THEN 'ACS 1-year estimates should not be treated as equivalent to ACS 5-year pooled estimates for trend or small-area comparisons.'
                WHEN v.dataset_code = 'acs5' THEN 'ACS 5-year estimates are pooled multi-year estimates and should not be compared directly to ACS 1-year values without noting the survey-span difference.'
                ELSE NULL
            END AS caveats,
            CASE
                WHEN v.dataset_code = 'acs1' THEN ARRAY['NATIONAL', 'STATE']::TEXT[]
                ELSE ARRAY['NATIONAL', 'STATE', 'COUNTY']::TEXT[]
            END AS valid_geo_grains,
            ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
            CASE
                WHEN v.is_publishable_default = TRUE AND v.value_role = 'ESTIMATE' THEN 'PUBLIC_SAFE'
                ELSE 'INTERNAL_ONLY'
            END AS dashboard_suitability,
            'ACS:' || v.variable_code AS comparability_group,
            ARRAY[
                'ACS:' || CASE WHEN v.dataset_code = 'acs1' THEN 'acs5' ELSE 'acs1' END || ':' || v.variable_code
            ]::TEXT[] AS do_not_compare_with,
            'LAST' AS recommended_aggregation,
            'data-eng' AS owner_team,
            TRUE AS is_active
        FROM (
            SELECT DISTINCT ON (dataset_code, variable_code)
                acs_table_sk, dataset_code, variable_code,
                variable_label, concept, universe,
                is_publishable_default, value_role
            FROM gold.dim_acs_variable
            ORDER BY dataset_code, variable_code, vintage_year DESC
        ) v
        JOIN gold.dim_acs_table t
          ON t.acs_table_sk = v.acs_table_sk
        ON CONFLICT (metric_code)
        DO UPDATE SET
            metric_display_name = EXCLUDED.metric_display_name,
            business_definition = EXCLUDED.business_definition,
            caveats = EXCLUDED.caveats,
            valid_geo_grains = EXCLUDED.valid_geo_grains,
            valid_time_grains = EXCLUDED.valid_time_grains,
            dashboard_suitability = EXCLUDED.dashboard_suitability,
            comparability_group = EXCLUDED.comparability_group,
            do_not_compare_with = EXCLUDED.do_not_compare_with,
            recommended_aggregation = EXCLUDED.recommended_aggregation,
            owner_team = EXCLUDED.owner_team,
            is_active = EXCLUDED.is_active,
            updated_at = NOW();
        """
    )

    cur.execute(
        """
        INSERT INTO gold.bridge_metric_acs_variable (metric_catalog_sk, acs_variable_sk)
        SELECT c.metric_catalog_sk, v.acs_variable_sk
        FROM gold.dim_metric_catalog c
        JOIN gold.dim_acs_variable v
          ON c.metric_code = 'ACS:' || v.dataset_code || ':' || v.variable_code
        ON CONFLICT (metric_catalog_sk, acs_variable_sk) DO NOTHING;
        """
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold.dim_metric_catalog
        WHERE source_code = 'CENSUS_ACS'
        """
    )
    return cur.fetchone()[0]


def _fetch_acs_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return ACS observation rows for the given month_start.

    ACS is annual; data only exists for January 1st months.
    acs5 takes precedence over acs1 for the same (geo_id, variable_code, year).
    """
    if month_start.month != 1 or month_start.day != 1:
        return []

    estimate_year = month_start.year
    sql = """
        WITH ranked AS (
            SELECT
                geo_id,
                variable_code,
                table_id,
                dataset,
                estimate_year,
                estimate_value,
                margin_of_error,
                margin_of_error_pct,
                MAKE_DATE(estimate_year, 1, 1)          AS observation_date,
                duration_start,
                duration_end,
                variable_label,
                variable_concept,
                universe,
                ROW_NUMBER() OVER (
                    PARTITION BY geo_id, variable_code
                    ORDER BY
                        CASE dataset WHEN 'acs5' THEN 1
                                     WHEN 'acs1' THEN 2
                                     ELSE 3 END ASC
                )                                       AS rn
            FROM silver_census.fact_demographics
            WHERE estimate_year = %s
              AND estimate_value IS NOT NULL
              AND variable_code IS NOT NULL
              AND variable_code != ''
        )
        SELECT
            geo_id,
            variable_code,
            table_id,
            dataset,
            estimate_year,
            estimate_value,
            margin_of_error,
            margin_of_error_pct,
            observation_date,
            duration_start,
            duration_end,
            variable_label,
            variable_concept,
            universe
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (estimate_year,))
        rows = cur.fetchall()
    logger.info("ACS fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_acs_elements(hook: PostgresHook | None = None) -> int:
    """Sync ACS source-specific metadata into gold.dim_acs_table and gold.dim_acs_variable."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.dim_acs_table (
                dataset_code, vintage_year, table_id, table_title, concept, universe,
                survey_span_years, reference_url
            )
            SELECT
                f.dataset AS dataset_code,
                f.estimate_year AS vintage_year,
                f.table_id,
                COALESCE(NULLIF(MIN(f.variable_concept), ''), f.table_id) AS table_title,
                MIN(f.variable_concept) AS concept,
                MIN(f.universe) AS universe,
                CASE WHEN f.dataset = 'acs5' THEN 5 ELSE 1 END AS survey_span_years,
                'https://api.census.gov/data/' || f.estimate_year::TEXT || '/acs/' || f.dataset || '/variables.json' AS reference_url
            FROM silver_census.fact_demographics f
            WHERE f.table_id IS NOT NULL
              AND f.table_id <> ''
              AND f.dataset IN ('acs1', 'acs5')
            GROUP BY f.dataset, f.estimate_year, f.table_id
            ON CONFLICT (dataset_code, vintage_year, table_id)
            DO UPDATE SET
                table_title = EXCLUDED.table_title,
                concept = EXCLUDED.concept,
                universe = EXCLUDED.universe,
                survey_span_years = EXCLUDED.survey_span_years,
                reference_url = EXCLUDED.reference_url,
                updated_at = NOW();
            """
        )

        cur.execute(
            """
            INSERT INTO gold.dim_acs_variable (
                acs_table_sk, dataset_code, vintage_year, variable_code,
                variable_label, concept, universe, value_role
            )
            SELECT
                t.acs_table_sk,
                f.dataset,
                f.estimate_year,
                f.variable_code,
                COALESCE(NULLIF(f.variable_label, ''), NULLIF(f.variable_concept, ''), f.variable_code) AS variable_label,
                f.variable_concept,
                f.universe,
                'ESTIMATE'::TEXT AS value_role
            FROM (
                SELECT
                    dataset,
                    estimate_year,
                    MIN(table_id)         AS table_id,
                    variable_code,
                    MIN(variable_label)   AS variable_label,
                    MIN(variable_concept) AS variable_concept,
                    MIN(universe)         AS universe
                FROM silver_census.fact_demographics
                WHERE variable_code IS NOT NULL
                  AND variable_code <> ''
                  AND dataset IN ('acs1', 'acs5')
                GROUP BY dataset, estimate_year, variable_code
            ) f
            JOIN gold.dim_acs_table t
              ON t.dataset_code = f.dataset
             AND t.vintage_year = f.estimate_year
             AND t.table_id = f.table_id
            ON CONFLICT (dataset_code, vintage_year, variable_code)
            DO UPDATE SET
                acs_table_sk = EXCLUDED.acs_table_sk,
                variable_label = EXCLUDED.variable_label,
                concept = EXCLUDED.concept,
                universe = EXCLUDED.universe,
                value_role = EXCLUDED.value_role,
                updated_at = NOW();
            """
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM gold.dim_acs_variable
            """
        )
        row_count = cur.fetchone()[0]
        catalog_count = _seed_acs_metric_catalog(cur)
        conn.commit()

    logger.info(
        "refresh_acs_elements: dim_acs_variable row_count=%d, acs_metric_catalog_count=%d",
        row_count,
        catalog_count,
    )
    return row_count


def _upsert_acs_rows(hook: PostgresHook, month_start: date, rows: list[tuple]) -> int:
    """Upsert ACS observation rows into gold.fact_acs_observation."""
    if not rows:
        return 0

    sql = """
        INSERT INTO gold.fact_acs_observation (
            geo_id, geo_level, state_id, state_name, county_id, county_name,
            geo_latitude, geo_longitude, geo_geom,
            time_sk, observation_date, duration_start, duration_end,
            acs_table_sk, acs_variable_sk, dataset_code, vintage_year,
            estimate_value, margin_of_error, margin_of_error_pct,
            estimate_annotation, moe_annotation, as_of_date
        )
        SELECT
            r.geo_id,
            CASE
                WHEN d.geo_level = 'us' THEN 'NATIONAL'
                WHEN d.geo_level = 'state' THEN 'STATE'
                WHEN d.geo_level = 'county' THEN 'COUNTY'
                WHEN r.geo_id = 'us:1' THEN 'NATIONAL'
                WHEN r.geo_id LIKE 'state:%%|county:%%' THEN 'COUNTY'
                WHEN r.geo_id LIKE 'state:%%' THEN 'STATE'
                ELSE 'NATIONAL'
            END AS geo_level,
            CASE WHEN d.state_fips IS NOT NULL THEN LPAD(d.state_fips::TEXT, 2, '0') ELSE NULL END AS state_id,
            d.state_name,
            CASE
                WHEN d.state_fips IS NOT NULL AND d.county_fips IS NOT NULL
                THEN CONCAT(LPAD(d.state_fips::TEXT, 2, '0'), LPAD(d.county_fips::TEXT, 3, '0'))
                ELSE NULL
            END AS county_id,
            d.county_name,
            d.latitude,
            d.longitude,
            d.geom,
            t.time_sk,
            r.observation_date,
            r.duration_start,
            r.duration_end,
            at.acs_table_sk,
            av.acs_variable_sk,
            r.dataset,
            r.estimate_year,
            r.estimate_value,
            r.margin_of_error,
            r.margin_of_error_pct,
            NULL::TEXT,
            NULL::TEXT,
            CURRENT_DATE
        FROM (
            VALUES %s
        ) AS r(
            geo_id, variable_code, table_id, dataset, estimate_year,
            estimate_value, margin_of_error, margin_of_error_pct,
            observation_date, duration_start, duration_end,
            variable_label, variable_concept, universe
        )
        JOIN gold.dim_acs_table at
          ON at.dataset_code = r.dataset
         AND at.vintage_year = r.estimate_year
         AND at.table_id = r.table_id
        JOIN gold.dim_acs_variable av
          ON av.dataset_code = r.dataset
         AND av.vintage_year = r.estimate_year
         AND av.variable_code = r.variable_code
        LEFT JOIN silver_ref.dim_geo d
          ON d.geo_id = r.geo_id
        LEFT JOIN silver_ref.dim_time t
          ON t.date_key = r.observation_date
        ON CONFLICT (geo_id, observation_date, acs_variable_sk, dataset_code)
        DO UPDATE SET
            geo_level = EXCLUDED.geo_level,
            state_id = EXCLUDED.state_id,
            state_name = EXCLUDED.state_name,
            county_id = EXCLUDED.county_id,
            county_name = EXCLUDED.county_name,
            geo_latitude = EXCLUDED.geo_latitude,
            geo_longitude = EXCLUDED.geo_longitude,
            geo_geom = EXCLUDED.geo_geom,
            time_sk = EXCLUDED.time_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            acs_table_sk = EXCLUDED.acs_table_sk,
            vintage_year = EXCLUDED.vintage_year,
            estimate_value = EXCLUDED.estimate_value,
            margin_of_error = EXCLUDED.margin_of_error,
            margin_of_error_pct = EXCLUDED.margin_of_error_pct,
            as_of_date = EXCLUDED.as_of_date,
            updated_at = NOW()
        WHERE (
            fact_acs_observation.estimate_value IS DISTINCT FROM EXCLUDED.estimate_value
            OR fact_acs_observation.margin_of_error IS DISTINCT FROM EXCLUDED.margin_of_error
            OR fact_acs_observation.margin_of_error_pct IS DISTINCT FROM EXCLUDED.margin_of_error_pct
            OR fact_acs_observation.geo_level IS DISTINCT FROM EXCLUDED.geo_level
            OR fact_acs_observation.state_id IS DISTINCT FROM EXCLUDED.state_id
            OR fact_acs_observation.state_name IS DISTINCT FROM EXCLUDED.state_name
            OR fact_acs_observation.county_id IS DISTINCT FROM EXCLUDED.county_id
            OR fact_acs_observation.county_name IS DISTINCT FROM EXCLUDED.county_name
            OR fact_acs_observation.geo_latitude IS DISTINCT FROM EXCLUDED.geo_latitude
            OR fact_acs_observation.geo_longitude IS DISTINCT FROM EXCLUDED.geo_longitude
            OR fact_acs_observation.geo_geom IS DISTINCT FROM EXCLUDED.geo_geom
            OR fact_acs_observation.time_sk IS DISTINCT FROM EXCLUDED.time_sk
            OR fact_acs_observation.duration_start IS DISTINCT FROM EXCLUDED.duration_start
            OR fact_acs_observation.duration_end IS DISTINCT FROM EXCLUDED.duration_end
            OR fact_acs_observation.acs_table_sk IS DISTINCT FROM EXCLUDED.acs_table_sk
            OR fact_acs_observation.vintage_year IS DISTINCT FROM EXCLUDED.vintage_year
            OR fact_acs_observation.as_of_date IS DISTINCT FROM EXCLUDED.as_of_date
        )
    """

    from psycopg2.extras import execute_values

    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=3000)
        row_count = cur.rowcount
        conn.commit()

    return row_count


def merge_acs_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for ACS: fetch and upsert to gold.fact_acs_observation.

    Args:
        shard: dict with key "month_start" (ISO date string).
        hook:  optional PostgresHook; created from CONFIG if not provided.

    Returns:
        dict with keys: month_start, input_rows, output_rows, source_system,
                        sample_observation_dates.
    """
    if hook is None:
        hook = _get_hook()

    month_start = date.fromisoformat(shard["month_start"])
    logger.info("[ACS GOLD] Processing shard %s", month_start)

    rows = _fetch_acs_for_month(hook, month_start)
    output_rows = _upsert_acs_rows(hook, month_start, rows)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[8]
        if obs is not None:
            sample_observation_dates.append(
                obs.isoformat() if hasattr(obs, "isoformat") else str(obs)
            )

    logger.info(
        "[ACS GOLD] Shard %s: input=%d output=%d",
        month_start, len(rows), output_rows,
    )
    return {
        "month_start": month_start.isoformat(),
        "input_rows": len(rows),
        "output_rows": output_rows,
        "source_system": "CENSUS_ACS",
        "sample_observation_dates": sample_observation_dates,
    }
