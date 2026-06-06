"""
Gold analytics layer — ACS/Census subject transform.

Handles fetching ACS silver data for a given month and upserting
into the shared gold.fact_metrics table.

ACS data is annual; month_start must be January 1st to yield rows.
ACS 5-year estimates (acs5) take precedence over 1-year (acs1).
"""
from __future__ import annotations

import logging
import pathlib
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.census_acs.config import CONFIG
from data_ingestion_toolbox.utility.gold_schema import ensure_gold_schema_from_files

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold_acs.sql"
_SCHEMA_COMPONENT = "gold_ddl_acs"
_REQUIRED_RELATIONS = (
    "gold.dim_geo",
    "gold.dim_time",
    "gold.dim_source_system",
    "gold.dim_metric_catalog",
    "gold.dim_geo_latest",
    "gold.dim_acs_table",
    "gold.dim_acs_variable",
    "gold.fact_acs_observation",
    "gold.rpt_observation_dashboard",
    "gold.mv_latest_dashboard",
)
_REQUIRED_PROCEDURES = (
    "gold.refresh_dim_geo_latest()",
    "gold.refresh_rpt_acs_observation_dashboard(date,date)",
    "gold.refresh_mv_acs_latest_dashboard(date,date)",
    "gold.refresh_dashboard_serving_layer_acs(date,date)",
)


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def ensure_acs_gold_schema(hook: PostgresHook | None = None) -> None:
    if hook is None:
        hook = _get_hook()

    ensure_gold_schema_from_files(
        ddl_files=[_DDL_PATH],
        component_name=_SCHEMA_COMPONENT,
        required_relations=_REQUIRED_RELATIONS,
        required_procedures=_REQUIRED_PROCEDURES,
        hook=hook,
    )


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
            WITH table_year_rollup AS (
                SELECT
                    f.dataset AS dataset_code,
                    f.estimate_year AS vintage_year,
                    f.table_id,
                    MIN(f.variable_concept) AS year_concept,
                    MIN(f.universe) AS year_universe
                FROM silver_census.fact_demographics f
                WHERE f.table_id IS NOT NULL
                  AND f.table_id <> ''
                  AND f.dataset IN ('acs1', 'acs5')
                GROUP BY f.dataset, f.estimate_year, f.table_id
            ),
            table_cross_year_title AS (
                SELECT DISTINCT ON (f.dataset, f.table_id)
                    f.dataset AS dataset_code,
                    f.table_id,
                    NULLIF(f.variable_concept, '') AS canonical_table_title
                FROM silver_census.fact_demographics f
                WHERE f.table_id IS NOT NULL
                  AND f.table_id <> ''
                  AND f.dataset IN ('acs1', 'acs5')
                  AND NULLIF(f.variable_concept, '') IS NOT NULL
                ORDER BY f.dataset, f.table_id, f.estimate_year DESC
            )
            SELECT
                r.dataset_code,
                r.vintage_year,
                r.table_id,
                UPPER(COALESCE(NULLIF(r.year_concept, ''), c.canonical_table_title, r.table_id)) AS table_title,
                UPPER(r.year_concept) AS concept,
                r.year_universe AS universe,
                CASE WHEN r.dataset_code = 'acs5' THEN 5 ELSE 1 END AS survey_span_years,
                'https://api.census.gov/data/' || r.vintage_year::TEXT || '/acs/' || r.dataset_code || '/variables.json' AS reference_url
            FROM table_year_rollup r
            LEFT JOIN table_cross_year_title c
              ON c.dataset_code = r.dataset_code
             AND c.table_id = r.table_id
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
