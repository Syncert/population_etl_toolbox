"""
Gold analytics layer for the ``gold_census`` schema.

Bootstraps source-specific objects and refreshes ACS metadata from silver.
"""
from __future__ import annotations

import logging
import pathlib

from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.census_acs.config import CONFIG
from data_ingestion_toolbox.utility.gold_schema import ensure_gold_schema_from_files

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold_acs.sql"
_SCHEMA_COMPONENT = "gold_ddl_acs"
_REQUIRED_RELATIONS = (
    "gold_glossary.dim_geo",
    "gold_glossary.dim_time",
    "gold_glossary.dim_source_system",
    "gold_glossary.dim_metric_catalog",
    "gold_glossary.dim_geo_latest",
    "gold_glossary.serving_refresh_state",
    "gold_glossary.serving_refresh_chunk_state",
    "gold_glossary.bridge_metric_acs_variable",
    "gold_census.dim_acs_table",
    "gold_census.dim_acs_variable",
    "gold_census.fact_acs_observation",
    "gold_census.rpt_acs_observations",
    "gold_census.mv_acs_latest",
)
_REQUIRED_PROCEDURES = (
    "gold_glossary.refresh_dim_geo_latest()",
    "gold_census.refresh_rpt_acs_observations(date,date)",
    "gold_census.refresh_mv_acs_latest(date,date)",
    "gold_census.refresh_dashboard_serving_layer_acs(date,date,boolean)",
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
        INSERT INTO gold_glossary.dim_metric_catalog (
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
            FROM gold_census.dim_acs_variable
            ORDER BY dataset_code, variable_code, vintage_year DESC
        ) v
        JOIN gold_census.dim_acs_table t
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
        INSERT INTO gold_glossary.bridge_metric_acs_variable (metric_catalog_sk, acs_variable_sk)
        SELECT c.metric_catalog_sk, v.acs_variable_sk
        FROM gold_glossary.dim_metric_catalog c
        JOIN gold_census.dim_acs_variable v
          ON c.metric_code = 'ACS:' || v.dataset_code || ':' || v.variable_code
        ON CONFLICT (metric_catalog_sk, acs_variable_sk) DO NOTHING;
        """
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold_glossary.dim_metric_catalog
        WHERE source_code = 'CENSUS_ACS'
        """
    )
    return cur.fetchone()[0]


def refresh_acs_elements(hook: PostgresHook | None = None) -> int:
    """Sync ACS source-specific metadata into gold_census.dim_acs_table and gold_census.dim_acs_variable."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        # Keep the metadata aggregates bounded to one ACS release at a time.
        # Aggregating every geography row across every release in one statement
        # can create multi-GB PostgreSQL temporary files.
        cur.execute(
            """
            SELECT DISTINCT dataset, estimate_year
            FROM silver_census.fact_demographics
            WHERE source_system = 'CENSUS_ACS'
              AND dataset IN ('acs1', 'acs5')
            ORDER BY dataset, estimate_year
            """
        )
        releases = cur.fetchall()

        for dataset_code, vintage_year in releases:
            cur.execute(
                """
                INSERT INTO gold_census.dim_acs_table (
                    dataset_code, vintage_year, table_id, table_title, concept, universe,
                    survey_span_years, reference_url
                )
                SELECT
                    f.dataset,
                    f.estimate_year,
                    f.table_id,
                    UPPER(COALESCE(
                        NULLIF(MIN(f.variable_concept), ''),
                        NULLIF(rt.concept, ''),
                        f.table_id
                    )) AS table_title,
                    UPPER(MIN(f.variable_concept)) AS concept,
                    MIN(f.universe) AS universe,
                    CASE WHEN f.dataset = 'acs5' THEN 5 ELSE 1 END AS survey_span_years,
                    'https://api.census.gov/data/' || f.estimate_year::TEXT ||
                        '/acs/' || f.dataset || '/variables.json' AS reference_url
                FROM silver_census.fact_demographics f
                LEFT JOIN raw_census.acs_tables rt
                  ON rt.dataset = f.dataset
                 AND rt.table_id = f.table_id
                WHERE f.dataset = %s
                  AND f.estimate_year = %s
                  AND f.source_system = 'CENSUS_ACS'
                  AND f.table_id IS NOT NULL
                  AND f.table_id <> ''
                GROUP BY
                    f.dataset, f.estimate_year, f.table_id, rt.concept
                ON CONFLICT (dataset_code, vintage_year, table_id)
                DO UPDATE SET
                    table_title = EXCLUDED.table_title,
                    concept = EXCLUDED.concept,
                    universe = EXCLUDED.universe,
                    survey_span_years = EXCLUDED.survey_span_years,
                    reference_url = EXCLUDED.reference_url,
                    updated_at = NOW();
                """,
                (dataset_code, vintage_year),
            )

            cur.execute(
                """
                INSERT INTO gold_census.dim_acs_variable (
                    acs_table_sk, dataset_code, vintage_year, variable_code,
                    variable_label, concept, universe, value_role
                )
                WITH variable_rollup AS (
                    SELECT
                        dataset,
                        estimate_year,
                        MIN(table_id) AS table_id,
                        variable_code,
                        MIN(variable_label) AS variable_label,
                        MIN(variable_concept) AS variable_concept,
                        MIN(universe) AS universe
                    FROM silver_census.fact_demographics
                    WHERE dataset = %s
                      AND estimate_year = %s
                      AND source_system = 'CENSUS_ACS'
                      AND variable_code IS NOT NULL
                      AND variable_code <> ''
                    GROUP BY dataset, estimate_year, variable_code
                )
                SELECT
                    t.acs_table_sk,
                    f.dataset,
                    f.estimate_year,
                    f.variable_code,
                    COALESCE(
                        NULLIF(f.variable_label, ''),
                        NULLIF(f.variable_concept, ''),
                        f.variable_code
                    ) AS variable_label,
                    f.variable_concept,
                    f.universe,
                    'ESTIMATE'::TEXT AS value_role
                FROM variable_rollup f
                JOIN gold_census.dim_acs_table t
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
                """,
                (dataset_code, vintage_year),
            )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM gold_census.dim_acs_variable
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
