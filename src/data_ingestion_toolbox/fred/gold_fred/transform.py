"""
Gold analytics layer for the ``gold_fred`` schema.

Bootstraps source-specific objects and refreshes FRED metadata from silver.
"""
from __future__ import annotations

import logging
import pathlib

from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.fred.config import CONFIG
from data_ingestion_toolbox.utility.gold_schema import ensure_gold_schema_from_files

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold_fred.sql"
_SCHEMA_COMPONENT = "gold_ddl_fred"
_REQUIRED_RELATIONS = (
    "gold_glossary.dim_geo",
    "gold_glossary.dim_source_system",
    "gold_glossary.dim_metric_catalog",
    "gold_glossary.dim_geo_latest",
    "gold_glossary.bridge_metric_fred_series",
    "gold_fred.dim_fred_series",
    "gold_fred.fact_fred_observation",
    "gold_fred.rpt_fred_observations",
    "gold_fred.mv_fred_latest",
)
_REQUIRED_PROCEDURES = (
    "gold_glossary.refresh_dim_geo_latest()",
    "gold_fred.refresh_rpt_fred_observations(date,date)",
    "gold_fred.refresh_mv_fred_latest(date,date)",
    "gold_fred.refresh_dashboard_serving_layer_fred(date,date)",
)


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def ensure_fred_gold_schema(hook: PostgresHook | None = None) -> None:
    if hook is None:
        hook = _get_hook()

    ensure_gold_schema_from_files(
        ddl_files=[_DDL_PATH],
        component_name=_SCHEMA_COMPONENT,
        required_relations=_REQUIRED_RELATIONS,
        required_procedures=_REQUIRED_PROCEDURES,
        hook=hook,
    )


def _seed_fred_metric_catalog(cur) -> int:
    """Populate dim_metric_catalog and bridge_metric_fred_series from dim_fred_series."""
    cur.execute(
        """
        WITH series_domain AS (
            SELECT DISTINCT ON (f.series_id)
                f.series_id,
                COALESCE(NULLIF(f.domain, ''), 'macro') AS domain
            FROM silver_fred.fact_economic_indicators f
            WHERE f.series_id IS NOT NULL
              AND f.series_id <> ''
            ORDER BY f.series_id, f.observation_date DESC
        )
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
            'FRED:' || s.series_id AS metric_code,
            COALESCE(NULLIF(s.series_title, ''), s.series_id) AS metric_display_name,
            'FRED' AS source_code,
            'FRED_SERIES' AS source_object_type,
            CONCAT_WS(
                ' ',
                COALESCE(NULLIF(s.lineage_notes, ''), 'FRED curated macro series.'),
                CASE
                    WHEN sd.domain IS NOT NULL THEN 'Domain: ' || sd.domain || '.'
                    ELSE NULL
                END,
                CASE
                    WHEN NULLIF(s.frequency, '') IS NOT NULL THEN 'Frequency: ' || s.frequency || '.'
                    ELSE NULL
                END,
                CASE
                    WHEN NULLIF(s.units, '') IS NOT NULL THEN 'Units: ' || s.units || '.'
                    ELSE NULL
                END
            ) AS business_definition,
            CASE
                WHEN s.is_republished_series = TRUE THEN 'FRED republishes upstream source series; confirm the original publisher before making primary-source comparability claims.'
                ELSE NULL
            END AS caveats,
            ARRAY['NATIONAL']::TEXT[] AS valid_geo_grains,
            CASE
                WHEN LOWER(COALESCE(s.frequency, '')) LIKE '%daily%' THEN ARRAY['DAILY']::TEXT[]
                WHEN LOWER(COALESCE(s.frequency, '')) LIKE '%weekly%' THEN ARRAY['WEEKLY']::TEXT[]
                WHEN LOWER(COALESCE(s.frequency, '')) LIKE '%quarter%' THEN ARRAY['QUARTERLY']::TEXT[]
                WHEN LOWER(COALESCE(s.frequency, '')) LIKE '%annual%' THEN ARRAY['ANNUAL']::TEXT[]
                ELSE ARRAY['MONTHLY']::TEXT[]
            END AS valid_time_grains,
            'PUBLIC_SAFE' AS dashboard_suitability,
            'FRED:' || UPPER(COALESCE(sd.domain, 'macro')) AS comparability_group,
            ARRAY[]::TEXT[] AS do_not_compare_with,
            'LAST' AS recommended_aggregation,
            'data-eng' AS owner_team,
            TRUE AS is_active
        FROM gold_fred.dim_fred_series s
        LEFT JOIN series_domain sd
          ON sd.series_id = s.series_id
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
        INSERT INTO gold_glossary.bridge_metric_fred_series (metric_catalog_sk, fred_series_sk)
        SELECT c.metric_catalog_sk, s.fred_series_sk
        FROM gold_glossary.dim_metric_catalog c
        JOIN gold_fred.dim_fred_series s
          ON c.metric_code = 'FRED:' || s.series_id
        ON CONFLICT (metric_catalog_sk, fred_series_sk) DO NOTHING;
        """
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold_glossary.dim_metric_catalog
        WHERE source_code = 'FRED'
        """
    )
    return cur.fetchone()[0]


def refresh_fred_elements(hook: PostgresHook | None = None) -> int:
    """Sync FRED source-specific metadata into gold_fred.dim_fred_series."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold_fred.dim_fred_series (
                series_id,
                series_title,
                source_provider,
                original_source_name,
                is_primary_source_series,
                is_republished_series,
                frequency,
                units,
                seasonal_adjustment,
                transformation_method,
                realtime_available,
                lineage_notes,
                reference_url
            )
            SELECT DISTINCT ON (f.series_id)
                f.series_id,
                COALESCE(NULLIF(f.series_title, ''), rs.title, f.series_id) AS series_title,
                'FRED' AS source_provider,
                NULL::TEXT AS original_source_name,
                FALSE AS is_primary_source_series,
                TRUE AS is_republished_series,
                COALESCE(f.frequency, rs.frequency) AS frequency,
                COALESCE(f.unit_of_measure, rs.units) AS units,
                COALESCE(f.seasonal_adjustment, rs.seasonal_adjustment) AS seasonal_adjustment,
                NULL::TEXT AS transformation_method,
                TRUE AS realtime_available,
                'Series ingested through FRED curation path; verify original publisher for primary-source comparisons.' AS lineage_notes,
                'https://fred.stlouisfed.org/series/' || f.series_id AS reference_url
            FROM silver_fred.fact_economic_indicators f
            LEFT JOIN raw_fred.fred_series rs
              ON rs.series_id = f.series_id
            WHERE f.series_id IS NOT NULL
              AND f.series_id <> ''
            ORDER BY f.series_id, f.observation_date DESC
            ON CONFLICT (series_id)
            DO UPDATE SET
                series_title = EXCLUDED.series_title,
                source_provider = EXCLUDED.source_provider,
                original_source_name = EXCLUDED.original_source_name,
                is_primary_source_series = EXCLUDED.is_primary_source_series,
                is_republished_series = EXCLUDED.is_republished_series,
                frequency = EXCLUDED.frequency,
                units = EXCLUDED.units,
                seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                transformation_method = EXCLUDED.transformation_method,
                realtime_available = EXCLUDED.realtime_available,
                lineage_notes = EXCLUDED.lineage_notes,
                reference_url = EXCLUDED.reference_url,
                updated_at = NOW();
            """
        )

        cur.execute("SELECT COUNT(*) FROM gold_fred.dim_fred_series")
        row_count = cur.fetchone()[0]
        catalog_count = _seed_fred_metric_catalog(cur)
        conn.commit()

    logger.info(
        "refresh_fred_elements: dim_fred_series row_count=%d, fred_metric_catalog_count=%d",
        row_count,
        catalog_count,
    )
    return row_count
