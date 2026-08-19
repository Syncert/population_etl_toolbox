"""
Gold analytics layer for the ``gold_fred`` schema.

Bootstraps source-specific objects and refreshes FRED metadata from silver.
"""

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

from data_ingestion_toolbox.fred.config import CONFIG
from data_ingestion_toolbox.utility.gold_schema import ensure_gold_schema_from_files

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold_fred.sql"
_PUBLISHER_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "publisher.sql"
_SCHEMA_COMPONENT = "gold_ddl_fred"
_REQUIRED_RELATIONS = (
    "control.serving_refresh_state",
    "control.serving_refresh_chunk_state",
    "gold_fred.dim_fred_series",
    "gold_fred.fact_fred_observation",
    "gold_fred.rpt_fred_observations",
    "gold_fred.mv_fred_latest",
    "gold_fred.metric_publisher",
)
_REQUIRED_PROCEDURES = (
    "gold_fred.refresh_rpt_fred_observations(date,date)",
    "gold_fred.refresh_mv_fred_latest(date,date)",
    "gold_fred.refresh_dashboard_serving_layer_fred(date,date,boolean)",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def ensure_fred_gold_schema(hook: PostgresHook | None = None) -> None:
    if hook is None:
        hook = _get_hook()

    ensure_gold_schema_from_files(
        ddl_files=[_DDL_PATH, _PUBLISHER_DDL_PATH],
        component_name=_SCHEMA_COMPONENT,
        required_relations=_REQUIRED_RELATIONS,
        required_procedures=_REQUIRED_PROCEDURES,
        hook=hook,
    )


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
        conn.commit()

    logger.info("refresh_fred_elements: dim_fred_series row_count=%d", row_count)
    return row_count
