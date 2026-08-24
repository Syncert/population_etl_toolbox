"""
Gold analytics layer for the ``gold_pep`` schema.

Bootstraps source-specific PEP objects and refreshes PEP metadata from silver.
"""

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.utility.gold_schema import ensure_gold_schema_from_files

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold_pep.sql"
_SCHEMA_COMPONENT = "gold_ddl_pep"
_REQUIRED_RELATIONS = (
    "gold_pep.dim_pep_table",
    "gold_pep.dim_pep_variable",
    "gold_pep.fact_pep_observation",
    "gold_pep.rpt_pep_observations",
    "gold_pep.mv_pep_latest",
)
_REQUIRED_PROCEDURES = (
    "gold_pep.refresh_rpt_pep_observations(date,date)",
    "gold_pep.refresh_mv_pep_latest(date,date)",
    "gold_pep.refresh_dashboard_serving_layer_pep(date,date,boolean)",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def ensure_pep_gold_schema(hook: PostgresHook | None = None) -> None:
    """Ensure the PEP gold schema and all required relations exist."""
    if hook is None:
        hook = _get_hook()

    ensure_gold_schema_from_files(
        ddl_files=[_DDL_PATH],
        component_name=_SCHEMA_COMPONENT,
        required_relations=_REQUIRED_RELATIONS,
        required_procedures=_REQUIRED_PROCEDURES,
        hook=hook,
    )


def refresh_pep_elements(hook: PostgresHook | None = None) -> int:
    """Sync PEP source-specific metadata into gold_pep.dim_pep_table and gold_pep.dim_pep_variable."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        # Get distinct (dataset, vintage_year) pairs from silver
        cur.execute(
            """
            SELECT DISTINCT dataset, estimate_year
            FROM silver_pep.fact_population
            WHERE source_system = 'CENSUS_PEP'
              AND estimate_value IS NOT NULL
            ORDER BY dataset, estimate_year
            """
        )
        releases = cur.fetchall()

        for dataset_code, vintage_year in releases:
            # Upsert dim_pep_table
            cur.execute(
                """
                INSERT INTO gold_pep.dim_pep_table (
                    dataset_code, vintage_year, table_id, table_title,
                    concept, universe, reference_url
                )
                SELECT
                    f.dataset,
                    f.estimate_year,
                    f.table_id,
                    UPPER(COALESCE(
                        NULLIF(MIN(f.variable_concept), ''),
                        f.table_id
                    )) AS table_title,
                    UPPER(MIN(f.variable_concept)) AS concept,
                    MIN(f.universe) AS universe,
                    'https://www.census.gov/data/datasets/time-series-democ-pep.html' AS reference_url
                FROM silver_pep.fact_population f
                WHERE f.dataset = %s
                  AND f.estimate_year = %s
                  AND f.source_system = 'CENSUS_PEP'
                  AND f.table_id IS NOT NULL
                  AND f.table_id <> ''
                GROUP BY f.dataset, f.estimate_year, f.table_id
                ON CONFLICT (dataset_code, vintage_year, table_id)
                DO UPDATE SET
                    table_title = EXCLUDED.table_title,
                    concept = EXCLUDED.concept,
                    universe = EXCLUDED.universe,
                    reference_url = EXCLUDED.reference_url,
                    updated_at = NOW();
                """,
                (dataset_code, vintage_year),
            )

            # Upsert dim_pep_variable
            cur.execute(
                """
                INSERT INTO gold_pep.dim_pep_variable (
                    pep_table_sk, dataset_code, vintage_year, variable_code,
                    variable_label, concept, universe, value_role
                )
                WITH variable_rollup AS (
                    SELECT
                        dt.pep_table_sk,
                        f.dataset,
                        f.estimate_year,
                        MIN(f.table_id) AS table_id,
                        f.variable_code,
                        MIN(f.variable_label) AS variable_label,
                        MIN(f.variable_concept) AS variable_concept,
                        MIN(f.universe) AS universe
                    FROM silver_pep.fact_population f
                    JOIN gold_pep.dim_pep_table dt
                      ON dt.dataset_code = f.dataset
                     AND dt.vintage_year = f.estimate_year
                     AND dt.table_id = f.table_id
                    WHERE f.dataset = %s
                      AND f.estimate_year = %s
                      AND f.source_system = 'CENSUS_PEP'
                      AND f.variable_code IS NOT NULL
                      AND f.variable_code <> ''
                    GROUP BY f.dataset, f.estimate_year, f.variable_code, dt.pep_table_sk
                )
                SELECT
                    vr.pep_table_sk,
                    vr.dataset,
                    vr.estimate_year,
                    vr.variable_code,
                    COALESCE(
                        NULLIF(vr.variable_label, ''),
                        NULLIF(vr.variable_concept, ''),
                        vr.variable_code
                    ) AS variable_label,
                    vr.variable_concept,
                    vr.universe,
                    'ESTIMATE'::TEXT AS value_role
                FROM variable_rollup vr
                ON CONFLICT (dataset_code, vintage_year, variable_code)
                DO UPDATE SET
                    pep_table_sk = EXCLUDED.pep_table_sk,
                    variable_label = EXCLUDED.variable_label,
                    concept = EXCLUDED.concept,
                    universe = EXCLUDED.universe,
                    value_role = EXCLUDED.value_role,
                    updated_at = NOW();
                """,
                (dataset_code, vintage_year),
            )

        conn.commit()

        # Return count of variables
        cur.execute("SELECT COUNT(*) FROM gold_pep.dim_pep_variable")
        row_count = cur.fetchone()[0]

    logger.info("refresh_pep_elements: dim_pep_variable row_count=%d", row_count)
    return row_count
