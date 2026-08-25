"""Gold publication helpers for Census PEP revision and latest views."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.utility.gold_schema import ensure_gold_schema_from_files

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
_DDL_PATH = Path(__file__).parent / "DDL" / "gold_pep.sql"


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def ensure_pep_gold_schema(hook: PostgresHook | None = None) -> None:
    """Apply idempotent source-owned publication views."""
    ensure_gold_schema_from_files(
        ddl_files=[_DDL_PATH],
        component_name="gold_ddl_pep",
        required_relations=(
            "gold_pep.population_estimate_revision",
            "gold_pep.population_estimate_latest",
            "gold_pep.population_change",
            "gold_pep.rpt_pep_observations",
            "gold_pep.mv_pep_latest",
            "gold_pep.measure_export",
            "gold_pep.metric_publisher",
        ),
        required_procedures=(),
        hook=hook or _get_hook(),
    )


def refresh_pep_elements(hook: PostgresHook | None = None) -> int:
    """Return the currently publishable measure count; views need no refresh."""
    hook = hook or _get_hook()
    with hook.get_conn() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM gold_pep.measure_export")
        count = cursor.fetchone()[0]
    logger.info("Census PEP publishable measure count: %d", count)
    return count
