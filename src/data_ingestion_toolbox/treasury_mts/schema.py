"""Apply the Treasury MTS relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.treasury_mts.config import TreasuryMtsConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_treasury_mts.sql",
    _PACKAGE / "gold_treasury_mts" / "DDL" / "gold_treasury_mts.sql",
    _PACKAGE / "gold_treasury_mts" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.treasury_mts_page",
    "silver_treasury_mts.quarantine",
    "silver_treasury_mts.statement_line",
    "silver_treasury_mts.statement_line_revision",
    "gold_treasury_mts.line_definition",
    "gold_treasury_mts.statement_line_revision",
    "gold_treasury_mts.observation_revision",
    "gold_treasury_mts.observation_latest",
    "gold_treasury_mts.outlays_by_function",
    "gold_treasury_mts.measure_export",
    "gold_treasury_mts.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=TreasuryMtsConfig().postgres_conn_id)


def ensure_treasury_mts_schema(hook: PostgresHook | None = None) -> None:
    """Apply the Treasury MTS control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["TREASURY_MTS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
