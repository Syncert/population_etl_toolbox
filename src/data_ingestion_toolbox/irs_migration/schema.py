"""Apply the IRS migration relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.irs_migration.config import IrsMigrationConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_irs_migration.sql",
    _PACKAGE / "gold_irs_migration" / "DDL" / "gold_irs_migration.sql",
    _PACKAGE / "gold_irs_migration" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.irs_migration_file",
    "silver_irs_migration.flow_revision",
    "silver_irs_migration.flow_quarantine",
    "silver_irs_migration.fact_flow",
    "gold_irs_migration.flow_revision",
    "gold_irs_migration.flow_latest",
    "gold_irs_migration.total_observation_revision",
    "gold_irs_migration.total_observation_latest",
    "gold_irs_migration.measure_export",
    "gold_irs_migration.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=IrsMigrationConfig().postgres_conn_id)


def ensure_irs_migration_schema(hook: PostgresHook | None = None) -> None:
    """Apply the IRS migration control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["IRS_MIGRATION"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
