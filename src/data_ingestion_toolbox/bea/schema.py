"""Apply the BEA relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.bea.config import BeaConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_bea.sql",
    _PACKAGE / "gold_bea" / "DDL" / "gold_bea.sql",
    _PACKAGE / "gold_bea" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.bea_table_capture",
    "silver_bea.dim_line",
    "silver_bea.observation_revision",
    "silver_bea.observation_quarantine",
    "silver_bea.fact_observation",
    "gold_bea.observation_revision",
    "gold_bea.observation_latest",
    "gold_bea.measure_export",
    "gold_bea.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=BeaConfig().postgres_conn_id)


def ensure_bea_schema(hook: PostgresHook | None = None) -> None:
    """Apply the BEA control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["BEA"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
