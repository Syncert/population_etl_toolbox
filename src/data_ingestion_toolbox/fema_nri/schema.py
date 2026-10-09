"""Apply the FEMA NRI relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.fema_nri.config import FemaConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_fema_nri.sql",
    _PACKAGE / "gold_fema_nri" / "DDL" / "gold_fema_nri.sql",
    _PACKAGE / "gold_fema_nri" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.fema_nri_run",
    "control.fema_nri_page",
    "silver_fema_nri.quarantine",
    "silver_fema_nri.nri_fact",
    "silver_fema_nri.declaration_revision",
    "gold_fema_nri.measure_definition",
    "gold_fema_nri.nri_observation",
    "gold_fema_nri.declaration_count",
    "gold_fema_nri.observation_revision",
    "gold_fema_nri.observation_latest",
    "gold_fema_nri.measure_export",
    "gold_fema_nri.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=FemaConfig().postgres_conn_id)


def ensure_fema_nri_schema(hook: PostgresHook | None = None) -> None:
    """Apply the FEMA NRI control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["FEMA_NRI"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
