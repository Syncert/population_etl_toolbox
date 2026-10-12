"""Apply the FHFA HPI relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.fhfa_hpi.config import HpiConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_fhfa_hpi.sql",
    _PACKAGE / "gold_fhfa_hpi" / "DDL" / "gold_fhfa_hpi.sql",
    _PACKAGE / "gold_fhfa_hpi" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.fhfa_hpi_file",
    "silver_fhfa_hpi.observation_revision",
    "silver_fhfa_hpi.observation_quarantine",
    "silver_fhfa_hpi.fact_observation",
    "gold_fhfa_hpi.measure_definition",
    "gold_fhfa_hpi.observation_revision",
    "gold_fhfa_hpi.observation_latest",
    "gold_fhfa_hpi.measure_export",
    "gold_fhfa_hpi.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=HpiConfig().postgres_conn_id)


def ensure_fhfa_hpi_schema(hook: PostgresHook | None = None) -> None:
    """Apply the FHFA HPI control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["FHFA_HPI"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
