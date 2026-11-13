"""Apply the NDCP childcare price relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.dol_childcare.config import ChildcareConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_dol_childcare.sql",
    _PACKAGE / "gold_dol_childcare" / "DDL" / "gold_dol_childcare.sql",
    _PACKAGE / "gold_dol_childcare" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.dol_childcare_file",
    "control.dol_childcare_page",
    "silver_dol_childcare.price_revision",
    "silver_dol_childcare.observation_quarantine",
    "silver_dol_childcare.fact_price",
    "gold_dol_childcare.measure_definition",
    "gold_dol_childcare.observation_revision",
    "gold_dol_childcare.observation_latest",
    "gold_dol_childcare.measure_export",
    "gold_dol_childcare.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=ChildcareConfig().postgres_conn_id)


def ensure_dol_childcare_schema(hook: PostgresHook | None = None) -> None:
    """Apply the NDCP childcare price control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["DOL_CHILDCARE"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
