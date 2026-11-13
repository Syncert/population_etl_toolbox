"""Apply the NCES IPEDS relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.nces_ipeds.config import NcesIpedsConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_nces_ipeds.sql",
    _PACKAGE / "gold_nces_ipeds" / "DDL" / "gold_nces_ipeds.sql",
    _PACKAGE / "gold_nces_ipeds" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.nces_ipeds_file",
    "silver_nces_ipeds.dim_measure",
    "silver_nces_ipeds.quarantine",
    "silver_nces_ipeds.institution",
    "silver_nces_ipeds.institution_value",
    "silver_nces_ipeds.area_total",
    "gold_nces_ipeds.area_revision",
    "gold_nces_ipeds.area_latest",
    "gold_nces_ipeds.institution_year",
    "gold_nces_ipeds.measure_export",
    "gold_nces_ipeds.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=NcesIpedsConfig().postgres_conn_id)


def ensure_nces_ipeds_schema(hook: PostgresHook | None = None) -> None:
    """Apply the NCES IPEDS control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["NCES_IPEDS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
