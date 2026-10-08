"""Apply the NCES school relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.nces_ccd.config import CcdConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_nces_ccd.sql",
    _PACKAGE / "gold_nces_ccd" / "DDL" / "gold_nces_ccd.sql",
    _PACKAGE / "gold_nces_ccd" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.nces_ccd_file",
    "silver_nces_ccd.quarantine",
    "silver_nces_ccd.school_location",
    "silver_nces_ccd.school_directory",
    "silver_nces_ccd.school_count",
    "gold_nces_ccd.measure_definition",
    "gold_nces_ccd.school_placement",
    "gold_nces_ccd.school_observation",
    "gold_nces_ccd.observation_revision",
    "gold_nces_ccd.observation_latest",
    "gold_nces_ccd.measure_export",
    "gold_nces_ccd.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CcdConfig().postgres_conn_id)


def ensure_nces_ccd_schema(hook: PostgresHook | None = None) -> None:
    """Apply the NCES school control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["NCES_CCD"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
