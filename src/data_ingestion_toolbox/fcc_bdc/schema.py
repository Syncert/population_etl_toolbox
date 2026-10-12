"""Apply the FCC broadband relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.fcc_bdc.config import BdcConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_fcc_bdc.sql",
    _PACKAGE / "gold_fcc_bdc" / "DDL" / "gold_fcc_bdc.sql",
    _PACKAGE / "gold_fcc_bdc" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.fcc_bdc_read",
    "control.fcc_bdc_file",
    "silver_fcc_bdc.quarantine",
    "silver_fcc_bdc.availability_row",
    "gold_fcc_bdc.measure_definition",
    "gold_fcc_bdc.availability_observation",
    "gold_fcc_bdc.observation_revision",
    "gold_fcc_bdc.observation_latest",
    "gold_fcc_bdc.measure_export",
    "gold_fcc_bdc.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=BdcConfig().postgres_conn_id)


def ensure_fcc_bdc_schema(hook: PostgresHook | None = None) -> None:
    """Apply the FCC broadband control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["FCC_BDC"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
