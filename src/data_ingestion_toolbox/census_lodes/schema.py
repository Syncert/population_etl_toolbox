"""Apply the LODES relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_lodes.config import LodesConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_census_lodes.sql",
    _PACKAGE / "gold_census_lodes" / "DDL" / "gold_census_lodes.sql",
    _PACKAGE / "gold_census_lodes" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.census_lodes_slice",
    "control.census_lodes_file",
    "silver_census_lodes.quarantine",
    "silver_census_lodes.fact_area",
    "silver_census_lodes.fact_flow",
    "gold_census_lodes.measure_definition",
    "gold_census_lodes.observation_revision",
    "gold_census_lodes.observation_latest",
    "gold_census_lodes.measure_export",
    "gold_census_lodes.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=LodesConfig().postgres_conn_id)


def ensure_census_lodes_schema(hook: PostgresHook | None = None) -> None:
    """Apply the LODES control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CENSUS_LODES"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
