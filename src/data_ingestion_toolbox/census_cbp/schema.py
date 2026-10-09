"""Apply the County Business Patterns relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_cbp.config import CbpConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_census_cbp.sql",
    _PACKAGE / "gold_census_cbp" / "DDL" / "gold_census_cbp.sql",
    _PACKAGE / "gold_census_cbp" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.census_cbp_file",
    "silver_census_cbp.observation_revision",
    "silver_census_cbp.observation_quarantine",
    "silver_census_cbp.fact_observation",
    "gold_census_cbp.measure_definition",
    "gold_census_cbp.sector_definition",
    "gold_census_cbp.observation_revision",
    "gold_census_cbp.observation_latest",
    "gold_census_cbp.measure_export",
    "gold_census_cbp.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CbpConfig().postgres_conn_id)


def ensure_census_cbp_schema(hook: PostgresHook | None = None) -> None:
    """Apply the County Business Patterns control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CENSUS_CBP"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
