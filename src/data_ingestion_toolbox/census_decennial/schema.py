"""Apply the 2020 Census relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_decennial.config import DecennialConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_census_decennial.sql",
    _PACKAGE / "gold_census_decennial" / "DDL" / "gold_census_decennial.sql",
    _PACKAGE / "gold_census_decennial" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.census_decennial_slice",
    "silver_census_decennial.dim_variable",
    "silver_census_decennial.observation_revision",
    "silver_census_decennial.observation_quarantine",
    "silver_census_decennial.fact_count",
    "gold_census_decennial.count_revision",
    "gold_census_decennial.count_latest",
    "gold_census_decennial.measure_export",
    "gold_census_decennial.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    config = DecennialConfig.from_environment()
    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def ensure_census_decennial_schema(hook: PostgresHook | None = None) -> None:
    """Apply the 2020 Census control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CENSUS_DECENNIAL"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
