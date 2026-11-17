"""Apply the NOAA nClimDiv relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.noaa_climdiv.config import ClimdivConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_noaa_climdiv.sql",
    _PACKAGE / "gold_noaa_climdiv" / "DDL" / "gold_noaa_climdiv.sql",
    _PACKAGE / "gold_noaa_climdiv" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.noaa_climdiv_file",
    "silver_noaa_climdiv.quarantine",
    "silver_noaa_climdiv.county_month",
    "silver_noaa_climdiv.county_month_revision",
    "gold_noaa_climdiv.measure_definition",
    "gold_noaa_climdiv.observation_revision",
    "gold_noaa_climdiv.observation_latest",
    "gold_noaa_climdiv.measure_export",
    "gold_noaa_climdiv.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=ClimdivConfig().postgres_conn_id)


def ensure_noaa_climdiv_schema(hook: PostgresHook | None = None) -> None:
    """Apply the nClimDiv control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["NOAA_CLIMDIV"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
