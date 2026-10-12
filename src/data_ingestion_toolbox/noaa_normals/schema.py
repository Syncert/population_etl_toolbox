"""Apply the NOAA climate normals relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.noaa_normals.config import NormalsConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_noaa_normals.sql",
    _PACKAGE / "gold_noaa_normals" / "DDL" / "gold_noaa_normals.sql",
    _PACKAGE / "gold_noaa_normals" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.noaa_normals_file",
    "silver_noaa_normals.quarantine",
    "silver_noaa_normals.station",
    "silver_noaa_normals.station_normal",
    "gold_noaa_normals.measure_definition",
    "gold_noaa_normals.station_observation",
    "gold_noaa_normals.observation_revision",
    "gold_noaa_normals.observation_latest",
    "gold_noaa_normals.measure_export",
    "gold_noaa_normals.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=NormalsConfig().postgres_conn_id)


def ensure_noaa_normals_schema(hook: PostgresHook | None = None) -> None:
    """Apply the NOAA climate normals control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["NOAA_NORMALS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
