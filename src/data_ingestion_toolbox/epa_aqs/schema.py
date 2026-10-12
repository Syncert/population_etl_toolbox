"""Apply the EPA air quality relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.epa_aqs.config import AqsConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_epa_aqs.sql",
    _PACKAGE / "gold_epa_aqs" / "DDL" / "gold_epa_aqs.sql",
    _PACKAGE / "gold_epa_aqs" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.epa_aqs_file",
    "silver_epa_aqs.quarantine",
    "silver_epa_aqs.monitor_fact",
    "gold_epa_aqs.measure_definition",
    "gold_epa_aqs.monitor_observation",
    "gold_epa_aqs.observation_revision",
    "gold_epa_aqs.observation_latest",
    "gold_epa_aqs.measure_export",
    "gold_epa_aqs.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=AqsConfig().postgres_conn_id)


def ensure_epa_aqs_schema(hook: PostgresHook | None = None) -> None:
    """Apply the EPA air quality control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["EPA_AQS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
