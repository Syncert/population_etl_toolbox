"""Apply the USAspending relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.usaspending.config import UsaspendingConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_usaspending.sql",
    _PACKAGE / "gold_usaspending" / "DDL" / "gold_usaspending.sql",
    _PACKAGE / "gold_usaspending" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.usaspending_request",
    "silver_usaspending.quarantine",
    "silver_usaspending.agency_year",
    "silver_usaspending.geography_amount",
    "silver_usaspending.geography_amount_revision",
    "gold_usaspending.measure_definition",
    "gold_usaspending.geography_amount_served",
    "gold_usaspending.observation_revision",
    "gold_usaspending.observation_latest",
    "gold_usaspending.awards_by_award_type",
    "gold_usaspending.awards_by_agency",
    "gold_usaspending.measure_export",
    "gold_usaspending.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=UsaspendingConfig().postgres_conn_id)


def ensure_usaspending_schema(hook: PostgresHook | None = None) -> None:
    """Apply the USAspending control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["USASPENDING"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
