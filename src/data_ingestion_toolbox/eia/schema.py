"""Apply the EIA relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.eia.config import EiaConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_eia.sql",
    _PACKAGE / "gold_eia" / "DDL" / "gold_eia.sql",
    _PACKAGE / "gold_eia" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.eia_read",
    "control.eia_page",
    "silver_eia.price_revision",
    "silver_eia.observation_quarantine",
    "silver_eia.fact_retail_price",
    "gold_eia.observation_revision",
    "gold_eia.observation_latest",
    "gold_eia.measure_export",
    "gold_eia.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=EiaConfig().postgres_conn_id)


def ensure_eia_schema(hook: PostgresHook | None = None) -> None:
    """Apply the EIA control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["EIA"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
