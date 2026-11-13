"""Apply the CDC ILINet relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.cdc_ilinet.config import CdcIlinetConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_cdc_ilinet.sql",
    _PACKAGE / "gold_cdc_ilinet" / "DDL" / "gold_cdc_ilinet.sql",
    _PACKAGE / "gold_cdc_ilinet" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.cdc_ilinet_release",
    "control.cdc_ilinet_download",
    "silver_cdc_ilinet.dim_measure",
    "silver_cdc_ilinet.quarantine",
    "silver_cdc_ilinet.region",
    "silver_cdc_ilinet.week_value",
    "gold_cdc_ilinet.area_revision",
    "gold_cdc_ilinet.area_latest",
    "gold_cdc_ilinet.measure_export",
    "gold_cdc_ilinet.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CdcIlinetConfig().postgres_conn_id)


def ensure_cdc_ilinet_schema(hook: PostgresHook | None = None) -> None:
    """Apply the CDC ILINet control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CDC_ILINET"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
