"""Apply the CMS long-term care relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.cms_ltc.config import CmsLtcConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_cms_ltc.sql",
    _PACKAGE / "gold_cms_ltc" / "DDL" / "gold_cms_ltc.sql",
    _PACKAGE / "gold_cms_ltc" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.cms_ltc_file",
    "silver_cms_ltc.dim_measure",
    "silver_cms_ltc.quarantine",
    "silver_cms_ltc.facility_report",
    "silver_cms_ltc.facility_count",
    "silver_cms_ltc.area_total",
    "silver_cms_ltc.facility_place",
    "gold_cms_ltc.area_revision",
    "gold_cms_ltc.area_latest",
    "gold_cms_ltc.facility_latest",
    "gold_cms_ltc.facility_history",
    "gold_cms_ltc.measure_export",
    "gold_cms_ltc.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CmsLtcConfig().postgres_conn_id)


def ensure_cms_ltc_schema(hook: PostgresHook | None = None) -> None:
    """Apply the CMS long-term care control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CMS_LTC"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
