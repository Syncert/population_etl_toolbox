"""Apply the HHS hospital capacity relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.hhs_hospital_capacity.config import (
    HhsHospitalCapacityConfig,
)
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_hhs_hospital_capacity.sql",
    _PACKAGE / "gold_hhs_hospital_capacity" / "DDL" / "gold_hhs_hospital_capacity.sql",
    _PACKAGE / "gold_hhs_hospital_capacity" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.hhs_hospital_capacity_snapshot",
    "control.hhs_hospital_capacity_page",
    "silver_hhs_hospital_capacity.dim_measure",
    "silver_hhs_hospital_capacity.quarantine",
    "silver_hhs_hospital_capacity.facility",
    "silver_hhs_hospital_capacity.facility_week_value",
    "silver_hhs_hospital_capacity.area_week",
    "gold_hhs_hospital_capacity.area_revision",
    "gold_hhs_hospital_capacity.area_latest",
    "gold_hhs_hospital_capacity.facility_latest",
    "gold_hhs_hospital_capacity.facility_week",
    "gold_hhs_hospital_capacity.measure_export",
    "gold_hhs_hospital_capacity.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=HhsHospitalCapacityConfig().postgres_conn_id)


def ensure_hhs_hospital_capacity_schema(hook: PostgresHook | None = None) -> None:
    """Apply the HHS hospital capacity control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["HHS_HOSPITAL_CAPACITY"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
