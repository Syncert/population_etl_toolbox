"""Apply the vehicle registrations relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)
from data_ingestion_toolbox.vehicle_registrations.config import (
    VehicleRegistrationsConfig,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_vehicle_registrations.sql",
    _PACKAGE / "gold_vehicle_registrations" / "DDL" / "gold_vehicle_registrations.sql",
    _PACKAGE / "gold_vehicle_registrations" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.vehicle_registrations_release",
    "control.vehicle_registrations_document",
    "silver_vehicle_registrations.wisdot_county",
    "silver_vehicle_registrations.dim_measure",
    "silver_vehicle_registrations.quarantine",
    "silver_vehicle_registrations.wisdot_count",
    "silver_vehicle_registrations.fhwa_mv1_figure",
    "gold_vehicle_registrations.wisdot_registration_latest",
    "gold_vehicle_registrations.fhwa_mv1_latest",
    "gold_vehicle_registrations.registration_revision",
    "gold_vehicle_registrations.registration_latest",
    "gold_vehicle_registrations.measure_export",
    "gold_vehicle_registrations.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=VehicleRegistrationsConfig().postgres_conn_id)


def ensure_vehicle_registrations_schema(hook: PostgresHook | None = None) -> None:
    """Apply the vehicle registrations control, silver and gold DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["VEHICLE_REGISTRATIONS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
