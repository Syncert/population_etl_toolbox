"""Apply the IRS SOI income relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.irs_soi.config import IrsSoiConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_irs_soi.sql",
    _PACKAGE / "gold_irs_soi" / "DDL" / "gold_irs_soi.sql",
    _PACKAGE / "gold_irs_soi" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.irs_soi_file",
    "silver_irs_soi.dim_measure",
    "silver_irs_soi.dim_agi_band",
    "silver_irs_soi.observation_revision",
    "silver_irs_soi.observation_quarantine",
    "silver_irs_soi.fact_observation",
    "gold_irs_soi.observation_revision",
    "gold_irs_soi.observation_latest",
    "gold_irs_soi.measure_export",
    "gold_irs_soi.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=IrsSoiConfig().postgres_conn_id)


def ensure_irs_soi_schema(hook: PostgresHook | None = None) -> None:
    """Apply the IRS SOI income control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["IRS_SOI"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
