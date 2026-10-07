"""Apply the QCEW relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.bls_qcew.config import QcewConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_bls_qcew.sql",
    _PACKAGE / "gold_bls_qcew" / "DDL" / "gold_bls_qcew.sql",
    _PACKAGE / "gold_bls_qcew" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.bls_qcew_slice",
    "silver_bls_qcew.dim_measure",
    "silver_bls_qcew.dim_industry",
    "silver_bls_qcew.observation_revision",
    "silver_bls_qcew.observation_quarantine",
    "silver_bls_qcew.fact_observation",
    "gold_bls_qcew.observation_revision",
    "gold_bls_qcew.observation_latest",
    "gold_bls_qcew.measure_export",
    "gold_bls_qcew.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=QcewConfig().postgres_conn_id)


def ensure_bls_qcew_schema(hook: PostgresHook | None = None) -> None:
    """Apply the QCEW control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["BLS_QCEW"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
