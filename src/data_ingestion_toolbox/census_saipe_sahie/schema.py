"""Apply the SAIPE/SAHIE relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_saipe_sahie.config import SaeConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_census_sae.sql",
    _PACKAGE / "gold_census_sae" / "DDL" / "gold_census_sae.sql",
    _PACKAGE / "gold_census_sae" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.census_sae_slice",
    "silver_census_sae.dim_measure",
    "silver_census_sae.observation_revision",
    "silver_census_sae.observation_quarantine",
    "silver_census_sae.fact_estimate",
    "gold_census_sae.estimate_revision",
    "gold_census_sae.estimate_latest",
    "gold_census_sae.measure_export",
    "gold_census_sae.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    config = SaeConfig.from_environment()
    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def ensure_census_sae_schema(hook: PostgresHook | None = None) -> None:
    """Apply the SAIPE/SAHIE control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CENSUS_SAIPE_SAHIE"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
