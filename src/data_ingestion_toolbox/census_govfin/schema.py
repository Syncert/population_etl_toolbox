"""Apply the government finance relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_govfin.config import GovFinConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it (the tax burden also reads
#: `gold_bea`), then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_census_govfin.sql",
    _PACKAGE / "gold_census_govfin" / "DDL" / "gold_census_govfin.sql",
    _PACKAGE / "gold_census_govfin" / "DDL" / "gold_tax_burden.sql",
    _PACKAGE / "gold_census_govfin" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.census_govfin_slice",
    "silver_census_govfin.dim_measure",
    "silver_census_govfin.measure_item",
    "silver_census_govfin.unit_government",
    "silver_census_govfin.unit_amount",
    "silver_census_govfin.state_type_total",
    "silver_census_govfin.api_revision",
    "silver_census_govfin.quarantine",
    "silver_census_govfin.fact_observation",
    "gold_census_govfin.observation_revision",
    "gold_census_govfin.observation_latest",
    "gold_census_govfin.county_area_government",
    "gold_census_govfin.tax_mix",
    "gold_census_govfin.tax_burden",
    "gold_census_govfin.measure_export",
    "gold_census_govfin.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    config = GovFinConfig.from_environment()
    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def ensure_census_govfin_schema(hook: PostgresHook | None = None) -> None:
    """Apply the government finance control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CENSUS_GOVFIN"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
