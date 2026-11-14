"""Apply the CDC COVID-19 case count relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.cdc_covid_cases.config import CdcCovidCasesConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_cdc_covid_cases.sql",
    _PACKAGE / "gold_cdc_covid_cases" / "DDL" / "gold_cdc_covid_cases.sql",
    _PACKAGE / "gold_cdc_covid_cases" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.cdc_covid_cases_snapshot",
    "control.cdc_covid_cases_slice",
    "silver_cdc_covid_cases.dim_measure",
    "silver_cdc_covid_cases.quarantine",
    "silver_cdc_covid_cases.slice_group",
    "silver_cdc_covid_cases.area_month",
    "gold_cdc_covid_cases.area_revision",
    "gold_cdc_covid_cases.area_latest",
    "gold_cdc_covid_cases.measure_export",
    "gold_cdc_covid_cases.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CdcCovidCasesConfig().postgres_conn_id)


def ensure_cdc_covid_cases_schema(hook: PostgresHook | None = None) -> None:
    """Apply the CDC COVID-19 case count control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["CDC_COVID_CASES"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
