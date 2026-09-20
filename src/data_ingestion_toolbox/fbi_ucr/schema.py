"""Apply the FBI UCR relations the source owns, before the DAG writes to them.

A migration runs once, in a bootstrap. Everything this source needs at run
time is a file under `src/`, so the DAG can re-apply it: a warehouse a step
behind the code is repaired before the first insert rather than failing on it,
or silently accepting an older vocabulary. This source is the clearest case --
`agency_geography_relationship.confidence_class` learned the word `derived` in
a migration, and until that migration reached a warehouse the DAG's own writes
violated its check constraint.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.fbi_ucr.config import FbiUcrConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it. `ensure_gold_schema_from_files`
#: applies the files in sorted order, which is this order; the ordering is
#: asserted by `tests/dags/test_source_schema_tasks.py` rather than assumed,
#: because a view whose table does not exist yet fails at `CREATE VIEW`.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_fbi.sql",
    _PACKAGE / "gold_fbi" / "DDL" / "gold_fbi.sql",
    _PACKAGE / "gold_fbi" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.fbi_ucr_release",
    "silver_fbi.observation_revision",
    "silver_fbi.dim_agency",
    "silver_fbi.agency_geography_relationship",
    "silver_fbi.fact_crime_observation",
    "gold_fbi.crime_observation",
    "gold_fbi.agency_observation_area_filter",
    "gold_fbi.measure_export",
    "gold_fbi.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    config = FbiUcrConfig.from_environment()
    if not config.postgres_conn_id.strip():
        raise RuntimeError("PostgreSQL connection ID is not configured")
    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def ensure_fbi_schema(hook: PostgresHook | None = None) -> None:
    """Apply the FBI control, silver, gold, and publisher DDL if it is stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["FBI_UCR"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
