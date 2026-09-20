"""Apply the USDA NASS relations the source owns, before the DAG writes to them.

A migration runs once, in a bootstrap. Everything this source needs at run
time is a file under `src/`, so the DAG can re-apply it: a warehouse a step
behind the code is repaired before the first insert rather than failing on it,
or silently accepting an older vocabulary.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.usda_nass.config import NassConfig
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
    _PACKAGE / "DDL" / "silver_nass.sql",
    _PACKAGE / "gold_nass" / "DDL" / "gold_nass.sql",
    _PACKAGE / "gold_nass" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.usda_nass_release",
    "control.usda_nass_slice",
    "silver_nass.observation_revision",
    "silver_nass.dim_commodity",
    "silver_nass.dim_statistic",
    "silver_nass.fact_crop_observation",
    "gold_nass.crop_observation",
    "gold_nass.crop_series",
    "gold_nass.measure_export",
    "gold_nass.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    config = NassConfig.from_environment()
    if not config.postgres_conn_id.strip():
        raise RuntimeError("PostgreSQL connection ID is not configured")
    return PostgresHook(postgres_conn_id=config.postgres_conn_id)


def ensure_nass_schema(hook: PostgresHook | None = None) -> None:
    """Apply the NASS control, silver, gold, and publisher DDL if it is stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["USDA_NASS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
