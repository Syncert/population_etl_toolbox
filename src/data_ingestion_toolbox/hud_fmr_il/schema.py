"""Apply the HUD FMR and income-limit relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.hud_fmr_il.config import HudConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_hud_fmr_il.sql",
    _PACKAGE / "gold_hud_fmr_il" / "DDL" / "gold_hud_fmr_il.sql",
    _PACKAGE / "gold_hud_fmr_il" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.hud_fmr_il_file",
    "control.hud_fmr_il_api_capture",
    "silver_hud_fmr_il.observation_revision",
    "silver_hud_fmr_il.observation_quarantine",
    "silver_hud_fmr_il.fact_observation",
    "gold_hud_fmr_il.measure_definition",
    "gold_hud_fmr_il.observation_revision",
    "gold_hud_fmr_il.observation_latest",
    "gold_hud_fmr_il.measure_export",
    "gold_hud_fmr_il.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=HudConfig().postgres_conn_id)


def ensure_hud_fmr_il_schema(hook: PostgresHook | None = None) -> None:
    """Apply the HUD FMR and income-limit control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["HUD_FMR_IL"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
