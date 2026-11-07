"""Apply the elections relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.elections.config import ElectionsConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_elections.sql",
    _PACKAGE / "gold_elections" / "DDL" / "gold_elections.sql",
    _PACKAGE / "gold_elections" / "DDL" / "lean_groups.sql",
    _PACKAGE / "gold_elections" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.elections_run",
    "control.elections_file",
    "silver_elections.quarantine",
    "silver_elections.seat_term",
    "silver_elections.candidate_result",
    "silver_elections.district_county_share",
    "gold_elections.office_definition",
    "gold_elections.party_definition",
    "gold_elections.current_read",
    "gold_elections.seat_term",
    "gold_elections.candidate_tally",
    "gold_elections.candidate_total",
    "gold_elections.contest_tally",
    "gold_elections.contest_result_store",
    "gold_elections.contest_result",
    "gold_elections.district_plan_definition",
    "gold_elections.house_plan_year",
    "gold_elections.district_county_share",
    "gold_elections.county_house_district",
    "gold_elections.measure_definition",
    "gold_elections.observation_revision",
    "gold_elections.observation_latest",
    "gold_elections.measure_export",
    "gold_elections.metric_publisher",
    "gold_elections.lean_measure_definition",
    "gold_elections.lean_band_definition",
    "gold_elections.lean_peer_definition",
    "gold_elections.lean_refresh",
    "gold_elections.lean_place_change",
    "gold_elections.lean_group_change",
)

#: The lean-group rebuild (E-3), resolved by name.
REQUIRED_PROCEDURES = (
    "gold_elections.load_lean_inputs(integer[])",
    "gold_elections.build_lean_groups(bigint)",
    "gold_elections.lean_input_fingerprint()",
    "gold_elections.refresh_lean_groups(boolean)",
    "gold_elections.sync_contest_result()",
    "gold_elections.reconcile_contest_result()",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=ElectionsConfig().postgres_conn_id)


def ensure_elections_schema(hook: PostgresHook | None = None) -> None:
    """Apply the elections control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["ELECTIONS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=REQUIRED_PROCEDURES,
        hook=hook or _get_hook(),
    )
