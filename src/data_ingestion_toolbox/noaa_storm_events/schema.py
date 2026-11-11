"""Apply the NOAA Storm Events relations the source owns, before the DAG writes to them."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion_toolbox.noaa_storm_events.config import StormEventsConfig
from data_ingestion_toolbox.utility.gold_schema import (
    SOURCE_SCHEMA_COMPONENTS,
    ensure_gold_schema_from_files,
)

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

_PACKAGE = Path(__file__).resolve().parent

#: Silver first, then the views that read it, then the publisher.
DDL_FILES = [
    _PACKAGE / "DDL" / "silver_noaa_storm_events.sql",
    _PACKAGE / "gold_noaa_storm_events" / "DDL" / "gold_noaa_storm_events.sql",
    _PACKAGE / "gold_noaa_storm_events" / "DDL" / "publisher.sql",
]

REQUIRED_RELATIONS = (
    "control.noaa_storm_events_file",
    "silver_noaa_storm_events.quarantine",
    "silver_noaa_storm_events.zone_county",
    "silver_noaa_storm_events.event",
    "silver_noaa_storm_events.event_revision",
    "silver_noaa_storm_events.event_place",
    "silver_noaa_storm_events.place_year",
    "silver_noaa_storm_events.state_year",
    "silver_noaa_storm_events.event_type",
    "gold_noaa_storm_events.recording_era",
    "gold_noaa_storm_events.measure_definition",
    "gold_noaa_storm_events.place_kind",
    "gold_noaa_storm_events.year_release",
    "gold_noaa_storm_events.observation_revision",
    "gold_noaa_storm_events.observation_latest",
    "gold_noaa_storm_events.event_detail",
    "gold_noaa_storm_events.state_coverage",
    "gold_noaa_storm_events.measure_export",
    "gold_noaa_storm_events.metric_publisher",
)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=StormEventsConfig().postgres_conn_id)


def ensure_noaa_storm_events_schema(hook: PostgresHook | None = None) -> None:
    """Apply the Storm Events control, silver, gold and publisher DDL if stale."""
    ensure_gold_schema_from_files(
        ddl_files=list(DDL_FILES),
        component_name=SOURCE_SCHEMA_COMPONENTS["NOAA_STORM_EVENTS"],
        required_relations=REQUIRED_RELATIONS,
        required_procedures=(),
        hook=hook or _get_hook(),
    )
