"""Shared seeding and cleanup for HUD FMR and income-limit database tests.

The checked-in workbooks under ``tests/fixtures/hud_fmr_il`` are HUD User's
FY 2026 FMRs, the FY 2026 reissue, the FY 2027 FMRs and the FY 2026 Section 8
income limits, each trimmed to five rows copied verbatim (row numbers and
cell references as HUD wrote them): Delaware's three counties (two metro, one
nonmetro), Napa County CA (reissued in the revised FY 2026 edition) and
Andover town in Connecticut's Capitol Planning Region (a New England town
row). They are played through the adapter's own capture path by a scripted
client.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.hud_fmr_il.capture import capture_file
from data_ingestion_toolbox.hud_fmr_il.config import SOURCE_CODE, HudConfig
from data_ingestion_toolbox.hud_fmr_il.registry import HudFile, registered_files
from data_ingestion_toolbox.hud_fmr_il.silver_hud_fmr_il.load import (
    publish_run,
    replay_run,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/hud_fmr_il"

KENT = "state:10|county:001"
SUSSEX = "state:10|county:005"
NAPA = "state:06|county:055"
ANDOVER = "state:09|county:110|cousub:01080"

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "06", "vintage": 2024, "name": "California"},
    {"geo_type": "state", "state_fips": "09", "vintage": 2024, "name": "Connecticut"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
    {
        "geo_type": "county",
        "state_fips": "06",
        "county_fips": "055",
        "vintage": 2024,
        "name": "Napa County",
    },
    {
        "geo_type": "county",
        "state_fips": "09",
        "county_fips": "110",
        "vintage": 2024,
        "name": "Capitol Planning Region",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Kent County",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "003",
        "vintage": 2024,
        "name": "New Castle County",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "005",
        "vintage": 2024,
        "name": "Sussex County",
    },
)


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


TRACKED_GEO_IDS = tuple(_canonical(arguments) for arguments in _SEED_ARGUMENTS)


def fixture_bytes(item: HudFile) -> bytes:
    return (FIXTURE_DIR / item.path.rsplit("/", 1)[1]).read_bytes()


class FixtureClient:
    """Answers each registered path with its fixture, an override, or 404."""

    def __init__(self, overrides: dict[str, bytes] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        name = url.rsplit("/", 1)[1]
        self.calls.append(name)
        request = httpx.Request("GET", url)
        if name in self.overrides:
            return httpx.Response(200, content=self.overrides[name], request=request)
        path = FIXTURE_DIR / name
        if not path.is_file():
            return httpx.Response(404, content=b"not found", request=request)
        return httpx.Response(200, content=path.read_bytes(), request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    item: HudFile,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish one edition; return (run, status, facts, published)."""
    run_id, status = capture_file(
        connection_factory,
        item,
        config=HudConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, facts, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    for item in registered_files():
        run_to_gold(connection_factory, item)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all HUD state afterwards."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(TRACKED_GEO_IDS),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()
    preexisting_glossary = glossary_registration_exists(connection_factory, SOURCE_CODE)
    baseline = source_run_ids(connection_factory, SOURCE_CODE)

    def cleanup() -> None:
        owned_runs = sorted(source_run_ids(connection_factory, SOURCE_CODE) - baseline)
        database_connection = connection_factory()
        try:
            with database_connection.cursor() as cursor:
                delete_harvested_glossary_rows(
                    cursor, SOURCE_CODE, preexisting=preexisting_glossary
                )
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event WHERE source_code = %s",
                    (SOURCE_CODE,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution WHERE provider_source = %s",
                    (SOURCE_CODE,),
                )
                for table in (
                    "silver_hud_fmr_il.fact_observation",
                    "silver_hud_fmr_il.observation_revision",
                    "silver_hud_fmr_il.observation_quarantine",
                    "control.hud_fmr_il_file",
                ):
                    cursor.execute(f"DELETE FROM {table}")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(
                    set(TRACKED_GEO_IDS) - preexisting,
                    key=lambda value: (-len(value), value),
                ):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    writer = connection_factory()
    try:
        with writer.cursor() as cursor:
            for arguments in _SEED_ARGUMENTS:
                if _canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory
