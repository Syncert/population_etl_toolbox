"""Shared seeding and cleanup for LEHD LODES database tests.

The checked-in files under ``tests/fixtures/census_lodes`` are Delaware's
LODES8 files trimmed to the blocks of two tracts (Kent 0401xx, Sussex
0501xx; origin-destination rows by work block), with the state's real
``version.txt`` and a checksum list recomputed for the trimmed files. They
are played through the adapter's own capture path by a scripted client.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_lodes.capture import capture_state_year
from data_ingestion_toolbox.census_lodes.config import SOURCE_CODE, LodesConfig
from data_ingestion_toolbox.census_lodes.silver_census_lodes.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/census_lodes"

KENT = "state:10|county:001"
SUSSEX = "state:10|county:005"

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
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


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers each path with its fixture, or a given override, or 404."""

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
    state: str = "de",
    year: int = 2023,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish one state-year; return (run, status, rows, published)."""
    run_id, status = capture_state_year(
        connection_factory,
        state,
        year,
        config=LodesConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    rows = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, rows, published


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all LODES state afterwards."""
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
                    "silver_census_lodes.fact_area",
                    "silver_census_lodes.fact_flow",
                    "silver_census_lodes.quarantine",
                    "control.census_lodes_file",
                    "control.census_lodes_slice",
                ):
                    cursor.execute(f"DELETE FROM {table}")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(TRACKED_GEO_IDS) - preexisting):
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
