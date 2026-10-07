"""Shared seeding and cleanup for Building Permits Survey database tests.

The checked-in files under ``tests/fixtures/census_bps`` are the published
files' header rows and a subset of their data rows, copied verbatim. They are
played through the adapter's own capture path by a scripted HTTP client.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_bps.capture import capture_period
from data_ingestion_toolbox.census_bps.config import SOURCE_CODE, BpsConfig
from data_ingestion_toolbox.census_bps.registry import BpsSlice
from data_ingestion_toolbox.census_bps.silver_census_bps.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/census_bps"

#: Path on the provider -> fixture file.
FIXTURES: dict[str, str] = {
    "/County/co2403c.txt": "County_co2403c.txt",
    "/County/co2412y.txt": "County_co2412y.txt",
    "/State/st2403c.txt": "State_st2403c.txt",
    "/Place/South Region/so2024a.txt": "Place_South_so2024a.txt",
}

#: The nation, Delaware, its three counties and two of its places. Every
#: other geography in the fixtures stays `unmapped`, explicit, not dropped.
_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
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
    {
        "geo_type": "place",
        "state_fips": "10",
        "place_fips": "21200",
        "vintage": 2024,
        "name": "Dover city",
    },
    {
        "geo_type": "place",
        "state_fips": "10",
        "place_fips": "77580",
        "vintage": 2024,
        "name": "Wilmington city",
    },
)

SILVER_TABLES: tuple[str, ...] = (
    "silver_census_bps.fact_observation",
    "silver_census_bps.observation_revision",
    "silver_census_bps.observation_quarantine",
)


def _canonical(arguments: dict[str, Any]) -> str:
    kind = arguments["geo_type"]
    if kind == "nation":
        return "us:1"
    if kind == "state":
        return f"state:{arguments['state_fips']}"
    if kind == "county":
        return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"
    return f"state:{arguments['state_fips']}|place:{arguments['place_fips']}"


TRACKED_GEO_IDS = tuple(_canonical(arguments) for arguments in _SEED_ARGUMENTS)


def fixture_bytes(path: str) -> bytes | None:
    name = FIXTURES.get(path)
    return (FIXTURE_DIR / name).read_bytes() if name else None


class FixtureClient:
    """Answers each file with its fixture, a given override, or 404."""

    def __init__(self, overrides: dict[str, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        path = url.split("/econ/bps", 1)[1]
        request = httpx.Request("GET", url)
        if path in self.overrides:
            override = self.overrides[path]
            return httpx.Response(
                override.status_code, content=override.content, request=request
            )
        payload = fixture_bytes(path)
        if payload is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200,
            content=payload,
            headers={"content-type": "text/plain"},
            request=request,
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    frequency: str,
    year: int,
    month: int,
    *,
    files: Sequence[BpsSlice] | None = None,
    client: FixtureClient | None = None,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish one period; return (run, facts, files published)."""
    run_id, _files = capture_period(
        connection_factory,
        frequency,
        year,
        month,
        files=files,
        config=BpsConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
        sleep=lambda _: None,
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all Building Permits state afterwards."""
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
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.census_bps_slice")
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
