"""Shared seeding and cleanup for 2020 Census (decennial) database tests.

The checked-in fixtures under ``tests/fixtures/census_decennial`` are real
Census Data API responses (the nation, every state, Wisconsin's counties and
Dane County's tracts). They are played through the adapter's own capture path
by a scripted HTTP client, so every database test exercises the capture,
replay and publication code production runs.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_decennial.capture import capture_slices
from data_ingestion_toolbox.census_decennial.config import (
    SOURCE_CODE,
    DecennialConfig,
)
from data_ingestion_toolbox.census_decennial.registry import (
    PL,
    DecennialDataset,
    DecennialSlice,
)
from data_ingestion_toolbox.census_decennial.silver_census_decennial.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/census_decennial"

#: The slices the fixtures answer: the nation, every state, every county the
#: county fixture holds (Wisconsin's) and Wisconsin's tracts (Dane's).
FIXTURE_SLICES: tuple[DecennialSlice, ...] = (
    DecennialSlice("us", "1"),
    DecennialSlice("state", "*"),
    DecennialSlice("county", "*"),
    DecennialSlice("tract", "55"),
)

DANE = "state:55|county:025"

SILVER_TABLES: tuple[str, ...] = (
    "silver_census_decennial.fact_count",
    "silver_census_decennial.observation_revision",
    "silver_census_decennial.observation_quarantine",
    "silver_census_decennial.dim_variable",
)


def _fixture_name(geo_level: str, scope: str) -> str:
    return f"pl_{geo_level}_{'all' if scope == '*' else scope}.json"


def fixture_bytes(geo_level: str, scope: str) -> bytes:
    return (FIXTURE_DIR / _fixture_name(geo_level, scope)).read_bytes()


def fixture_rows(geo_level: str, scope: str) -> list[dict[str, str]]:
    rows = json.loads(fixture_bytes(geo_level, scope))
    header = rows[0]
    return [dict(zip(header, row, strict=True)) for row in rows[1:]]


def dane_tract_codes() -> tuple[str, ...]:
    return tuple(
        row["tract"] for row in fixture_rows("tract", "55") if row["county"] == "025"
    )


def _seed_arguments() -> tuple[dict[str, Any], ...]:
    return (
        {"geo_type": "nation", "vintage": 2020, "name": "United States"},
        {"geo_type": "state", "state_fips": "55", "vintage": 2020, "name": "Wisconsin"},
        {
            "geo_type": "county",
            "state_fips": "55",
            "county_fips": "025",
            "vintage": 2020,
            "name": "Dane County",
        },
        *(
            {
                "geo_type": "tract",
                "state_fips": "55",
                "county_fips": "025",
                "tract_code": tract,
                "vintage": 2020,
                "name": f"Census Tract {tract}",
            }
            for tract in dane_tract_codes()
        ),
    )


def canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "nation":
        return "us:1"
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    county = f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"
    if arguments["geo_type"] == "county":
        return county
    return f"{county}|tract:{arguments['tract_code']}"


def tracked_geo_ids() -> tuple[str, ...]:
    return tuple(canonical(arguments) for arguments in _seed_arguments())


class FixtureClient:
    """Answers each fixture slice with its bytes (or the given override)."""

    def __init__(self, overrides: dict[str, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[dict[str, str]] = []

    def get(self, url: str, *, params: dict[str, str]) -> httpx.Response:
        self.calls.append(dict(params))
        geo_level, scope = params["for"].split(":", 1)
        if geo_level == "tract":
            scope = params["in"].removeprefix("state:")
        request = httpx.Request("GET", url)
        if geo_level in self.overrides:
            response = self.overrides[geo_level]
            return httpx.Response(
                response.status_code, content=response.content, request=request
            )
        return httpx.Response(
            200,
            content=fixture_bytes(geo_level, scope),
            headers={"content-type": "application/json;charset=utf-8"},
            request=request,
        )

    def close(self) -> None:
        return None


def config() -> DecennialConfig:
    return DecennialConfig(
        census_api_key="integration-test-key", min_spacing_seconds=0, max_attempts=1
    )


def run_to_gold(
    connection_factory: Callable[[], connection],
    dataset: DecennialDataset = PL,
    *,
    client: FixtureClient | None = None,
    slices: tuple[DecennialSlice, ...] = FIXTURE_SLICES,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish the fixtures; return (run, facts, slices published)."""
    run_id, _captured = capture_slices(
        connection_factory,
        dataset,
        slices,
        config=config(),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id, dataset=dataset)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def _preexisting_geographies(connection_factory: Callable[[], connection]) -> set[str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids()),),
            )
            return {row[0] for row in cursor.fetchall()}
    finally:
        database_connection.close()


def _cleanup(
    connection_factory: Callable[[], connection],
    preexisting: set[str],
    *,
    preexisting_glossary: bool,
    baseline_run_ids: frozenset,
) -> Callable[[], None]:
    def run() -> None:
        owned_runs = sorted(
            source_run_ids(connection_factory, SOURCE_CODE) - baseline_run_ids
        )
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
                cursor.execute("DELETE FROM control.census_decennial_slice")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(tracked_geo_ids()) - preexisting):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    return run


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all decennial state afterwards."""
    preexisting = _preexisting_geographies(connection_factory)
    request.addfinalizer(
        _cleanup(
            connection_factory,
            preexisting,
            preexisting_glossary=glossary_registration_exists(
                connection_factory, SOURCE_CODE
            ),
            baseline_run_ids=source_run_ids(connection_factory, SOURCE_CODE),
        )
    )
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            for arguments in _seed_arguments():
                if canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory


__all__ = [
    "DANE",
    "FIXTURE_SLICES",
    "FixtureClient",
    "dane_tract_codes",
    "fixture_bytes",
    "fixture_rows",
    "reviewed_warehouse",
    "run_to_gold",
    "tracked_geo_ids",
]
