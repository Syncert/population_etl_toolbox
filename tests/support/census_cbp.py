"""Shared seeding and cleanup for County Business Patterns database tests.

The checked-in zips under ``tests/fixtures/census_cbp`` hold each file's
header and its Delaware rows (with the nation's for the nation file), copied
verbatim. They are played through the adapter's own capture path by a
scripted HTTP client.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_cbp.capture import capture_file
from data_ingestion_toolbox.census_cbp.config import SOURCE_CODE, CbpConfig
from data_ingestion_toolbox.census_cbp.registry import get_file
from data_ingestion_toolbox.census_cbp.silver_census_cbp.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/census_cbp"

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
)

SILVER_TABLES: tuple[str, ...] = (
    "silver_census_cbp.fact_observation",
    "silver_census_cbp.observation_revision",
    "silver_census_cbp.observation_quarantine",
)


def _canonical(arguments: dict[str, Any]) -> str:
    kind = arguments["geo_type"]
    if kind == "nation":
        return "us:1"
    if kind == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


TRACKED_GEO_IDS = tuple(_canonical(arguments) for arguments in _SEED_ARGUMENTS)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / f"{name}.zip").read_bytes()


class FixtureClient:
    """Answers each file with its fixture or a given override."""

    def __init__(self, overrides: dict[str, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        code = url.rsplit("/", 1)[1].removesuffix(".zip")
        if not (FIXTURE_DIR / f"{code}.zip").is_file() and code not in self.overrides:
            return httpx.Response(
                404, content=b"not found", request=httpx.Request("GET", url)
            )
        request = httpx.Request("GET", url)
        if code in self.overrides:
            override = self.overrides[code]
            return httpx.Response(
                override.status_code, content=override.content, request=request
            )
        return httpx.Response(
            200,
            content=fixture_bytes(code),
            headers={"content-type": "application/zip"},
            request=request,
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    kind: str,
    year: int,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish one file; return (run, facts, published)."""
    run_id, _capture = capture_file(
        connection_factory,
        get_file(kind, year),
        config=CbpConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all County Business Patterns state afterwards."""
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
                cursor.execute("DELETE FROM control.census_cbp_file")
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
