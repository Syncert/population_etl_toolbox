"""Shared seeding and cleanup for IRS SOI county and state income database tests.

The checked-in files under ``tests/fixtures/irs_soi`` hold SOI's own rows
for Delaware (``SOURCE_NOTES.md``). They are played through the adapter's
own capture path by a scripted HTTP client. The seeded geographies are the
nation, Delaware and its three counties; SOI's Other Areas and Puerto Rico
rows stay unmapped and are never served.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.irs_soi.capture import capture_file
from data_ingestion_toolbox.irs_soi.config import SOURCE_CODE, IrsSoiConfig
from data_ingestion_toolbox.irs_soi.registry import get_file
from data_ingestion_toolbox.irs_soi.silver_irs_soi.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/irs_soi"

NATION = "us:1"
DELAWARE = "state:10"
KENT = "state:10|county:001"
NEW_CASTLE = "state:10|county:003"
SUSSEX = "state:10|county:005"

#: Every file the fixtures answer, by registry key.
FIXTURE_FILES: tuple[str, ...] = (
    "county_total:1989",
    "county_total:1996",
    "county_total:2003",
    "county_total:2009",
    "county_total:2010",
    "county_agi:2010",
    "county_total:2011",
    "county_agi:2011",
    "county_total:2023",
    "county_agi:2023",
    "state:2023",
)

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2023, "name": "United States"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2023, "name": "Delaware"},
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "001",
        "vintage": 2023,
        "name": "Kent County",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "003",
        "vintage": 2023,
        "name": "New Castle County",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "005",
        "vintage": 2023,
        "name": "Sussex County",
    },
)

TRACKED_GEO_IDS: tuple[str, ...] = (NATION, DELAWARE, KENT, NEW_CASTLE, SUSSEX)

SILVER_TABLES: tuple[str, ...] = (
    "silver_irs_soi.fact_observation",
    "silver_irs_soi.observation_revision",
    "silver_irs_soi.observation_quarantine",
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers each file with its fixture or a given override; 404 otherwise."""

    def __init__(self, overrides: dict[str, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        name = url.rsplit("/", 1)[1]
        request = httpx.Request("GET", url)
        if name in self.overrides:
            override = self.overrides[name]
            return httpx.Response(
                override.status_code, content=override.content, request=request
            )
        path = FIXTURE_DIR / name
        if not path.is_file():
            return httpx.Response(404, content=b"not found", request=request)
        return httpx.Response(
            200,
            content=path.read_bytes(),
            headers={"content-type": "text/csv"},
            request=request,
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    key: str,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish one file; return (run, facts, published)."""
    run_id, _capture = capture_file(
        connection_factory,
        get_file(key),
        config=IrsSoiConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    for key in FIXTURE_FILES:
        run_to_gold(connection_factory, key)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all SOI income state afterwards."""
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
                cursor.execute("DELETE FROM control.irs_soi_file")
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


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "nation":
        return NATION
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"
