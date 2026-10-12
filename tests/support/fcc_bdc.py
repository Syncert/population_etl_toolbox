"""Shared seeding and cleanup for FCC broadband availability database tests.

The checked-in files under ``tests/fixtures/fcc_bdc/`` are real answers of
the National Broadband Map public data API read on 2026-10-07: each
registered vintage's ``listAvailabilityData`` answer cut to the fixed
broadband files kept here, the national other-geographies summary cut to
the nation, Delaware, the District of Columbia, their counties and one CBSA
(read and dropped), and Delaware's and DC's place summaries cut to their
``Total`` and ``Urban`` rows. Kept CSV rows are copied verbatim; each file is
named after its FCC ``file_id``. They are played through the adapter's own
capture path by a scripted client with stand-in credentials.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fcc_bdc.capture import capture_vintage
from data_ingestion_toolbox.fcc_bdc.config import SOURCE_CODE, BdcConfig
from data_ingestion_toolbox.fcc_bdc.registry import AS_OF_DATES
from data_ingestion_toolbox.fcc_bdc.silver_fcc_bdc.load import publish_run, replay_run
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/fcc_bdc"

#: Stand-ins: tests prove neither reaches a capture.
FIXTURE_USERNAME = "fixture-user@example.invalid"
FIXTURE_TOKEN = "fixture-token-not-a-real-secret"

NATION = "us:1"
DELAWARE = "state:10"
KENT = "state:10|county:001"
DOVER = "state:10|place:21200"
WASHINGTON_DC = "state:11|place:50000"
DEC_2024, DEC_2025 = AS_OF_DATES

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
    {
        "geo_type": "state",
        "state_fips": "11",
        "vintage": 2024,
        "name": "District of Columbia",
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
    {
        "geo_type": "county",
        "state_fips": "11",
        "county_fips": "001",
        "vintage": 2024,
        "name": "District of Columbia",
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
        "state_fips": "11",
        "place_fips": "50000",
        "vintage": 2024,
        "name": "Washington city",
    },
)


def _canonical(arguments: dict[str, Any]) -> str:
    kind = arguments["geo_type"]
    if kind == "nation":
        return NATION
    if kind == "state":
        return f"state:{arguments['state_fips']}"
    if kind == "county":
        return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"
    return f"state:{arguments['state_fips']}|place:{arguments['place_fips']}"


TRACKED_GEO_IDS = tuple(_canonical(arguments) for arguments in _SEED_ARGUMENTS)


def fixture_config(**overrides: object) -> BdcConfig:
    values: dict[str, object] = {
        "fcc_bdc_username": FIXTURE_USERNAME,
        "fcc_bdc_api_token": FIXTURE_TOKEN,
        "min_spacing_seconds": 0,
        "max_attempts": 1,
    }
    values.update(overrides)
    return BdcConfig(**values)


class FixtureClient:
    """Answers each API URL with its recorded answer, an override, or 404."""

    def __init__(self, overrides: dict[str, bytes] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []
        self.headers: list[dict[str, str]] = []

    def get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        path = url.split("/api/public/map/", 1)[1]
        self.calls.append(path)
        self.headers.append(dict(headers))
        request = httpx.Request("GET", url)
        if path in self.overrides:
            return httpx.Response(200, content=self.overrides[path], request=request)
        if path.startswith("downloads/listAvailabilityData/"):
            name = f"listAvailabilityData_{path.rsplit('/', 1)[1]}.json"
        elif path.startswith("downloads/downloadFile/availability/"):
            name = f"{path.rsplit('/', 1)[1]}.zip"
        else:
            name = ""
        fixture = FIXTURE_DIR / name
        if not name or not fixture.is_file():
            return httpx.Response(404, content=b'{"status":"error"}', request=request)
        return httpx.Response(200, content=fixture.read_bytes(), request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    as_of_date: date,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish one vintage; return (run, status, rows, published)."""
    run_id, status = capture_vintage(
        connection_factory,
        as_of_date,
        config=fixture_config(),
        client=client or FixtureClient(),
    )
    rows = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, rows, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    for as_of_date in AS_OF_DATES:
        run_to_gold(connection_factory, as_of_date)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all FCC state afterwards."""
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
                    "silver_fcc_bdc.availability_row",
                    "silver_fcc_bdc.quarantine",
                    "control.fcc_bdc_file",
                    "control.fcc_bdc_read",
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
