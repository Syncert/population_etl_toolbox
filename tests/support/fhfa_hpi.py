"""Shared seeding and cleanup for FHFA House Price Index database tests.

The checked-in ``tests/fixtures/fhfa_hpi/hpi_at_county.xlsx`` is FHFA's
county workbook (``Last updated: March 31, 2026``) trimmed to seven
counties, every kept row copied verbatim: Autauga AL (a text FIPS), St.
Clair AL (an interior gap), Chugach AK (no 2000 base, trailing gaps), the
Connecticut planning region 09110, and Delaware's three counties (numeric
FIPS). It is played through the adapter's own capture path by a scripted
client.
"""

from __future__ import annotations

import io
import re
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fhfa_hpi.capture import capture_file
from data_ingestion_toolbox.fhfa_hpi.config import SOURCE_CODE, HpiConfig
from data_ingestion_toolbox.fhfa_hpi.registry import COUNTY_FILE
from data_ingestion_toolbox.fhfa_hpi.silver_fhfa_hpi.load import (
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
FIXTURE = REPOSITORY_ROOT / "tests/fixtures/fhfa_hpi/hpi_at_county.xlsx"
SHEET = "xl/worksheets/sheet1.xml"

KENT = "state:10|county:001"
CHUGACH = "state:02|county:063"

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "01", "vintage": 2024, "name": "Alabama"},
    {"geo_type": "state", "state_fips": "02", "vintage": 2024, "name": "Alaska"},
    {"geo_type": "state", "state_fips": "09", "vintage": 2024, "name": "Connecticut"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
    {
        "geo_type": "county",
        "state_fips": "01",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Autauga County",
    },
    {
        "geo_type": "county",
        "state_fips": "01",
        "county_fips": "115",
        "vintage": 2024,
        "name": "St. Clair County",
    },
    {
        "geo_type": "county",
        "state_fips": "02",
        "county_fips": "063",
        "vintage": 2024,
        "name": "Chugach Census Area",
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


def fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


def revised_workbook(*, vintage: str, kent_2025_hpi: str) -> bytes:
    """The fixture with a new "Last updated" date and Kent's 2025 index revised."""
    source = zipfile.ZipFile(io.BytesIO(fixture_bytes()))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            data = source.read(info)
            if info.filename == "xl/sharedStrings.xml":
                data = data.replace(
                    b"Last updated: March 31, 2026.",
                    f"Last updated: {vintage}.".encode(),
                )
            if info.filename == SHEET:
                sheet = data.decode("utf-8")
                rows = re.findall(r'<row r="\d+"[^>]*>.*?</row>', sheet, re.S)
                kent = next(
                    row
                    for row in rows
                    if "<v>10001</v>" in row and "<v>2025</v>" in row
                )
                revised = re.sub(
                    r'(<c r="F\d+"[^>]*><v>)[^<]*(</v>)',
                    rf"\g<1>{kent_2025_hpi}\2",
                    kent,
                    count=1,
                )
                data = sheet.replace(kent, revised).encode("utf-8")
            target.writestr(info, data)
    return out.getvalue()


class FixtureClient:
    """Answers the county workbook path with the fixture or a given payload."""

    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload if payload is not None else fixture_bytes()
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        return httpx.Response(200, content=self.payload, request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish the county file; return (run, status, facts, published)."""
    run_id, status = capture_file(
        connection_factory,
        COUNTY_FILE,
        config=HpiConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, facts, published


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all FHFA state afterwards."""
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
                    "silver_fhfa_hpi.fact_observation",
                    "silver_fhfa_hpi.observation_revision",
                    "silver_fhfa_hpi.observation_quarantine",
                    "control.fhfa_hpi_file",
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
