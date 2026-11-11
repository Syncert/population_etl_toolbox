"""Shared seeding and cleanup for NOAA Storm Events database tests.

``tests/fixtures/noaa_storm_events/`` holds, read through OFFICE_LAB on
2026-11-11:

* NCEI's details files for 1950, 1995 and 2024 (``c20260323``, ``c20260323``
  and ``c20260728``), trimmed and re-compressed: 1995 keeps its 13 Wisconsin
  county rows (all Dane County); 2024 keeps its 34 rows for Delaware's four
  forecast zones, Wisconsin zones 063 (Dane), 013 (Northern Marinette) and
  the three county rows for Dane County; 1950 keeps its header only. Rows are
  NCEI's, written back with Python's csv module.
* NWS's zone-county files ``bp16ap26.dbx`` and ``bp18mr25.dbx``, trimmed to
  their Delaware lines and Wisconsin zones 013, 063 and 073, lines verbatim.

The NWS page and NCEI listing the client answers are built here from those
names; both are HTML pages of the same hrefs.

Seeded: the nation, Delaware and its three counties, Wisconsin and Dane
County. Marinette County is not seeded, so its zone events match a zone and
reach no county.
"""

from __future__ import annotations

import csv
import gzip
import io
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.noaa_storm_events.capture import capture_release
from data_ingestion_toolbox.noaa_storm_events.config import (
    SOURCE_CODE,
    StormEventsConfig,
)
from data_ingestion_toolbox.noaa_storm_events.silver_noaa_storm_events.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/noaa_storm_events"

DETAILS_1950 = "StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz"
DETAILS_1995 = "StormEvents_details-ftp_v1.0_d1995_c20260323.csv.gz"
DETAILS_2024 = "StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz"
ZONE_2025 = "bp18mr25.dbx"
ZONE_2026 = "bp16ap26.dbx"

NATION = "us:1"
DELAWARE = "state:10"
WISCONSIN = "state:55"
KENT = "state:10|county:001"
NEW_CASTLE = "state:10|county:003"
SUSSEX = "state:10|county:005"
DANE = "state:55|county:025"
TRACKED_GEO_IDS = (NATION, DELAWARE, WISCONSIN, KENT, NEW_CASTLE, SUSSEX, DANE)


def fixture_files() -> dict[str, bytes]:
    return {
        name: (FIXTURE_DIR / name).read_bytes()
        for name in (DETAILS_1950, DETAILS_1995, DETAILS_2024, ZONE_2025, ZONE_2026)
    }


def details_rows(name: str) -> list[list[str]]:
    """The header and rows of a fixture details file."""
    text = gzip.decompress((FIXTURE_DIR / name).read_bytes()).decode()
    return list(csv.reader(io.StringIO(text, newline="")))


def details_file(rows: list[list[str]]) -> bytes:
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    return gzip.compress(buffer.getvalue().encode(), mtime=0)


def page(names: list[str]) -> bytes:
    links = "".join(f'<a href="{name}">{name}</a>\n' for name in names)
    return f"<html><body>\n{links}</body></html>".encode()


class FixtureClient:
    """Answers the NWS page, the NCEI listing, and each file by name."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files = files if files is not None else fixture_files()
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        if url.endswith("/gis/ZoneCounty"):
            zones = sorted(name for name in self.files if name.endswith(".dbx"))
            # NWS links the files by site path.
            return httpx.Response(
                200,
                content=page(
                    [f"/source/gis/Shapefiles/County/{name}" for name in zones]
                ),
                request=request,
            )
        name = url.rsplit("/", 1)[-1]
        if not name:
            details = sorted(name for name in self.files if name.endswith(".csv.gz"))
            return httpx.Response(200, content=page(details), request=request)
        if name not in self.files:
            return httpx.Response(404, content=b"", request=request)
        return httpx.Response(200, content=self.files[name], request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, dict[str, str], int, int]:
    """Capture, replay and publish a read; return (run, statuses, written, published)."""
    run_id, statuses = capture_release(
        connection_factory,
        config=StormEventsConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
        sleep=lambda _seconds: None,
    )
    written = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, statuses, written, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    run_to_gold(connection_factory)


_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
    {"geo_type": "state", "state_fips": "55", "vintage": 2024, "name": "Wisconsin"},
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
        "state_fips": "55",
        "county_fips": "025",
        "vintage": 2024,
        "name": "Dane County",
    },
)


def _geo_id(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "nation":
        return NATION
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


STORM_TABLES = (
    "silver_noaa_storm_events.place_year",
    "silver_noaa_storm_events.state_year",
    "silver_noaa_storm_events.event_place",
    "silver_noaa_storm_events.event_revision",
    "silver_noaa_storm_events.event",
    "silver_noaa_storm_events.event_type",
    "silver_noaa_storm_events.zone_county",
    "silver_noaa_storm_events.quarantine",
    "control.noaa_storm_events_file",
)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the reviewed geography and remove all Storm Events state afterwards."""
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
                for table in STORM_TABLES:
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
                if _geo_id(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory
