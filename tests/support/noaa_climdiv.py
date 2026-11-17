"""Shared seeding and cleanup for NOAA nClimDiv database tests.

``tests/fixtures/noaa_climdiv/`` holds NCEI's six registered county files of
the v1.0.0-20261105 release, trimmed to ten lines each copied verbatim
(read through OFFICE_LAB on 2026-11-11): Kent County, Delaware (NCEI state
07, county 001) and Dane County, Wisconsin (47, 025), each for 1895, 1896
and 2024 to 2026, whose November and December 2026 are NCEI's missing
markers. The directory listing the client answers is built here from those
names; NCEI's page is an Apache index of the same hrefs.

Delaware and Kent County are seeded; Dane County is not, so its lines are
kept ``unmapped`` and never served.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.noaa_climdiv.capture import capture_release
from data_ingestion_toolbox.noaa_climdiv.config import SOURCE_CODE, ClimdivConfig
from data_ingestion_toolbox.noaa_climdiv.silver_noaa_climdiv.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/noaa_climdiv"
STAMP = "20261105"
ELEMENTS = ("pcpn", "tmpc", "tmax", "tmin", "hddc", "cddc")

KENT = "state:10|county:001"
TRACKED_GEO_IDS = ("state:10", KENT)


def file_name(element: str, stamp: str = STAMP) -> str:
    return f"climdiv-{element}cy-v1.0.0-{stamp}"


def fixture_files() -> dict[str, bytes]:
    return {
        file_name(element): (FIXTURE_DIR / file_name(element)).read_bytes()
        for element in ELEMENTS
    }


def listing(names: list[str]) -> bytes:
    rows = "".join(f'<a href="{name}">{name}</a>\n' for name in names)
    return f"<html><body><pre>\n{rows}</pre></body></html>".encode()


def next_release(stamp: str, changes: dict[tuple[str, int], str]) -> dict[str, bytes]:
    """The fixture files under a later stamp, with some lines' months replaced.

    ``changes`` maps (element, line number) to the replacement line.
    """
    released: dict[str, bytes] = {}
    for element in ELEMENTS:
        lines = (FIXTURE_DIR / file_name(element)).read_text().splitlines()
        for (changed_element, number), text in changes.items():
            if changed_element == element:
                lines[number - 1] = text
        released[file_name(element, stamp)] = ("\n".join(lines) + "\n").encode()
    return released


class FixtureClient:
    """Answers the directory with a listing of its files, and each file by name."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files = files if files is not None else fixture_files()
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        name = url.rsplit("/", 1)[-1]
        if not name:
            return httpx.Response(
                200, content=listing(sorted(self.files)), request=request
            )
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
    """Capture, replay and publish a release; return (run, statuses, written, published)."""
    run_id, statuses = capture_release(
        connection_factory,
        config=ClimdivConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    written = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, statuses, written, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    run_to_gold(connection_factory)


_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Kent County",
    },
)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed Delaware and Kent County and remove all nClimDiv state afterwards."""
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
                    "silver_noaa_climdiv.county_month_revision",
                    "silver_noaa_climdiv.county_month",
                    "silver_noaa_climdiv.quarantine",
                    "control.noaa_climdiv_file",
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
                geo_id = (
                    f"state:{arguments['state_fips']}"
                    if arguments["geo_type"] == "state"
                    else f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"
                )
                if geo_id not in preexisting:
                    seed_geography(cursor, **arguments)
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory
