"""Shared seeding and cleanup for NCES Common Core of Data database tests.

The checked-in files under ``tests/fixtures/nces_ccd/`` are NCES's 2024-25
directory, membership, staff and lunch files and the EDGE 2024-25
public-school geocode file, trimmed to Delaware's and Rhode Island's schools
plus one Bureau of Indian Education school (operating code 59, located in
Rolette County, North Dakota), every kept row copied verbatim. Membership
keeps each school's ``Education Unit Total`` row and every breakdown row of
one Rhode Island school, and is compressed with Deflate64 like NCES's own.
Delaware reports direct certification but not free or reduced-price lunch;
Rhode Island reports both. They are played through the adapter's own
capture path by a scripted client. A registered file with no fixture answers
a header-only file.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.nces_ccd.capture import capture_file
from data_ingestion_toolbox.nces_ccd.client import CcdResponse, open_member
from data_ingestion_toolbox.nces_ccd.config import SOURCE_CODE, CcdConfig
from data_ingestion_toolbox.nces_ccd.registry import (
    SchoolFile,
    get_file,
    registered_files,
)
from data_ingestion_toolbox.nces_ccd.silver_nces_ccd.load import publish_run, replay_run
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/nces_ccd"

KENT_DE = "state:10|county:001"
NEW_CASTLE_DE = "state:10|county:003"
SUSSEX_DE = "state:10|county:005"
PROVIDENCE_RI = "state:44|county:007"
DELAWARE = "state:10"
RHODE_ISLAND = "state:44"
BIE_SCHOOL = "590002500172"

GEOCODE = get_file("geocode:2024-2025")
DIRECTORY = get_file("directory:2024-2025")
MEMBERSHIP = get_file("membership:2024-2025")
STAFF = get_file("staff:2024-2025")
LUNCH = get_file("lunch:2024-2025")
#: The files with a checked-in fixture, in the order a run publishes them.
REVIEWED = (GEOCODE, DIRECTORY, MEMBERSHIP, STAFF, LUNCH)

_COUNTIES = {
    "10": (
        ("001", "Kent County"),
        ("003", "New Castle County"),
        ("005", "Sussex County"),
    ),
    "44": (
        ("001", "Bristol County"),
        ("003", "Kent County"),
        ("005", "Newport County"),
        ("007", "Providence County"),
        ("009", "Washington County"),
    ),
}
_STATES = {"10": "Delaware", "44": "Rhode Island"}

TRACKED_GEO_IDS = tuple(f"state:{state}" for state in _STATES) + tuple(
    f"state:{state}|county:{county}"
    for state, counties in _COUNTIES.items()
    for county, _name in counties
)


def fixture_bytes(item: SchoolFile) -> bytes | None:
    path = FIXTURE_DIR / f"{item.stem}.zip"
    return path.read_bytes() if path.is_file() else None


def header_only(item: SchoolFile) -> bytes:
    """A zip holding the registered member with the header and no school."""
    if item.is_geocode:
        reviewed = zipfile.ZipFile(io.BytesIO(fixture_bytes(GEOCODE) or b"")).read(
            GEOCODE.member
        )
        bie = next(
            line
            for line in reviewed.split(b"\r\n")
            if line.startswith(BIE_SCHOOL.encode())
        )
        text = (
            bie.replace(GEOCODE.school_year.encode(), item.school_year.encode())
            + b"\r\n"
        )
    else:
        reviewed = next(
            candidate for candidate in REVIEWED if candidate.component is item.component
        )
        # Through the client: the membership fixture is Deflate64.
        with open_member(fixture_bytes(reviewed) or b"", reviewed) as member:
            text = member.readline()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(item.member, text)
    return buffer.getvalue()


def fixture_response(item: SchoolFile) -> CcdResponse:
    """What the scripted NCES answers for a registered file."""
    payload = fixture_bytes(item) or header_only(item)
    return CcdResponse(
        item.path, payload, {"content-type": "application/x-zip-compressed"}, 200
    )


class FixtureClient:
    """Answers each registered file with its fixture, an override, or a header-only file."""

    def __init__(self, overrides: dict[str, bytes] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        stem = url.rsplit("/", 1)[1].removesuffix(".zip")
        self.calls.append(stem)
        request = httpx.Request("GET", url)
        if stem in self.overrides:
            return httpx.Response(200, content=self.overrides[stem], request=request)
        item = next(
            candidate for candidate in registered_files() if candidate.stem == stem
        )
        return httpx.Response(
            200, content=fixture_response(item).raw_bytes, request=request
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    item: SchoolFile,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish one file; return (run, status, rows, published)."""
    run_id, status = capture_file(
        connection_factory,
        item,
        config=CcdConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    rows = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, rows, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    for item in REVIEWED:
        run_to_gold(connection_factory, item)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed Delaware's and Rhode Island's geographies and remove all NCES state afterwards."""
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
                    "silver_nces_ccd.school_count",
                    "silver_nces_ccd.school_directory",
                    "silver_nces_ccd.school_location",
                    "silver_nces_ccd.quarantine",
                    "control.nces_ccd_file",
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
            for state, name in _STATES.items():
                if f"state:{state}" not in preexisting:
                    seed_geography(
                        cursor,
                        geo_type="state",
                        state_fips=state,
                        vintage=2024,
                        name=name,
                    )
                for county, county_name in _COUNTIES[state]:
                    if f"state:{state}|county:{county}" not in preexisting:
                        seed_geography(
                            cursor,
                            geo_type="county",
                            state_fips=state,
                            county_fips=county,
                            vintage=2024,
                            name=county_name,
                        )
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory
