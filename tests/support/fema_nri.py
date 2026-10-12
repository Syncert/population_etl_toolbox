"""Shared seeding and cleanup for FEMA National Risk Index and declarations tests.

The checked-in pages under ``tests/fixtures/fema_nri`` are FEMA's own
answers, saved verbatim: the National Risk Index county layer queried for
Delaware's three counties, Connecticut's Capitol Planning Region, Puerto
Rico's Adjuntas (where a tsunami is ``Not Applicable``) and American Samoa's
Eastern District, and OpenFEMA's declarations for Delaware and Connecticut
since 2020 (with statewide and tribal-area rows, and Connecticut's legacy
county codes). A scripted client serves them by page offset.
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

from data_ingestion_toolbox.fema_nri.capture import capture_stream
from data_ingestion_toolbox.fema_nri.config import SOURCE_CODE, FemaConfig
from data_ingestion_toolbox.fema_nri.registry import DECLARATIONS, NRI, STREAMS
from data_ingestion_toolbox.fema_nri.silver_fema_nri.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/fema_nri"

KENT = "state:10|county:001"
NEW_CASTLE = "state:10|county:003"
CAPITOL = "state:09|county:110"
NEW_HAVEN = "state:09|county:009"
ADJUNTAS = "state:72|county:001"

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "09", "vintage": 2024, "name": "Connecticut"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
    {"geo_type": "state", "state_fips": "72", "vintage": 2024, "name": "Puerto Rico"},
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
    {
        "geo_type": "county",
        "state_fips": "72",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Adjuntas Municipio",
    },
)


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


TRACKED_GEO_IDS = tuple(_canonical(arguments) for arguments in _SEED_ARGUMENTS)


def fixture_records(stream: str) -> list[dict[str, Any]]:
    body = json.loads((FIXTURE_DIR / _NAMES[stream]).read_bytes())
    if stream == NRI:
        return [feature["attributes"] for feature in body["features"]]
    return list(body["DisasterDeclarationsSummaries"])


_NAMES = {NRI: "nri_counties.json", DECLARATIONS: "declarations.json"}


class FixtureClient:
    """Answers each stream's page from its fixture, sliced as the service would.

    Page 0 at the default page size is the fixture's own bytes, verbatim; a
    smaller page size, or a stream override, re-serializes the slice.
    """

    def __init__(
        self, overrides: dict[str, list[dict[str, Any]] | bytes] | None = None
    ) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[dict[str, str]] = []

    def get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        self.calls.append(dict(params))
        request = httpx.Request("GET", url)
        stream = NRI if "resultOffset" in params else DECLARATIONS
        override = self.overrides.get(stream)
        if isinstance(override, bytes):
            return httpx.Response(200, content=override, request=request)
        offset = int(params.get("resultOffset", params.get("$skip", "0")))
        size = int(params.get("resultRecordCount", params.get("$top", "0")))
        records = override if override is not None else fixture_records(stream)
        if override is None and offset == 0 and size >= len(records):
            return httpx.Response(
                200,
                content=(FIXTURE_DIR / _NAMES[stream]).read_bytes(),
                request=request,
            )
        chunk = records[offset : offset + size]
        if stream == NRI:
            body: dict[str, Any] = {
                "features": [{"attributes": item} for item in chunk]
            }
        else:
            body = {"DisasterDeclarationsSummaries": chunk}
        return httpx.Response(200, content=json.dumps(body).encode(), request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    stream: str,
    *,
    client: FixtureClient | None = None,
    config: FemaConfig | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish one stream; return (run, status, rows, published)."""
    runtime = config or FemaConfig(min_spacing_seconds=0, max_attempts=1)
    run_id, status = capture_stream(
        connection_factory, stream, config=runtime, client=client or FixtureClient()
    )
    rows = replay_run(connection_factory, run_id=run_id, config=runtime)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, rows, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    for stream in STREAMS:
        run_to_gold(connection_factory, stream)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all FEMA state afterwards."""
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
                    "silver_fema_nri.nri_fact",
                    "silver_fema_nri.declaration_revision",
                    "silver_fema_nri.quarantine",
                    "control.fema_nri_page",
                    "control.fema_nri_run",
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
