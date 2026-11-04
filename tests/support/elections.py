"""Shared seeding and cleanup for elections tests.

The fixtures under ``tests/fixtures/elections``:

* ``legislators-current.json`` and ``legislators-historical.json`` are the
  unitedstates/congress-legislators files of 2026-11-04 cut to Wisconsin's
  senators and second-district members since 1969, Bernie Sanders (an
  independent caucusing with Democrats), Kay Granger, and two members whose
  party changed during a term (Jefferson Van Drew, James Grant), with each
  term's address and contact fields dropped. Their values are the
  project's own.
* ``wikidata-governors.json`` is shaped exactly like a Wikidata SPARQL
  answer to the registered governor query, but its entity identifiers are
  invented (the build container cannot reach Wikidata): Wisconsin's
  governors since 1987, Florida's Charlie Crist (who left the Republican
  Party in office) and a row with no state code.
* The MEDSL files are constructed in MEDSL's published layouts (comma- or
  tab-delimited, MEDSL's column names) with illustrative vote counts: county
  presidential rows with a TOTAL line, an area published only by mode, New
  York's fusion lines, a county row with no code and a row with ``NA``
  votes; state presidential, Senate (with a special election) and House
  rows. The Dataverse file lists carry invented file ids.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.elections.capture import capture_stream
from data_ingestion_toolbox.elections.config import SOURCE_CODE, ElectionsConfig
from data_ingestion_toolbox.elections.registry import STREAMS
from data_ingestion_toolbox.elections.silver_elections.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/elections"

WISCONSIN = "state:55"
DANE = "state:55|county:025"
WAUKESHA = "state:55|county:133"
ALBANY = "state:36|county:001"

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "12", "vintage": 2024, "name": "Florida"},
    {"geo_type": "state", "state_fips": "27", "vintage": 2024, "name": "Minnesota"},
    {"geo_type": "state", "state_fips": "34", "vintage": 2024, "name": "New Jersey"},
    {"geo_type": "state", "state_fips": "36", "vintage": 2024, "name": "New York"},
    {"geo_type": "state", "state_fips": "48", "vintage": 2024, "name": "Texas"},
    {"geo_type": "state", "state_fips": "50", "vintage": 2024, "name": "Vermont"},
    {"geo_type": "state", "state_fips": "55", "vintage": 2024, "name": "Wisconsin"},
    {"geo_type": "state", "state_fips": "56", "vintage": 2024, "name": "Wyoming"},
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "025",
        "vintage": 2024,
        "name": "Dane County",
    },
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "133",
        "vintage": 2024,
        "name": "Waukesha County",
    },
    {
        "geo_type": "county",
        "state_fips": "36",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Albany County",
    },
)


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


TRACKED_GEO_IDS = tuple(_canonical(arguments) for arguments in _SEED_ARGUMENTS)

#: Dataverse file id -> results fixture, matching the listing fixtures.
_DATA_FILES = {
    "6104603": "county_president.csv",
    "4299753": "state_president.csv",
    "6559998": "senate.csv",
    "11384761": "house.tab",
}
_LISTINGS = {
    "doi:10.7910/DVN/VOQCHQ": "listing-county_president.json",
    "doi:10.7910/DVN/42MVDX": "listing-state_president.json",
    "doi:10.7910/DVN/PEJ5QU": "listing-senate.json",
    "doi:10.7910/DVN/IGFUJS": "listing-house.json",
}


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers every registered URL from its fixture.

    ``overrides`` maps a fixture name to the bytes served in its place, or to
    an HTTP status.
    """

    def __init__(self, overrides: dict[str, bytes | int] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[tuple[str, dict[str, str]]] = []

    def _name(self, url: str, params: dict[str, str]) -> str:
        if url.endswith(".json") and "congress-legislators" in url:
            return url.rsplit("/", 1)[1]
        if "query.wikidata.org" in url:
            return "wikidata-governors.json"
        if url.endswith("/files"):
            return _LISTINGS[params["persistentId"]]
        return _DATA_FILES[url.rsplit("/", 1)[1]]

    def get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        self.calls.append((url, dict(params)))
        request = httpx.Request("GET", url)
        name = self._name(url, params)
        override = self.overrides.get(name)
        if isinstance(override, int):
            return httpx.Response(override, content=b"", request=request)
        body = override if isinstance(override, bytes) else fixture_bytes(name)
        return httpx.Response(200, content=body, request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    stream: str,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish one stream; return (run, status, rows, published)."""
    runtime = ElectionsConfig(min_spacing_seconds=0, max_attempts=1)
    run_id, status = capture_stream(
        connection_factory, stream, config=runtime, client=client or FixtureClient()
    )
    rows = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, rows, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    for stream in STREAMS:
        run_to_gold(connection_factory, stream)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all elections state afterwards."""
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
                    "silver_elections.seat_term",
                    "silver_elections.candidate_result",
                    "silver_elections.quarantine",
                    "control.elections_file",
                    "control.elections_run",
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
