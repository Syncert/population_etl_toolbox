"""Shared fixtures for the Census government finance tests.

The unit-file fixtures under ``tests/fixtures/census_govfin`` are excerpts
in the 2017/2022 Census of Governments fixed-width layouts: each opens with
real lines from the 2022 zip (read on OFFICE_LAB, 2026-11-05) and continues
with constructed Wisconsin governments whose figures are chosen so the
county-area sums can be checked by hand (see ``SOURCE_NOTES.md``). They are
zipped here, deterministically, into the archive the registry names, and
played through the adapter's own capture path by a scripted HTTP client, so
the database tests exercise the capture, replay and roll-up code production
runs.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_govfin.capture import capture_period
from data_ingestion_toolbox.census_govfin.config import SOURCE_CODE, GovFinConfig
from data_ingestion_toolbox.census_govfin.silver_census_govfin.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/census_govfin"
UNIT_YEAR = 2022

#: The fixture's figures for Dane County's local governments, in thousands
#: of dollars, summed by hand from ``finestdat_2022_excerpt.txt``: the county
#: (T01 150,000, T09 45,000), Madison (T01 250,000, T13 1,500) and the
#: Madison school district (T01 300,000).
DANE_TAXES_TOTAL = 746_500
DANE_GOVERNMENT_COUNT = 3

TRACKED_GEO_IDS: tuple[str, ...] = (
    "state:55",
    "state:55|county:025",
    "state:55|county:079",
)

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "55", "vintage": 2022, "name": "Wisconsin"},
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "025",
        "vintage": 2022,
        "name": "Dane County",
    },
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "079",
        "vintage": 2022,
        "name": "Milwaukee County",
    },
)

SILVER_TABLES: tuple[str, ...] = (
    "silver_census_govfin.fact_observation",
    "silver_census_govfin.api_revision",
    "silver_census_govfin.unit_amount",
    "silver_census_govfin.unit_government",
    "silver_census_govfin.state_type_total",
    "silver_census_govfin.quarantine",
    "silver_census_govfin.measure_item",
    "silver_census_govfin.dim_measure",
)


UNIT_MEMBERS: dict[str, str] = {
    "2022_Individual_Unit_files/2022FinEstDAT_07152026modp.txt": "finestdat_2022_excerpt.txt",
    "2022_Individual_Unit_files/Fin_PID_2022.txt": "fin_pid_2022_excerpt.txt",
    "2022_Individual_Unit_files/22statetypepu.txt": "statetypepu_2022_excerpt.txt",
}

UNIT_ARCHIVE_PATH = "/tables/2022/2022_Individual_Unit_File.zip"


def zipped(members: dict[str, bytes]) -> bytes:
    """A deterministic zip of the given members, beside the documentation Census ships."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        documentation = "2022_Individual_Unit_files/2022 S&L Public Use Files Technical Documentation.pdf"
        for name, payload in {documentation: b"%PDF-1.4 layout", **members}.items():
            bundle.writestr(
                zipfile.ZipInfo(name, date_time=(2026, 7, 15, 0, 0, 0)), payload
            )
    return buffer.getvalue()


def unit_bundle(overrides: dict[str, bytes] | None = None) -> bytes:
    """The 2022 edition's zip built from the fixture excerpts."""
    members = {
        name: (FIXTURE_DIR / fixture).read_bytes()
        for name, fixture in UNIT_MEMBERS.items()
    }
    members.update(overrides or {})
    return zipped(members)


def unit_archives() -> dict[str, bytes]:
    """The 2022 edition's zip, keyed by the path the registry names."""
    return {UNIT_ARCHIVE_PATH: unit_bundle()}


#: The real Census Data API answers read on OFFICE_LAB on 2026-11-05: the
#: nation and Wisconsin for govslocalfin 2022 and QTAX 2024.
API_FIXTURES: dict[str, str] = {
    "/timeseries/govslocalfin|us": "asslgf_2022_us.json",
    "/timeseries/govslocalfin|state": "asslgf_2022_state.json",
    "/timeseries/eits/qtax|us": "qtax_2024_us.json",
    "/timeseries/eits/qtax|state": "qtax_2024_state.json",
}


def api_responses() -> dict[str, bytes]:
    return {
        key: (FIXTURE_DIR / name).read_bytes() for key, name in API_FIXTURES.items()
    }


class FixtureClient:
    """Answers each registered path with its fixture bytes (or an override).

    A key is a URL path, optionally with ``|<grain>`` to match only the API
    request whose ``for`` clause asks for that grain.
    """

    def __init__(self, responses: dict[str, bytes | httpx.Response]) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        self.calls.append((url, dict(params) if params else None))
        request = httpx.Request("GET", url)
        grain = (params or {}).get("for", "").split(":", 1)[0]
        for key, answer in self.responses.items():
            path, _, wanted = key.partition("|")
            if url.endswith(path) and (not wanted or wanted == grain):
                if isinstance(answer, httpx.Response):
                    return httpx.Response(
                        answer.status_code, content=answer.content, request=request
                    )
                return httpx.Response(200, content=answer, request=request)
        return httpx.Response(404, request=request)

    def close(self) -> None:
        return None


def config() -> GovFinConfig:
    return GovFinConfig(
        census_api_key="integration-test-key", min_spacing_seconds=0, max_attempts=1
    )


def run_to_gold(
    connection_factory: Callable[[], connection],
    dataset_id: str,
    period: int,
    client: FixtureClient,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish one period; return (run, facts, slices published)."""
    run_id, _captured = capture_period(
        connection_factory, dataset_id, period, config=config(), client=client
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def _preexisting(connection_factory: Callable[[], connection]) -> set[str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(TRACKED_GEO_IDS),),
            )
            return {row[0] for row in cursor.fetchall()}
    finally:
        database_connection.close()


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


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
                cursor.execute("DELETE FROM control.census_govfin_slice")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(TRACKED_GEO_IDS) - preexisting):
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
    """Seed the fixture geographies and remove all government finance state afterwards."""
    preexisting = _preexisting(connection_factory)
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
            for arguments in _SEED_ARGUMENTS:
                if _canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory
