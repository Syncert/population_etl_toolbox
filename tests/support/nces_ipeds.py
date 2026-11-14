"""Shared seeding and cleanup for NCES IPEDS database tests.

The checked-in fixtures under ``tests/fixtures/nces_ipeds`` are cut from the
complete data files NCES serves: the 2023 and 2024 zips keep each CSV's
header and its Wisconsin rows, line for line, under the member names NCES
uses (IC2023 holds both ``IC2023.csv`` and ``IC2023_RV.csv``); the 2012 zips
keep the header and the University of Wisconsin-Madison row as the
2026-11-12 probe wrote them (UTF-8 without a byte-order mark, ``\\n`` line
ends). A scripted HTTP client plays them through the adapter's own capture
path and answers 404 for every other file, so every database test exercises
the capture, replay and publication code production runs.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.nces_ipeds.capture import capture_file
from data_ingestion_toolbox.nces_ipeds.config import (
    DATA_BASE_URL,
    SOURCE_CODE,
    NcesIpedsConfig,
)
from data_ingestion_toolbox.nces_ipeds.plan import answered, files_to_capture
from data_ingestion_toolbox.nces_ipeds.silver_nces_ipeds.load import (
    publish_run,
    replay_run,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.cms_ltc import _seed_arguments, canonical, tracked_geo_ids
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/nces_ipeds"

DANE = "state:55|county:025"
UW_MADISON = "240444"

#: The file NCES serves -> the fixture holding its cut.
FIXTURE_FILES: dict[str, str] = {
    "HD2012.zip": "HD2012_uw.zip",
    "IC2012.zip": "IC2012_uw.zip",
    "EF2012A.zip": "EF2012A_uw.zip",
    "HD2023.zip": "HD2023_wi.zip",
    "IC2023.zip": "IC2023_wi.zip",
    "EF2023A.zip": "EF2023A_wi.zip",
    "HD2024.zip": "HD2024_wi.zip",
    "IC2024.zip": "IC2024_wi.zip",
}

SILVER_TABLES: tuple[str, ...] = (
    "silver_nces_ipeds.area_total",
    "silver_nces_ipeds.institution_value",
    "silver_nces_ipeds.institution",
    "silver_nces_ipeds.quarantine",
    "silver_nces_ipeds.dim_measure",
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers each fixture file; anything else is a 404."""

    def __init__(self, overrides: dict[str, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str] | None = None) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        if url in self.overrides:
            response = self.overrides[url]
            return httpx.Response(
                response.status_code, content=response.content, request=request
            )
        name = url.removeprefix(DATA_BASE_URL)
        if name in FIXTURE_FILES:
            return httpx.Response(
                200,
                content=fixture_bytes(FIXTURE_FILES[name]),
                headers={"content-type": "application/x-zip-compressed"},
                request=request,
            )
        return httpx.Response(404, content=b"<html>not found</html>", request=request)

    def close(self) -> None:
        return None


def config() -> NcesIpedsConfig:
    return NcesIpedsConfig(min_spacing_seconds=0, max_attempts=1)


def load_files(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
    conf: dict | None = None,
) -> list[tuple[str, UUID, str, int, int]]:
    """Plan, capture, replay and publish as the DAG does.

    Returns, per file read, (file stem, run, status, rows stored, published).
    """
    active = client or FixtureClient()
    database_connection = connection_factory()
    try:
        done = answered(database_connection)
    finally:
        database_connection.close()
    results = []
    for component, year in files_to_capture(done, conf):
        run_id, status = capture_file(
            connection_factory, component, year, config=config(), client=active
        )
        stored = replay_run(connection_factory, run_id=run_id)
        published = publish_run(connection_factory, run_id=run_id)
        results.append((component.file_stem(year), run_id, status, stored, published))
    return results


def _preexisting(connection_factory: Callable[[], connection]) -> set[str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids()),),
            )
            return {row[0] for row in cursor.fetchall()}
    finally:
        database_connection.close()


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
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.nces_ipeds_file")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(tracked_geo_ids()) - preexisting):
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
    """Seed the nation, Wisconsin and its counties; remove all IPEDS state afterwards."""
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
            for arguments in _seed_arguments():
                if canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory


__all__ = [
    "DANE",
    "FIXTURE_FILES",
    "UW_MADISON",
    "FixtureClient",
    "fixture_bytes",
    "load_files",
    "reviewed_warehouse",
]
