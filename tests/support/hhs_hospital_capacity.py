"""Shared seeding and cleanup for HHS hospital capacity database tests.

The checked-in fixtures under ``tests/fixtures/hhs_hospital_capacity`` are
HHS's own rows of healthdata.gov ``anag-cw7u`` for the five Dane County
hospitals (1,044 hospital weeks, 2020-03-29 to 2024-04-21, read on
2026-11-12), cut to the registered ``$select`` columns, ordered by hospital
and week and split at offsets 0, 400 and 800 as a 400-row page size reads
them; ``anag_view.json`` is the dataset's view document. A scripted HTTP
client answers each page by its ``$offset`` and the view document, so every
database test exercises the capture, replay and publication code production
runs.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.hhs_hospital_capacity.capture import capture_snapshot
from data_ingestion_toolbox.hhs_hospital_capacity.config import (
    BASE_URL,
    METADATA_PATH,
    RESOURCE_PATH,
    SOURCE_CODE,
    HhsHospitalCapacityConfig,
)
from data_ingestion_toolbox.hhs_hospital_capacity.silver_hhs_hospital_capacity.load import (
    publish_snapshot,
    replay_snapshot,
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/hhs_hospital_capacity"

DANE = "state:55|county:025"
PAGE_SIZE = 400
PAGE_OFFSETS = (0, 400, 800)

SILVER_TABLES: tuple[str, ...] = (
    "silver_hhs_hospital_capacity.area_week",
    "silver_hhs_hospital_capacity.facility_week_value",
    "silver_hhs_hospital_capacity.facility",
    "silver_hhs_hospital_capacity.quarantine",
    "silver_hhs_hospital_capacity.dim_measure",
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def page_bytes(offset: int) -> bytes:
    return fixture_bytes(f"anag_dane_offset_{offset}.json")


def all_rows() -> list[dict]:
    """Every fixture row in page order, for a page size other than 400."""
    rows: list[dict] = []
    for offset in PAGE_OFFSETS:
        rows.extend(json.loads(page_bytes(offset)))
    return rows


class FixtureClient:
    """Answers the view document and each page by its ``$offset`` and ``$limit``.

    At the 400-row page size the fixture pages are served byte for byte; any
    other page size is served the same rows, re-sliced.
    """

    def __init__(self, overrides: dict[int, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[tuple[str, dict]] = []

    def get(
        self,
        url: str,
        *,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        parameters = dict(params or {})
        self.calls.append((url, parameters))
        request = httpx.Request("GET", url)
        if url == f"{BASE_URL}{METADATA_PATH}":
            return httpx.Response(
                200, content=fixture_bytes("anag_view.json"), request=request
            )
        if url == f"{BASE_URL}{RESOURCE_PATH}":
            offset = int(parameters["$offset"])
            if offset in self.overrides:
                response = self.overrides[offset]
                return httpx.Response(
                    response.status_code, content=response.content, request=request
                )
            limit = int(parameters.get("$limit", PAGE_SIZE))
            if limit == PAGE_SIZE and offset in PAGE_OFFSETS:
                return httpx.Response(200, content=page_bytes(offset), request=request)
            rows = all_rows()[offset : offset + limit]
            return httpx.Response(
                200, content=json.dumps(rows).encode(), request=request
            )
        return httpx.Response(404, content=b"{}", request=request)

    def close(self) -> None:
        return None


def config() -> HhsHospitalCapacityConfig:
    return HhsHospitalCapacityConfig(
        min_spacing_seconds=0, max_attempts=1, page_size=PAGE_SIZE
    )


def load_snapshot(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
    conf: dict | None = None,
) -> tuple[UUID | None, int, int]:
    """Capture, replay and publish as the DAG does: (snapshot, stored, published)."""
    snapshot_id = capture_snapshot(
        connection_factory,
        conf=conf,
        config=config(),
        client=client or FixtureClient(),
        sleep=lambda _seconds: None,
    )
    if snapshot_id is None:
        return None, 0, 0
    stored = replay_snapshot(connection_factory, snapshot_id=snapshot_id)
    published = publish_snapshot(connection_factory, snapshot_id=snapshot_id)
    return snapshot_id, stored, published


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
                cursor.execute("DELETE FROM control.hhs_hospital_capacity_page")
                cursor.execute("DELETE FROM control.hhs_hospital_capacity_snapshot")
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
    """Seed the nation, Wisconsin and its counties; remove all HHS state afterwards."""
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
    "FixtureClient",
    "load_snapshot",
    "page_bytes",
    "reviewed_warehouse",
]
