"""Shared seeding and cleanup for CDC FluView ILINet database tests.

The checked-in fixtures under ``tests/fixtures/cdc_ilinet`` are FluView's own
init document and its national, HHS-region and state ILINet downloads read on
2026-11-12, trimmed to a few weeks (see ``SOURCE_NOTES.md``). A scripted HTTP
client answers the init GET and each download POST by its ``RegionTypeId``,
so every database test exercises the capture, replay and publication code
production runs. Only the nation, Wisconsin and New York are seeded, so
California and Puerto Rico exercise the unmapped path.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc_ilinet.capture import capture_release
from data_ingestion_toolbox.cdc_ilinet.config import (
    BASE_URL,
    DOWNLOAD_PATH,
    INIT_PATH,
    SOURCE_CODE,
    CdcIlinetConfig,
)
from data_ingestion_toolbox.cdc_ilinet.silver_cdc_ilinet.load import (
    publish_release,
    replay_release,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/cdc_ilinet"

WISCONSIN = "state:55"
NEW_YORK = "state:36"
NATION = "us:1"

DOWNLOADS: dict[int, str] = {
    3: "ilinet_national.zip",
    1: "ilinet_hhs_region.zip",
    5: "ilinet_state.zip",
}

SILVER_TABLES: tuple[str, ...] = (
    "silver_cdc_ilinet.week_value",
    "silver_cdc_ilinet.quarantine",
    "silver_cdc_ilinet.region",
    "silver_cdc_ilinet.dim_measure",
)

_SEED: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
    {"geo_type": "state", "state_fips": "55", "vintage": 2024, "name": "Wisconsin"},
    {"geo_type": "state", "state_fips": "36", "vintage": 2024, "name": "New York"},
)
_SEEDED_IDS: tuple[str, ...] = (NATION, WISCONSIN, NEW_YORK)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers the init GET and each download POST by its ``RegionTypeId``.

    ``overrides`` maps a ``RegionTypeId`` to the response served instead.
    """

    def __init__(self, overrides: dict[int, httpx.Response] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[tuple[str, str, dict]] = []

    def get(
        self,
        url: str,
        *,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        self.calls.append(("GET", url, dict(params or {})))
        request = httpx.Request("GET", url)
        if url == f"{BASE_URL}{INIT_PATH}":
            return httpx.Response(
                200, content=fixture_bytes("fluview_init.json"), request=request
            )
        return httpx.Response(404, content=b"{}", request=request)

    def post(
        self,
        url: str,
        *,
        json: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        body = dict(json or {})
        self.calls.append(("POST", url, body))
        request = httpx.Request("POST", url)
        if url == f"{BASE_URL}{DOWNLOAD_PATH}":
            region_type_id = body.get("RegionTypeId")
            if region_type_id in self.overrides:
                response = self.overrides[region_type_id]
                return httpx.Response(
                    response.status_code, content=response.content, request=request
                )
            if region_type_id in DOWNLOADS:
                return httpx.Response(
                    200,
                    content=fixture_bytes(DOWNLOADS[region_type_id]),
                    request=request,
                )
        return httpx.Response(404, content=b"{}", request=request)

    def close(self) -> None:
        return None


def config() -> CdcIlinetConfig:
    return CdcIlinetConfig(min_spacing_seconds=0, max_attempts=1)


def load_release(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID | None, int, int]:
    """Capture, replay and publish as the DAG does: (release, changed, published)."""
    release_id = capture_release(
        connection_factory,
        config=config(),
        client=client or FixtureClient(),
        sleep=lambda _seconds: None,
    )
    if release_id is None:
        return None, 0, 0
    changed = replay_release(connection_factory, release_id=release_id)
    published = publish_release(connection_factory, release_id=release_id)
    return release_id, changed, published


def _preexisting(connection_factory: Callable[[], connection]) -> set[str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(_SEEDED_IDS),),
            )
            return {row[0] for row in cursor.fetchall()}
    finally:
        database_connection.close()


def cleanup_cdc_ilinet(cursor) -> None:  # noqa: ANN001
    """Delete every CDC ILINet row (tables in foreign-key order)."""
    for table in SILVER_TABLES:
        cursor.execute(f"DELETE FROM {table}")
    cursor.execute("DELETE FROM control.cdc_ilinet_download")
    cursor.execute("DELETE FROM control.cdc_ilinet_release")


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
                cleanup_cdc_ilinet(cursor)
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(_SEEDED_IDS) - preexisting):
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
    """Seed the nation, Wisconsin and New York; remove all ILINet state afterwards."""
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
            for arguments, geo_id in zip(_SEED, _SEEDED_IDS, strict=True):
                if geo_id not in preexisting:
                    seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory


__all__ = [
    "NATION",
    "NEW_YORK",
    "WISCONSIN",
    "FixtureClient",
    "cleanup_cdc_ilinet",
    "load_release",
    "reviewed_warehouse",
]
