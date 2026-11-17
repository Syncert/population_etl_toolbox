"""Shared seeding and cleanup for vehicle registrations database tests.

The checked-in fixtures under ``tests/fixtures/vehicle_registrations`` are
WisDOT's and FHWA's own files read on 2026-11-17 (see ``SOURCE_NOTES.md``). A
scripted HTTP client answers each GET by its URL: the two WisDOT index
pages, the 2025 Report 28 (cut to Dane County and the statewide tables) and
the 2023 and 2024 MV-1 workbooks; any other MV-1 year answers 404, as FHWA
does for a year it has not posted. Every database test exercises the
capture, replay and publication code production runs.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.vehicle_registrations.capture import capture_release
from data_ingestion_toolbox.vehicle_registrations.config import (
    FHWA_BASE_URL,
    FHWA_MV1_PATH,
    SOURCE_CODE,
    WISDOT_BASE_URL,
    WISDOT_INDEX_PATHS,
    VehicleRegistrationsConfig,
)
from data_ingestion_toolbox.vehicle_registrations.silver_vehicle_registrations.load import (
    publish_release,
    replay_release,
)
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/vehicle_registrations"

RPT28_PATH = "/Documents/dmv/shared/rpt-28-cal-25.pdf"

FILES: dict[str, str] = {
    f"{WISDOT_BASE_URL}{WISDOT_INDEX_PATHS[0]}": "veh-info.html",
    f"{WISDOT_BASE_URL}{WISDOT_INDEX_PATHS[1]}": "veh-info-prev.html",
    f"{WISDOT_BASE_URL}{RPT28_PATH}": "rpt-28-cal-25-dane-and-state.pdf",
    f"{FHWA_BASE_URL}{FHWA_MV1_PATH.format(year=2023)}": "mv1-2023.xlsx",
    f"{FHWA_BASE_URL}{FHWA_MV1_PATH.format(year=2024)}": "mv1-2024.xlsx",
}

SILVER_TABLES: tuple[str, ...] = (
    "silver_vehicle_registrations.wisdot_count",
    "silver_vehicle_registrations.fhwa_mv1_figure",
    "silver_vehicle_registrations.quarantine",
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers each GET by its URL; ``overrides`` maps a URL to bytes served instead."""

    def __init__(self, overrides: dict[str, bytes] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str] | None = None) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        if url in self.overrides:
            return httpx.Response(200, content=self.overrides[url], request=request)
        if url in FILES:
            return httpx.Response(
                200, content=fixture_bytes(FILES[url]), request=request
            )
        return httpx.Response(404, content=b"<html>not found</html>", request=request)

    def close(self) -> None:
        return None


def config() -> VehicleRegistrationsConfig:
    return VehicleRegistrationsConfig(
        min_spacing_seconds=0, max_attempts=1, fhwa_first_year=2023
    )


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
        today=lambda: datetime(2025, 6, 1, tzinfo=UTC),
    )
    if release_id is None:
        return None, 0, 0
    changed = replay_release(connection_factory, release_id=release_id)
    published = publish_release(connection_factory, release_id=release_id)
    return release_id, changed, published


def cleanup_vehicle_registrations(cursor) -> None:  # noqa: ANN001
    """Delete every vehicle registrations row (tables in foreign-key order)."""
    for table in SILVER_TABLES:
        cursor.execute(f"DELETE FROM {table}")
    cursor.execute("DELETE FROM control.vehicle_registrations_document")
    cursor.execute("DELETE FROM control.vehicle_registrations_release")


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Remove all vehicle registrations state, and its harvested catalog, after the test."""
    baseline = source_run_ids(connection_factory, SOURCE_CODE)
    preexisting_glossary = glossary_registration_exists(connection_factory, SOURCE_CODE)

    def run() -> None:
        owned = sorted(source_run_ids(connection_factory, SOURCE_CODE) - baseline)
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
                cleanup_vehicle_registrations(cursor)
                delete_capture_graph(cursor, owned)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(run)
    return connection_factory


__all__ = [
    "FixtureClient",
    "cleanup_vehicle_registrations",
    "fixture_bytes",
    "load_release",
    "reviewed_warehouse",
]
