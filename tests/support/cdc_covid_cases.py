"""Shared seeding and cleanup for CDC COVID-19 case count database tests.

The checked-in fixtures under ``tests/fixtures/cdc_covid_cases`` are CDC's
own grouped responses of data.cdc.gov ``n8mc-b4w4`` for two state-months
(Wisconsin 2021-11 and California 2022-01, the loader's exact request, read
on 2026-11-12), an index of those two slices, and the dataset's columns. A
scripted HTTP client answers the view document, the index and each slice by
its ``$where``, so every database test exercises the capture, replay and
publication code production runs. Only Wisconsin and its counties are
seeded, so California's records exercise the unplaced path.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc_covid_cases.capture import capture_snapshot
from data_ingestion_toolbox.cdc_covid_cases.config import (
    BASE_URL,
    METADATA_PATH,
    RESOURCE_PATH,
    SOURCE_CODE,
    CdcCovidCasesConfig,
)
from data_ingestion_toolbox.cdc_covid_cases.silver_cdc_covid_cases.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/cdc_covid_cases"

DANE = "state:55|county:025"

SLICES: dict[str, str] = {
    "res_state='WI' AND case_month='2021-11'": "n8mc_wi_2021-11.json",
    "res_state='CA' AND case_month='2022-01'": "n8mc_ca_2022-01.json",
}

SILVER_TABLES: tuple[str, ...] = (
    "silver_cdc_covid_cases.area_month",
    "silver_cdc_covid_cases.slice_group",
    "silver_cdc_covid_cases.quarantine",
    "silver_cdc_covid_cases.dim_measure",
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


class FixtureClient:
    """Answers the view document, the slice index and each slice by its ``$where``.

    ``overrides`` maps a ``$where`` text to the response served instead.
    """

    def __init__(self, overrides: dict[str, httpx.Response] | None = None) -> None:
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
                200, content=fixture_bytes("n8mc_view.json"), request=request
            )
        if url == f"{BASE_URL}{RESOURCE_PATH}":
            where = parameters.get("$where")
            if where is None:
                return httpx.Response(
                    200, content=fixture_bytes("n8mc_index.json"), request=request
                )
            if where in self.overrides:
                response = self.overrides[where]
                return httpx.Response(
                    response.status_code, content=response.content, request=request
                )
            if where in SLICES:
                return httpx.Response(
                    200, content=fixture_bytes(SLICES[where]), request=request
                )
        return httpx.Response(404, content=b"{}", request=request)

    def close(self) -> None:
        return None


def config() -> CdcCovidCasesConfig:
    return CdcCovidCasesConfig(min_spacing_seconds=0, max_attempts=1)


def load_snapshot(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
    conf: dict | None = None,
) -> tuple[UUID | None, int, int]:
    """Capture, replay and publish as the DAG does: (snapshot, records, published)."""
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


def cleanup_cdc_covid_cases(cursor) -> None:  # noqa: ANN001
    """Delete every CDC COVID-19 case count row (tables in foreign-key order)."""
    for table in SILVER_TABLES:
        cursor.execute(f"DELETE FROM {table}")
    cursor.execute("DELETE FROM control.cdc_covid_cases_slice")
    cursor.execute("DELETE FROM control.cdc_covid_cases_snapshot")


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
                cleanup_cdc_covid_cases(cursor)
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
    """Seed the nation, Wisconsin and its counties; remove all CDC case state afterwards."""
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
    "cleanup_cdc_covid_cases",
    "load_snapshot",
    "reviewed_warehouse",
]
