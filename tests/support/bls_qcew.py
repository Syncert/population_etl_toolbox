"""Shared seeding and cleanup for BLS QCEW database tests.

The checked-in fixtures under ``tests/fixtures/bls_qcew`` are rows copied
verbatim from published open-data slices. They are played through the
adapter's own capture path by a scripted HTTP client, so every database test
exercises the capture, replay and publication code production runs.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls_qcew.capture import capture_period
from data_ingestion_toolbox.bls_qcew.config import SOURCE_CODE, QcewConfig
from data_ingestion_toolbox.bls_qcew.registry import QcewIndustry
from data_ingestion_toolbox.bls_qcew.silver_bls_qcew.load import publish_run, replay_run
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/bls_qcew"

#: Delaware, its three counties and the nation. Autauga County, Alabama
#: (`01001`) is in the fixtures and deliberately not seeded: it stays
#: `unmapped`, explicit rather than dropped.
TRACKED_GEO_IDS: tuple[str, ...] = (
    "us:1",
    "state:10",
    "state:10|county:001",
    "state:10|county:003",
    "state:10|county:005",
)

SILVER_TABLES: tuple[str, ...] = (
    "silver_bls_qcew.fact_observation",
    "silver_bls_qcew.observation_revision",
    "silver_bls_qcew.observation_quarantine",
)

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2024, "name": "Delaware"},
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
)

_PATH = re.compile(r"/(\d{4})/([1-4a])/industry/([0-9_]+)\.csv$")


def fixture_bytes(year: int, period: str, industry_code: str) -> bytes | None:
    path = (
        FIXTURE_DIR / f"{year}_{period}_industry_{industry_code.replace('-', '_')}.csv"
    )
    return path.read_bytes() if path.is_file() else None


class FixtureClient:
    """Answers each slice with its fixture, a given override, or 404."""

    def __init__(
        self, overrides: dict[tuple[int, str, str], httpx.Response] | None = None
    ) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        match = _PATH.search(url)
        assert match, url
        key = (int(match.group(1)), match.group(2), match.group(3).replace("_", "-"))
        request = httpx.Request("GET", url)
        if key in self.overrides:
            override = self.overrides[key]
            return httpx.Response(
                override.status_code, content=override.content, request=request
            )
        payload = fixture_bytes(*key)
        if payload is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200, content=payload, headers={"content-type": "text/csv"}, request=request
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    year: int,
    period: str,
    industries: Sequence[QcewIndustry],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish one period; return (run, facts, slices published)."""
    run_id, _slices = capture_period(
        connection_factory,
        year,
        period,
        industries=industries,
        config=QcewConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
        sleep=lambda _: None,
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
    if arguments["geo_type"] == "nation":
        return "us:1"
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture geographies and remove all QCEW state afterwards."""
    preexisting = _preexisting(connection_factory)
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
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.bls_qcew_slice")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(TRACKED_GEO_IDS) - preexisting):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
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
