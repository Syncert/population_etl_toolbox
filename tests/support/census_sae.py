"""Shared seeding and cleanup for Census SAIPE/SAHIE database tests.

The checked-in fixtures under ``tests/fixtures/census_saipe_sahie`` are real
Census Data API responses. They are played through the adapter's own capture
path by a scripted HTTP client, so every database test exercises the capture,
replay and publication code production runs.
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

from data_ingestion_toolbox.census_saipe_sahie.capture import capture_dataset_year
from data_ingestion_toolbox.census_saipe_sahie.config import SOURCE_CODE, SaeConfig
from data_ingestion_toolbox.census_saipe_sahie.registry import GEO_LEVELS, SaeDataset
from data_ingestion_toolbox.census_saipe_sahie.silver_census_sae.load import (
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
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/census_saipe_sahie"
FIXTURE_YEAR = 2023

#: Geographies the fixtures resolve against: the nation, Delaware, and its
#: three counties. Every other state in the state fixtures stays ``unmapped``.
TRACKED_GEO_IDS: tuple[str, ...] = (
    "us:1",
    "state:10",
    "state:10|county:001",
    "state:10|county:003",
    "state:10|county:005",
)

SILVER_TABLES: tuple[str, ...] = (
    "silver_census_sae.fact_estimate",
    "silver_census_sae.observation_revision",
    "silver_census_sae.observation_quarantine",
    "silver_census_sae.dim_measure",
)

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2023, "name": "United States"},
    {"geo_type": "state", "state_fips": "10", "vintage": 2023, "name": "Delaware"},
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "001",
        "vintage": 2023,
        "name": "Kent County",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "003",
        "vintage": 2023,
        "name": "New Castle County",
    },
    {
        "geo_type": "county",
        "state_fips": "10",
        "county_fips": "005",
        "vintage": 2023,
        "name": "Sussex County",
    },
)


def fixture_bytes(dataset_id: str, geo_level: str) -> bytes:
    return (FIXTURE_DIR / f"{dataset_id}_{FIXTURE_YEAR}_{geo_level}.json").read_bytes()


def fixture_rows(dataset_id: str, geo_level: str) -> list[list[Any]]:
    return json.loads(fixture_bytes(dataset_id, geo_level))


class FixtureClient:
    """Answers each registered grain with its fixture (or the given override)."""

    def __init__(
        self, dataset: SaeDataset, overrides: dict[str, httpx.Response] | None = None
    ) -> None:
        self.dataset = dataset
        self.overrides = dict(overrides or {})
        self.calls: list[dict[str, str]] = []

    def get(self, url: str, *, params: dict[str, str]) -> httpx.Response:
        self.calls.append(dict(params))
        geo_level = params["for"].split(":", 1)[0]
        request = httpx.Request("GET", url)
        if geo_level in self.overrides:
            response = self.overrides[geo_level]
            return httpx.Response(
                response.status_code, content=response.content, request=request
            )
        return httpx.Response(
            200,
            content=fixture_bytes(self.dataset.dataset_id, geo_level),
            headers={"content-type": "application/json;charset=utf-8"},
            request=request,
        )

    def close(self) -> None:
        return None


def config() -> SaeConfig:
    return SaeConfig(
        census_api_key="integration-test-key", min_spacing_seconds=0, max_attempts=1
    )


def run_to_gold(
    connection_factory: Callable[[], connection],
    dataset: SaeDataset,
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish one fixture year; return (run, facts, slices published)."""
    run_id, _captured = capture_dataset_year(
        connection_factory,
        dataset,
        FIXTURE_YEAR,
        config=config(),
        client=client or FixtureClient(dataset),
    )
    facts = replay_run(connection_factory, run_id=run_id, dataset=dataset)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def _preexisting_geographies(connection_factory: Callable[[], connection]) -> set[str]:
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
                cursor.execute("DELETE FROM control.census_sae_slice")
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
    """Seed the fixture geographies and remove all SAIPE/SAHIE state afterwards."""
    preexisting = _preexisting_geographies(connection_factory)
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
                if canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory


def canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "nation":
        return "us:1"
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


__all__ = [
    "FIXTURE_YEAR",
    "FixtureClient",
    "GEO_LEVELS",
    "TRACKED_GEO_IDS",
    "fixture_bytes",
    "fixture_rows",
    "reviewed_warehouse",
    "run_to_gold",
]
