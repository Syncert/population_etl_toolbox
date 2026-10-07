"""Shared seeding and cleanup for EIA retail gasoline database tests.

The checked-in answers under ``tests/fixtures/eia`` are EIA API v2 bytes
copied verbatim: two weeks of every registered grade for every area, and the
route's area facet. They are played through the adapter's own capture path
by a scripted HTTP client; the key the adapter sends is a fixture value.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.eia.capture import capture_window
from data_ingestion_toolbox.eia.config import SOURCE_CODE, EiaConfig
from data_ingestion_toolbox.eia.silver_eia.load import publish_run, replay_run
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
)
from data_ingestion_toolbox.silver_ref.provider_areas import (
    parse_eia_areas,
    provider_area_records,
)
from tests.support.capture_seed import delete_geography, seed_capture
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/eia"
WINDOW_FIXTURE = FIXTURE_DIR / "weekly_2026-08-31_2026-09-07.json"
FACET_FIXTURE = FIXTURE_DIR / "duoarea_facet.json"
WINDOW = (date(2026, 8, 31), date(2026, 9, 7))
FIXTURE_KEY = "fixture-eia-key"

#: The nation and two of EIA's nine states, with the USPS code the Census
#: Gazetteer gives them; the other seven are left unresolved on purpose.
STATES = (
    GeographyRecord("nation", "us:1", "1", None, None, None, "United States", 2024),
    GeographyRecord(
        "state", "state:06", "06", "06", None, None, "California", 2024, usps="CA"
    ),
    GeographyRecord(
        "state", "state:48", "48", "48", None, None, "Texas", 2024, usps="TX"
    ),
)

SILVER_TABLES = (
    "silver_eia.fact_retail_price",
    "silver_eia.price_revision",
    "silver_eia.observation_quarantine",
)


def fixture_config(**overrides: Any) -> EiaConfig:
    return EiaConfig(
        eia_api_key=FIXTURE_KEY, min_spacing_seconds=0, max_attempts=1, **overrides
    )


class FixtureClient:
    """Answers the data route with the window fixture, or a given override."""

    def __init__(self, override: httpx.Response | None = None) -> None:
        self.override = override
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, *, params: Any, headers: dict[str, str]) -> httpx.Response:
        self.calls.append({"url": url, "params": list(params)})
        request = httpx.Request("GET", url)
        if self.override is not None:
            return httpx.Response(
                self.override.status_code, content=self.override.content, request=request
            )
        return httpx.Response(
            200,
            content=WINDOW_FIXTURE.read_bytes(),
            headers={"content-type": "application/json"},
            request=request,
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, int, int]:
    """Capture, replay and publish the fixture window; return (run, facts, published)."""
    run_id = capture_window(
        connection_factory,
        *WINDOW,
        config=fixture_config(),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, facts, published


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the nation, two states and EIA's areas; remove all EIA state afterwards."""
    areas = provider_area_records(
        "eia", parse_eia_areas(FACET_FIXTURE.read_bytes()), vintage=2026
    )
    seeded = [*STATES, *areas]
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                ([record.geo_id for record in seeded],),
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
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.eia_page")
                cursor.execute("DELETE FROM control.eia_read")
                delete_capture_graph(cursor, owned_runs)
                for record in seeded:
                    if record.geo_id not in preexisting:
                        delete_geography(cursor, record.geo_id)
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
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()
    finally:
        writer.close()
    GeographyRepository(connection_factory).load_attributes(
        [record for record in seeded if record.geo_id not in preexisting],
        capture_id=capture_id,
    )
    return connection_factory
