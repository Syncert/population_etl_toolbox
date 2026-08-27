"""Shared disposable-warehouse setup for USDA NASS database contracts."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from psycopg2.extensions import connection

from tests.support.capture_seed import delete_geography, seed_geography

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/usda_nass"

#: Geographies the reviewed USDA NASS fixtures resolve against.
TRACKED_GEO_IDS: tuple[str, ...] = (
    "us:1",
    "state:01",
    "state:48",
    "state:01|county:001",
    "state:48|county:301",
)

#: Silver relations a USDA NASS contract test must leave empty.
SILVER_TABLES: tuple[str, ...] = (
    "silver_nass.fact_crop_observation",
    "silver_nass.observation_revision",
    "silver_nass.observation_quarantine",
    "silver_nass.dim_dataset_release",
    "silver_nass.dim_statistic",
    "silver_nass.dim_commodity",
    "silver_nass.dim_domain",
)

#: The reviewed fixture geographies, in the order they must be seeded.
_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
    {"geo_type": "state", "state_fips": "01", "vintage": 2024, "name": "Alabama"},
    {"geo_type": "state", "state_fips": "48", "vintage": 2024, "name": "Texas"},
    {
        "geo_type": "county",
        "state_fips": "01",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Autauga County",
    },
    {
        "geo_type": "county",
        "state_fips": "48",
        "county_fips": "301",
        "vintage": 2024,
        "name": "Loving County",
    },
)


def load_product_fixture(name: str) -> dict[str, Any]:
    """Load one reviewed USDA NASS fixture document by stem."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8"))


def _preexisting_geographies(
    connection_factory: Callable[[], connection],
) -> set[str]:
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
    connection_factory: Callable[[], connection], preexisting: set[str]
) -> Callable[[], None]:
    def run() -> None:
        database_connection = connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event "
                    "WHERE source_code = 'USDA_NASS'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'USDA_NASS'"
                )
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.usda_nass_slice")
                cursor.execute("DELETE FROM control.usda_nass_release")
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
    """Seed the reviewed geographies and remove all USDA NASS state afterwards."""
    preexisting = _preexisting_geographies(connection_factory)
    request.addfinalizer(_cleanup(connection_factory, preexisting))

    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            for arguments in _SEED_ARGUMENTS:
                seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory
