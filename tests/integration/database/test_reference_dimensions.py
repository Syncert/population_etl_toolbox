"""Versioned reference replay, geometry, relationship, and serving contracts."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import date

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref import time_dim
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
    GeometryRecord,
)
from tests.support.capture_seed import seed_capture
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

TEST_IDS = [
    "us:1",
    "state:98",
    "state:98|county:764",
    "state:98|county:765",
    "state:98|place:54321",
]


@pytest.fixture
def reference_dimension_scope(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[None]:
    try:
        yield
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution WHERE geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.bridge_geo_relationship_version WHERE parent_geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)) "
                    "OR related_geo_sk IN (SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS, TEST_IDS),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_geometry_version WHERE geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_entity_version WHERE geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01'"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_time_dimension_sync_replays_exact_leap_window(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: ETL-024, ETL-025 — time replay is exact and idempotent."""
    monkeypatch.setattr(
        time_dim, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    for _ in range(2):
        assert time_dim.sync_time_dim(date(2096, 2, 28), date(2096, 3, 1)) == 3
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """SELECT date_key::TEXT, is_month_start, is_month_end, day_of_week, quarter
                   FROM silver_ref.dim_time
                   WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01' ORDER BY date_key"""
            )
            assert cursor.fetchall() == [
                ("2096-02-28", False, False, 2, 1),
                ("2096-02-29", False, True, 3, 1),
                ("2096-03-01", True, False, 4, 1),
            ]
    finally:
        reader.close()


def _polygon(x1: float, x2: float) -> str:
    return json.dumps(
        {
            "type": "Polygon",
            "coordinates": [[[x1, 40], [x2, 40], [x2, 41], [x1, 41], [x1, 40]]],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_geography_replay_retains_versions_and_cross_county_place_relationships(
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: DB-018 — versions, retirement, geometry, and intersections persist."""
    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_2096 = seed_capture(cursor, "CENSUS_GEO", b"snapshot-2096")
    writer.commit()
    writer.close()

    repository = GeographyRepository(postgres_connection_factory)
    records = [
        GeographyRecord("nation", "us:1", "1", None, None, None, "United States", 2096),
        GeographyRecord(
            "state", "state:98", "98", "98", None, None, "Old State Name", 2096
        ),
        GeographyRecord(
            "county",
            "state:98|county:764",
            "98764",
            "98",
            "764",
            None,
            "West County",
            2096,
        ),
        GeographyRecord(
            "county",
            "state:98|county:765",
            "98765",
            "98",
            "765",
            None,
            "East County",
            2096,
        ),
        GeographyRecord(
            "place",
            "state:98|place:54321",
            "9854321",
            "98",
            None,
            "54321",
            "Crossing Place",
            2096,
        ),
    ]
    assert repository.load_attributes(records, capture_id=capture_2096) == 5
    geometries = [
        GeometryRecord("state:98", 2096, _polygon(-91, -88)),
        GeometryRecord("state:98|county:764", 2096, _polygon(-91, -89.5)),
        GeometryRecord("state:98|county:765", 2096, _polygon(-89.5, -88)),
        GeometryRecord("state:98|place:54321", 2096, _polygon(-90, -89)),
    ]
    assert repository.load_geometries(geometries, capture_id=capture_2096) == 4
    repository.reconcile_relationships(vintage=2096, capture_id=capture_2096)

    serving_reader = postgres_connection_factory()
    try:
        with serving_reader.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            cursor.execute(
                """SELECT place_fips, place_name, boundary_vintage,
                          ST_IsValid(geo_geom), ST_SRID(geo_geom)
                   FROM gold.dim_geo_latest
                   WHERE geo_id = 'state:98|place:54321'"""
            )
            assert cursor.fetchone() == (
                "54321",
                "Crossing Place",
                2096,
                True,
                4326,
            )
        serving_reader.rollback()
    finally:
        serving_reader.close()

    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_2097 = seed_capture(cursor, "CENSUS_GEO", b"snapshot-2097")
    writer.commit()
    writer.close()
    renamed = GeographyRecord(
        "state", "state:98", "98", "98", None, None, "New State Name", 2097
    )
    repository.load_attributes([renamed], capture_id=capture_2097)
    repository.retire_missing(
        active_geo_ids={"state:98"}, vintage=2097, capture_id=capture_2097
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT name, is_active FROM silver_ref.dim_geo_current WHERE geo_id = 'state:98'"
            )
            assert cursor.fetchone() == ("New State Name", True)
            cursor.execute(
                "SELECT name FROM silver_ref.dim_geo_entity_version v JOIN silver_ref.dim_geo_entity e USING (geo_sk) "
                "WHERE e.geo_id = 'state:98' ORDER BY geography_vintage"
            )
            assert cursor.fetchall() == [("Old State Name",), ("New State Name",)]
            cursor.execute(
                "SELECT is_active FROM silver_ref.dim_geo_current WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone() == (False,)
            cursor.execute(
                """SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version r
                   JOIN silver_ref.dim_geo_entity p ON p.geo_sk = r.related_geo_sk
                   WHERE p.geo_id = 'state:98|place:54321' AND r.relationship_type = 'intersects'"""
            )
            assert cursor.fetchone() == (2,)
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            cursor.execute(
                "SELECT state_name, ST_IsValid(geo_geom), ST_SRID(geo_geom) FROM gold.dim_geo_latest "
                "WHERE geo_id = 'state:98'"
            )
            assert cursor.fetchone() == ("New State Name", True, 4326)
        reader.rollback()
    finally:
        reader.close()
