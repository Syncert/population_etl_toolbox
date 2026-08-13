"""Production reference-dimension replay, geometry, index, and serving contracts."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date

import polars as pl
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref import geography, time_dim
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

TEST_STATE_FIPS = "98"
TEST_COUNTY_FIPS = "765"
TEST_STATE_GEO_ID = f"state:{TEST_STATE_FIPS}"
TEST_COUNTY_GEO_ID = f"state:{TEST_STATE_FIPS}|county:{TEST_COUNTY_FIPS}"


@pytest.fixture
def reference_dimension_scope(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[None]:
    """Own committed production-loader rows and verify teardown state."""
    try:
        yield
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo WHERE geo_id = ANY(%s)",
                    ([TEST_STATE_GEO_ID, TEST_COUNTY_GEO_ID],),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time "
                    "WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01'"
                )
            cleanup.commit()
            with cleanup.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM silver_ref.dim_geo WHERE geo_id = ANY(%s))
                      +
                      (SELECT COUNT(*) FROM silver_ref.dim_time
                       WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01')
                    """,
                    ([TEST_STATE_GEO_ID, TEST_COUNTY_GEO_ID],),
                )
                assert cursor.fetchone() == (0,)
        finally:
            cleanup.rollback()
            cleanup.close()


def test_time_dimension_sync_replays_exact_leap_window(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: ETL-024, ETL-025 — real dim_time upsert replays exactly."""
    monkeypatch.setattr(
        time_dim,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )

    for _ in range(2):
        assert time_dim.sync_time_dim(date(2096, 2, 28), date(2096, 3, 1)) == 3

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT date_key::TEXT, is_month_start, is_month_end,
                       day_of_week, quarter
                FROM silver_ref.dim_time
                WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01'
                ORDER BY date_key
                """
            )
            assert cursor.fetchall() == [
                ("2096-02-28", False, False, 2, 1),
                ("2096-02-29", False, True, 3, 1),
                ("2096-03-01", True, False, 4, 1),
            ]
    finally:
        reader.close()


def test_geography_sync_replays_valid_polygons_and_refreshes_serving_view(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: ETL-024, DB-018 — production geography replay serves valid PostGIS."""
    states = pl.DataFrame(
        {
            "GEOID": [TEST_STATE_FIPS],
            "NAME": ["Test State"],
            "INTPTLAT": ["40.0"],
            "INTPTLONG": ["-90.0"],
        }
    )
    counties = pl.DataFrame(
        {
            "GEOID": [TEST_STATE_FIPS + TEST_COUNTY_FIPS],
            "NAME": ["Replay County"],
            "INTPTLAT": ["40.1"],
            "INTPTLONG": ["-89.9"],
        }
    )
    state_polygon = {
        "type": "Polygon",
        "coordinates": [[[-91, 39], [-89, 39], [-89, 41], [-91, 41], [-91, 39]]],
    }
    county_polygon = {
        "type": "Polygon",
        "coordinates": [
            [[-90.1, 40.0], [-89.8, 40.0], [-89.8, 40.2], [-90.1, 40.2], [-90.1, 40.0]]
        ],
    }

    monkeypatch.setattr(
        geography,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    monkeypatch.setattr(geography, "_url_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        geography,
        "_fetch_zipped_tsv",
        lambda url: counties if "counties" in url else states,
    )
    monkeypatch.setattr(
        geography,
        "_load_polygon_lookup",
        lambda: (
            {TEST_STATE_FIPS: __import__("json").dumps(state_polygon)},
            {
                TEST_STATE_FIPS + TEST_COUNTY_FIPS: __import__("json").dumps(
                    county_polygon
                )
            },
        ),
    )

    for _ in range(2):
        assert geography.sync_geo_dim(source_year=2097, min_year=2097) == 3

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT geo_id, state_name, county_name, first_seen_year,
                       last_seen_year, ST_IsValid(geom), ST_SRID(geom)
                FROM silver_ref.dim_geo
                WHERE geo_id = ANY(%s) ORDER BY geo_id
                """,
                ([TEST_STATE_GEO_ID, TEST_COUNTY_GEO_ID],),
            )
            assert cursor.fetchall() == [
                (
                    TEST_STATE_GEO_ID,
                    "Test State",
                    None,
                    2097,
                    2097,
                    True,
                    4326,
                ),
                (
                    TEST_COUNTY_GEO_ID,
                    "Test State",
                    "Replay County",
                    2097,
                    2097,
                    True,
                    4326,
                ),
            ]
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            cursor.execute(
                """
                SELECT geo_id, ST_IsValid(geo_geom), ST_SRID(geo_geom)
                FROM gold.dim_geo_latest
                WHERE geo_id = %s
                """,
                (TEST_COUNTY_GEO_ID,),
            )
            assert cursor.fetchone() == (TEST_COUNTY_GEO_ID, True, 4326)
            cursor.execute(
                """
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = 'silver_ref' AND tablename = 'dim_geo'
                  AND indexdef ILIKE '%USING gist%geom%'
                """
            )
            assert cursor.fetchone() is not None
        reader.rollback()
    finally:
        reader.close()
