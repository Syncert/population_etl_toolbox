"""Real PostgreSQL CDC capture-to-gold deployment contract."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release
from data_ingestion_toolbox.cdc.registry import CDI_ASSET, PLACES_COUNTY_ASSET
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import transform_release
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.cdc_release import persist_fixture_release

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_cdc_fixtures_replay_reconcile_and_publish_idempotently(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> None:
    """Covers: ARC-002, DB-003 — CDC releases reach gold without loss."""
    tracked_geo_ids = {
        "us:1",
        "state:01",
        "state:01|county:001",
        "state:48|county:301",
    }
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids),),
            )
            preexisting_geo_ids = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()

    def cleanup() -> None:
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event "
                    "WHERE source_code = 'CDC'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'CDC'"
                )
                cursor.execute("DELETE FROM silver_cdc.fact_health_observation")
                cursor.execute("DELETE FROM silver_cdc.observation_revision")
                cursor.execute("DELETE FROM silver_cdc.observation_quarantine")
                cursor.execute("DELETE FROM silver_cdc.dim_measure")
                cursor.execute("DELETE FROM silver_cdc.dim_stratum")
                cursor.execute("DELETE FROM silver_cdc.dim_dataset_release")
                cursor.execute("DELETE FROM control.cdc_dataset_release")
                for geo_id in sorted(tracked_geo_ids - preexisting_geo_ids):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor, geo_type="nation", vintage=2020, name="United States"
            )
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="01",
                vintage=2020,
                name="Alabama",
            )
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="01",
                county_fips="001",
                vintage=2020,
                name="Autauga County",
            )
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="48",
                county_fips="301",
                vintage=2020,
                name="Loving County",
            )
        writer.commit()
    finally:
        writer.close()

    fixtures = (
        (CDI_ASSET, "cdi_metadata.json", "cdi_observations.json"),
        (
            PLACES_COUNTY_ASSET,
            "places_county_metadata.json",
            "places_county_observations.json",
        ),
    )
    for asset, metadata_name, observations_name in fixtures:
        release = persist_fixture_release(
            postgres_connection_factory,
            asset=asset,
            metadata_name=metadata_name,
            observations_name=observations_name,
        )
        result = replay_captured_run(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
        )
        persist_replay_result(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
            result=result,
        )
        persist_replay_result(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
            result=result,
        )
        transformed = transform_release(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
        )
        published = publish_release(
            postgres_connection_factory,
            run_id=release.run_id,
            asset_id=asset.asset_id,
            release_watermark=release.metadata.release_version,
        )
        assert transformed == result.input_count
        assert published == result.input_count

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT asset_id, COUNT(*),
                       COUNT(*) FILTER (WHERE value_status = 'suppressed'),
                       COUNT(*) FILTER (WHERE geography_status = 'resolved')
                FROM gold_cdc.health_observation
                GROUP BY asset_id
                ORDER BY asset_id
                """
            )
            assert cursor.fetchall() == [
                ("cdi", 3, 0, 3),
                ("places_county", 4, 1, 4),
            ]
            cursor.execute(
                """
                SELECT DISTINCT estimate_method, population_basis
                FROM gold_cdc.health_observation
                WHERE asset_id = 'places_county'
                """
            )
            assert cursor.fetchone() == (
                "model_based_small_area_estimate",
                "adults age 18 years and older",
            )
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM control.publisher_ready_event
                WHERE source_code = 'CDC'
                """
            )
            assert cursor.fetchone() == (2,)
    finally:
        reader.close()
