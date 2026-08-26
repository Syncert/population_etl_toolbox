"""Real PostgreSQL CDC capture-to-gold deployment contract."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.cdc.capture import (
    CapturedCdcRelease,
    persist_release_state,
)
from data_ingestion_toolbox.cdc.client import page_parameters
from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release
from data_ingestion_toolbox.cdc.metadata import MetadataDecision, parse_metadata
from data_ingestion_toolbox.cdc.registry import CDI_ASSET, PLACES_COUNTY_ASSET, CdcAsset
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import transform_release
from tests.support.capture_seed import delete_geography, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "cdc"


def _persist_fixture_release(
    connection_factory: Callable[[], connection],
    *,
    asset: CdcAsset,
    metadata_name: str,
    observations_name: str,
) -> CapturedCdcRelease:
    metadata_payload = (FIXTURE_DIR / metadata_name).read_bytes()
    observations_payload = (FIXTURE_DIR / observations_name).read_bytes()
    metadata = parse_metadata(metadata_payload, asset)
    control = CaptureControl(connection_factory, source_code="CDC")
    run_id = control.start_run(watermark={"asset_id": asset.asset_id})

    metadata_request = control.start_request(
        run_id=run_id,
        endpoint=asset.metadata_path,
        parameters={},
    )
    metadata_capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=metadata_request.request_id,
        run_id=run_id,
        source_code="CDC",
        endpoint=asset.metadata_path,
        request_parameters={},
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=metadata_payload,
        payload_schema_version=asset.parser_contract_version,
    )
    persist_response_capture(connection_factory, metadata_capture)
    control.finish_request(metadata_request.request_id, status="captured")

    parameters = page_parameters(asset, page_size=100, offset=0)
    page_request = control.start_request(
        run_id=run_id,
        endpoint=asset.api_path,
        parameters=parameters,
    )
    page_capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=page_request.request_id,
        run_id=run_id,
        source_code="CDC",
        endpoint=asset.api_path,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=observations_payload,
        payload_schema_version=asset.parser_contract_version,
        source_revision=metadata.release_version,
    )
    persist_response_capture(connection_factory, page_capture)
    control.finish_request(page_request.request_id, status="captured")
    control.set_run_watermark(
        run_id,
        watermark={
            "asset_id": asset.asset_id,
            "release_watermark": metadata.watermark,
        },
    )
    control.finish_run(run_id, status="success")

    release = CapturedCdcRelease(
        run_id=run_id,
        asset_id=asset.asset_id,
        metadata_capture_id=metadata_capture.capture_id,
        page_capture_ids=(page_capture.capture_id,),
        metadata=metadata,
        decision=MetadataDecision.INGEST,
        row_count=3 if asset.asset_id == "cdi" else 4,
        complete=True,
    )
    persist_release_state(connection_factory, release)
    return release


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
        release = _persist_fixture_release(
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
