"""Real PostgreSQL capture-to-replay contract for Census PEP."""

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
from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.census_pep.silver_pep.replay import replay_pep_capture
from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "census_pep" / "nst_2025.csv"
)


def test_pep_fixture_capture_replays_idempotently(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ARC-002, DB-003 — PEP replay stays capture scoped and rerunnable."""
    release = next(
        item
        for item in CONFIG.releases
        if item.dataset_code == "pep_nst_alldata" and item.vintage_year == 2025
    )
    control = CaptureControl(
        postgres_connection_factory,
        source_code=CONFIG.source_code,
    )
    run_id = control.start_run(watermark={"product_code": release.product_code})
    parameters = {
        "dataset_code": release.dataset_code,
        "vintage_year": release.vintage_year,
        "product_code": release.product_code,
    }
    request = control.start_request(
        run_id=run_id,
        endpoint=release.data_url,
        parameters=parameters,
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=CONFIG.source_code,
        endpoint=release.data_url,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "text/csv"},
        media_type=release.media_type,
        payload=FIXTURE.read_bytes(),
        payload_schema_version=release.schema_version,
        source_revision=release.product_code,
    )
    persist_response_capture(postgres_connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")

    first_count = replay_pep_capture(
        postgres_connection_factory,
        capture_id=capture.capture_id,
        release=release,
    )
    second_count = replay_pep_capture(
        postgres_connection_factory,
        capture_id=capture.capture_id,
        release=release,
    )

    assert first_count > 0
    assert second_count == 0
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT release_vintage, observation_year, value
                FROM silver_pep.observation_revision
                WHERE capture_id = %s
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2024
                """,
                (capture.capture_id,),
            )
            assert cursor.fetchone() == (2025, 2024, 340003797)
    finally:
        reader.close()


def _capture_fixture(
    connection_factory: Callable[[], connection],
    *,
    dataset_code: str,
    vintage_year: int,
    fixture_name: str,
) -> ResponseCapture:
    release = next(
        item
        for item in CONFIG.releases
        if item.dataset_code == dataset_code and item.vintage_year == vintage_year
    )
    control = CaptureControl(connection_factory, source_code=CONFIG.source_code)
    run_id = control.start_run(watermark={"product_code": release.product_code})
    parameters = {
        "dataset_code": dataset_code,
        "vintage_year": vintage_year,
        "product_code": release.product_code,
    }
    request = control.start_request(
        run_id=run_id,
        endpoint=release.data_url,
        parameters=parameters,
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=CONFIG.source_code,
        endpoint=release.data_url,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "text/csv"},
        media_type=release.media_type,
        payload=(FIXTURE.parent / fixture_name).read_bytes(),
        payload_schema_version=release.schema_version,
        source_revision=release.product_code,
    )
    persist_response_capture(connection_factory, capture)
    replay_pep_capture(
        connection_factory,
        capture_id=capture.capture_id,
        release=release,
    )
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    return capture


def test_pep_two_vintages_and_place_publish_without_losing_revision_history(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-003 — revision/latest and canonical place contracts coexist."""
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo_entity (
                    geo_id, geo_type, state_fips, place_fips,
                    first_seen_version, last_seen_version
                ) VALUES
                    ('us:1', 'nation', NULL, NULL, 2020, 2025),
                    ('state:01|place:00124', 'place', '01', '00124', 2020, 2025)
                ON CONFLICT (geo_id) DO NOTHING
                """
            )
        writer.commit()
    finally:
        writer.close()

    _capture_fixture(
        postgres_connection_factory,
        dataset_code="pep_nst_alldata",
        vintage_year=2024,
        fixture_name="nst_2024.csv",
    )
    current_capture = _capture_fixture(
        postgres_connection_factory,
        dataset_code="pep_nst_alldata",
        vintage_year=2025,
        fixture_name="nst_2025.csv",
    )
    _capture_fixture(
        postgres_connection_factory,
        dataset_code="pep_subcounty",
        vintage_year=2025,
        fixture_name="subcounty_2025.csv",
    )

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_pep.observation_revision (
                    capture_id, source_row_index, source_column_index,
                    source_header, dataset_code, release_vintage,
                    product_code, observation_year, metric_code, unit,
                    summary_level, state_fips_source, name_source,
                    value_source, value, value_status
                ) VALUES (
                    %s, 99, 99, 'POPESTIMATE2025', 'pep_nst_alldata', 2025,
                    'NST-EST2025-ALLDATA', 2025, 'POPESTIMATE', 'persons',
                    '040', '99', 'Missing test state', '1', 1, 'valid'
                )
                ON CONFLICT (capture_id, source_row_index, source_column_index)
                DO NOTHING
                """,
                (current_capture.capture_id,),
            )
            cursor.execute(
                "DELETE FROM control.publisher_ready_event "
                "WHERE source_code = 'CENSUS_PEP'"
            )
        writer.commit()
    finally:
        writer.close()

    first_insert = transform_pep_to_silver(
        PostgresHookStub(postgres_connection_factory)
    )
    second_insert = transform_pep_to_silver(
        PostgresHookStub(postgres_connection_factory)
    )

    assert first_insert > 0
    assert second_insert == 0
    event_id = emit_latest_publisher_ready(
        postgres_connection_factory,
        publisher_schema="gold_pep",
    )
    assert event_id is not None
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT pep_vintage, value
                FROM gold_pep.population_estimate_revision
                WHERE dataset_code = 'pep_nst_alldata'
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2024
                  AND geo_id = 'us:1'
                ORDER BY pep_vintage
                """
            )
            assert cursor.fetchall() == [(2024, 340110988), (2025, 340003797)]
            cursor.execute(
                """
                SELECT pep_vintage, value
                FROM gold_pep.population_estimate_latest
                WHERE dataset_code = 'pep_nst_alldata'
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2024
                  AND geo_id = 'us:1'
                """
            )
            assert cursor.fetchone() == (2025, 340003797)
            cursor.execute(
                """
                SELECT geo_id, geo_type, functional_status_source, value
                FROM gold_pep.population_estimate_latest
                WHERE dataset_code = 'pep_subcounty'
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2025
                """
            )
            assert cursor.fetchone() == (
                "state:01|place:00124",
                "place",
                "A",
                2378,
            )
            cursor.execute(
                """
                SELECT status, resolution_method
                FROM silver_ref.geography_resolution
                WHERE provider_source = 'CENSUS_PEP'
                  AND provider_dataset = 'pep_subcounty'
                  AND source_geo_type = 'place'
                  AND source_code = '0100124'
                  AND source_vintage = 2025
                """
            )
            assert cursor.fetchone() == ("resolved", "exact_code")
            cursor.execute(
                """
                SELECT status, reason_code
                FROM silver_ref.geography_resolution
                WHERE provider_source = 'CENSUS_PEP'
                  AND provider_dataset = 'pep_nst_alldata'
                  AND source_geo_type = 'state'
                  AND source_code = '99'
                  AND source_vintage = 2025
                """
            )
            assert cursor.fetchone() == (
                "unmapped",
                "canonical_geography_absent",
            )
            cursor.execute(
                """
                SELECT source_code, status
                FROM control.publisher_ready_event
                WHERE event_id = %s
                """,
                (event_id,),
            )
            assert cursor.fetchone() == ("CENSUS_PEP", "pending")
    finally:
        reader.close()
