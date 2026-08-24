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

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "census_pep"
    / "nst_2025.csv"
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
    control.finish_request(request.request_id, status="success")
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
