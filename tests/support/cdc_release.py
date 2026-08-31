"""Shared CDC fixture-release seeding for database integration tests."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

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
from data_ingestion_toolbox.cdc.metadata import MetadataDecision, parse_metadata
from data_ingestion_toolbox.cdc.registry import CdcAsset

CDC_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "cdc"


def persist_fixture_release(
    connection_factory: Callable[[], connection],
    *,
    asset: CdcAsset,
    metadata_name: str,
    observations_name: str,
    metadata_payload: bytes | None = None,
    observations_payload: bytes | None = None,
) -> CapturedCdcRelease:
    """Capture one reviewed CDC fixture release through the real control path.

    ``metadata_payload``/``observations_payload`` override the fixture file
    bytes when a test needs a variant release (for example a later watermark)
    without adding a near-duplicate fixture file.
    """
    if metadata_payload is None:
        metadata_payload = (CDC_FIXTURE_DIR / metadata_name).read_bytes()
    if observations_payload is None:
        observations_payload = (CDC_FIXTURE_DIR / observations_name).read_bytes()
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
