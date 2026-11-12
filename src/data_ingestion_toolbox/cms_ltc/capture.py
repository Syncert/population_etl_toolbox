"""Capture-first orchestration for CMS's catalog and the registered CSV files.

A load reads the catalog once and commits its bytes as its own run before
anything parses them; the releases it lists are then read one run each, one
request and one committed capture per CSV. A file whose bytes, parser
contract and geography equal the same release's last published capture is
recorded ``unchanged`` and replays nothing (utility.unchanged).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.utility.unchanged import repeats_last_publication

from .catalog import Release, Skipped, releases
from .client import fetch_catalog, fetch_file
from .config import SOURCE_CODE, CmsLtcConfig
from .registry import get_series

CATALOG_CONTRACT_VERSION = "cms_data_json:v1"

#: The release's newest published capture.
LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, file.published_at
    FROM control.cms_ltc_file AS file
    JOIN raw_capture.response_capture AS response ON response.capture_id = file.capture_id
    WHERE file.series_id = %s AND file.period_start = %s AND file.status = 'published'
    ORDER BY file.published_at DESC
    LIMIT 1
"""

PersistCapture = Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt]


def capture_catalog(
    connection_factory: Callable[[], Any],
    *,
    config: CmsLtcConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
) -> tuple[UUID, list[Release], list[Skipped]]:
    """Capture the catalog; return (its capture id, the releases, what was set aside)."""
    runtime = config or CmsLtcConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {"document": "data.json"}
    run_id = capture_control.start_run(watermark=dict(parameters))
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=runtime.catalog_url,
        parameters=parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_catalog(
            config=runtime,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
        )
        receipt = persist_capture(
            connection_factory,
            ResponseCapture(
                capture_id=uuid4(),
                request_id=request.request_id,
                run_id=run_id,
                source_code=SOURCE_CODE,
                endpoint=runtime.catalog_url,
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/json",
                payload=response.raw_bytes,
                payload_schema_version=CATALOG_CONTRACT_VERSION,
                source_revision=None,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        found, skipped = releases(response.raw_bytes)
        capture_control.finish_run(run_id, status="success")
        return receipt.capture_id, found, skipped
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise


def capture_release(
    connection_factory: Callable[[], Any],
    release: Release,
    *,
    catalog_capture_id: UUID,
    config: CmsLtcConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one release's CSV; return (run_id, file status)."""
    runtime = config or CmsLtcConfig()
    series = get_series(release.series_id)
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {
        "series": release.series_id,
        "period_start": release.period_start.isoformat(),
        "period_end": release.period_end.isoformat(),
        "title": release.title,
    }
    run_id = capture_control.start_run(watermark=dict(parameters))
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=release.download_url,
        parameters=parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_file(
            release.download_url,
            series,
            config=runtime,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
        )
        receipt = persist_capture(
            connection_factory,
            ResponseCapture(
                capture_id=uuid4(),
                request_id=request.request_id,
                run_id=run_id,
                source_code=SOURCE_CODE,
                endpoint=release.download_url,
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="text/csv",
                payload=response.raw_bytes,
                payload_schema_version=series.parser_contract_version,
                source_revision=release.modified or None,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        unchanged = repeats_last_publication(
            connection_factory,
            LATEST_PUBLISHED_SQL,
            (release.series_id, release.period_start),
            payload_checksum=receipt.payload_checksum,
            parser_contract=series.parser_contract_version,
        )
        status = "unchanged" if unchanged else "captured"
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.cms_ltc_file (
                        run_id, series_id, period_start, period_end, release_title,
                        catalog_modified, download_url, catalog_capture_id,
                        capture_id, payload_checksum, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(run_id),
                        release.series_id,
                        release.period_start,
                        release.period_end,
                        release.title,
                        release.modified,
                        release.download_url,
                        str(catalog_capture_id),
                        str(receipt.capture_id),
                        receipt.payload_checksum,
                        status,
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
