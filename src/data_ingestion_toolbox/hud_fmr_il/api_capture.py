"""Capture-first orchestration for HUD User Data API reads.

One ingestion run per read of a registered dataset and fiscal year. Every
API answer is its own request and committed capture, before anything parses
it: for FMRs, ``fmr/listStates`` and then each state's ``fmr/statedata``; for
income limits, ``fmr/listStates``, each state's ``fmr/listCounties``, and
then each whole county's ``il/data``. The run's checksum is the checksum of
its answers' checksums in order; a read equal to the last published read is
recorded ``unchanged`` and replays nothing.

The token travels only in the ``Authorization`` header. The captured
endpoint and parameters name the path and year, never the token.
"""

from __future__ import annotations

import hashlib
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

from .api import (
    HUD_API_BASE_URL,
    ApiRead,
    HudApiClient,
    state_codes,
    whole_counties,
)
from .config import SOURCE_CODE, HudConfig
from .registry import FMR

PARSER_CONTRACT_VERSION = "hud_fmr_il_api_json:v1"


def _execute(
    connection_factory: Callable[[], Any], sql: str, parameters: tuple
) -> list[tuple]:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            rows = cursor.fetchall() if cursor.description else []
        connection.commit()
        return rows
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_api_read(
    connection_factory: Callable[[], Any],
    read: ApiRead,
    *,
    config: HudConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    sleep: Callable[[float], None] | None = None,
) -> tuple[UUID, str]:
    """Capture every answer of one registered API read; return (run_id, status)."""
    runtime = config or HudConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_parameters = {
        "channel": "api",
        "dataset": read.dataset,
        "fiscal_year": str(read.fiscal_year),
    }
    # A missing token fails here, before any run is recorded.
    api = HudApiClient(runtime, client=client, **({"sleep": sleep} if sleep else {}))
    run_id = capture_control.start_run(watermark=dict(run_parameters))
    answers: list[tuple[str, CaptureReceipt]] = []
    try:

        def answer(slice_key: str, path: str, parameters: dict[str, str]) -> bytes:
            endpoint = f"{HUD_API_BASE_URL}/{path}"
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            try:
                query = "&".join(
                    f"{name}={value}"
                    for name, value in parameters.items()
                    if name == "year"
                )
                response = api.get(
                    f"{path}?{query}" if query else path,
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
                        endpoint=endpoint,
                        request_parameters=parameters,
                        retrieved_at=datetime.now(UTC),
                        http_status=response.http_status,
                        response_headers=dict(response.response_headers),
                        media_type="application/json",
                        payload=response.raw_bytes,
                        payload_schema_version=PARSER_CONTRACT_VERSION,
                        source_revision=read.key,
                    ),
                )
            except BaseException as exc:
                capture_control.finish_request(
                    request.request_id, status="failed", error=exc
                )
                raise
            capture_control.finish_request(request.request_id, status="captured")
            answers.append((slice_key, receipt))
            return response.raw_bytes

        year = str(read.fiscal_year)
        states = state_codes(answer("list:STATES", "fmr/listStates", {}))
        for state in states:
            if read.dataset == FMR:
                answer(
                    f"fmr:{state}",
                    f"fmr/statedata/{state}",
                    {"state": state, "year": year},
                )
                continue
            listing = answer(
                f"list:{state}", f"fmr/listCounties/{state}", {"state": state}
            )
            for fips in whole_counties(listing, state):
                answer(f"il:{fips}", f"il/data/{fips}", {"fips": fips, "year": year})
        checksum = hashlib.sha256(
            "\n".join(
                f"{key}={receipt.payload_checksum}" for key, receipt in answers
            ).encode()
        ).hexdigest()
        previous = _execute(
            connection_factory,
            """
            SELECT payload_checksum FROM control.hud_fmr_il_file
            WHERE channel = 'api' AND dataset = %s AND fiscal_year = %s AND status = 'published'
            ORDER BY published_at DESC LIMIT 1
            """,
            (read.dataset, read.fiscal_year),
        )
        status = "unchanged" if previous and previous[0][0] == checksum else "captured"
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.hud_fmr_il_file (
                        run_id, channel, dataset, fiscal_year, edition, effective_date,
                        capture_id, payload_checksum, status
                    ) VALUES (%s, 'api', %s, %s, %s, %s, NULL, %s, %s)
                    """,
                    (
                        str(run_id),
                        read.dataset,
                        read.fiscal_year,
                        read.edition,
                        read.effective_date,
                        checksum,
                        status,
                    ),
                )
                for slice_key, receipt in answers:
                    cursor.execute(
                        """
                        INSERT INTO control.hud_fmr_il_api_capture (run_id, slice_key, capture_id, payload_checksum)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            str(run_id),
                            slice_key,
                            str(receipt.capture_id),
                            receipt.payload_checksum,
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
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    finally:
        api.close()
