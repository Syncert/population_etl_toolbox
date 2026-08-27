"""USDA NASS orchestration over the shared raw/control foundation.

Every registered slice is preflighted through ``get_counts`` before a record is
requested, so an over-limit partition is refused rather than truncated, and a
retrieval that does not match its own preflight marks the release incomplete.
Nothing is parsed until its bytes have committed.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)

from .client import (
    API_COUNT_PATH,
    API_DATA_PATH,
    NassOverLimitError,
    count_parameters,
    data_parameters,
    fetch_slice_count,
    fetch_slice_records,
)
from .config import NassConfig
from .metadata import (
    PUBLISHABLE_DECISIONS,
    NassReleaseContract,
    NassSliceCount,
    ReleaseDecision,
    decide_preflight,
    decide_release,
    summarize_release,
)
from .registry import NassProduct, SliceMode, iter_slices

SOURCE_CODE = "USDA_NASS"

#: Slice outcomes recorded in the control plane.
SLICE_PREFLIGHTED = "preflighted"
SLICE_CAPTURED = "captured"
SLICE_EMPTY = "empty"
SLICE_OVER_LIMIT = "over_limit"
SLICE_PARTIAL = "partial"
SLICE_SKIPPED = "skipped"


@dataclass(frozen=True)
class CapturedNassSlice:
    """Durable lineage for one preflighted and possibly retrieved partition."""

    slice_key: str
    agg_level_desc: str
    year: int
    provider_count: int
    captured_row_count: int
    count_capture_id: UUID
    data_capture_id: UUID | None
    status: str


@dataclass(frozen=True)
class CapturedNassRelease:
    """Durable capture lineage for one product extraction."""

    run_id: UUID
    product_id: str
    slice_mode: str
    slices: tuple[CapturedNassSlice, ...]
    contract: NassReleaseContract
    decision: ReleaseDecision
    row_count: int
    complete: bool

    @property
    def data_capture_ids(self) -> tuple[UUID, ...]:
        return tuple(
            item.data_capture_id
            for item in self.slices
            if item.data_capture_id is not None
        )


def persist_release_state(
    connection_factory: Callable[[], Any],
    release: CapturedNassRelease,
) -> None:
    """Persist one typed release decision and every slice outcome."""
    status = (
        "captured"
        if release.decision in PUBLISHABLE_DECISIONS
        else "quarantined"
    )
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.usda_nass_release (
                    run_id, product_id, slice_mode, parser_contract_version,
                    extraction_watermark, total_row_count, slice_counts,
                    field_signature, decision, status, captured_row_count,
                    slice_count, complete
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s::JSONB, %s::JSONB,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    decision = EXCLUDED.decision,
                    status = EXCLUDED.status,
                    extraction_watermark = EXCLUDED.extraction_watermark,
                    total_row_count = EXCLUDED.total_row_count,
                    slice_counts = EXCLUDED.slice_counts,
                    field_signature = EXCLUDED.field_signature,
                    captured_row_count = EXCLUDED.captured_row_count,
                    slice_count = EXCLUDED.slice_count,
                    complete = EXCLUDED.complete,
                    updated_at = NOW()
                """,
                (
                    str(release.run_id),
                    release.product_id,
                    release.slice_mode,
                    release.contract.parser_contract_version,
                    release.contract.extraction_watermark,
                    release.contract.total_row_count,
                    json.dumps(
                        [list(item) for item in release.contract.slice_counts]
                    ),
                    json.dumps(list(release.contract.field_signature)),
                    release.decision.value,
                    status,
                    release.row_count,
                    len(release.slices),
                    release.complete,
                ),
            )
            for item in release.slices:
                cursor.execute(
                    """
                    INSERT INTO control.usda_nass_slice (
                        run_id, slice_key, product_id, agg_level_desc, year,
                        provider_count, captured_row_count, count_capture_id,
                        data_capture_id, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, slice_key) DO UPDATE SET
                        provider_count = EXCLUDED.provider_count,
                        captured_row_count = EXCLUDED.captured_row_count,
                        count_capture_id = EXCLUDED.count_capture_id,
                        data_capture_id = EXCLUDED.data_capture_id,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                    """,
                    (
                        str(release.run_id),
                        item.slice_key,
                        release.product_id,
                        item.agg_level_desc,
                        item.year,
                        item.provider_count,
                        item.captured_row_count,
                        str(item.count_capture_id),
                        (
                            str(item.data_capture_id)
                            if item.data_capture_id is not None
                            else None
                        ),
                        item.status,
                    ),
                )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()


def _capture(
    connection_factory: Callable[[], Any],
    *,
    control: CaptureControl,
    run_id: UUID,
    endpoint: str,
    parameters: dict[str, object],
    payload: bytes,
    response_headers: dict[str, str],
    http_status: int,
    parser_version: str,
    source_revision: str | None,
    request_id: UUID,
    persist_capture: Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt],
) -> UUID:
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request_id,
        run_id=run_id,
        source_code=SOURCE_CODE,
        endpoint=endpoint,
        request_parameters=parameters,
        retrieved_at=datetime.now(UTC),
        http_status=http_status,
        response_headers=response_headers,
        media_type="application/json",
        payload=payload,
        payload_schema_version=parser_version,
        source_revision=source_revision,
    )
    receipt = persist_capture(connection_factory, capture)
    control.finish_request(request_id, status="captured")
    return receipt.capture_id


def _quarantine_release(
    control: CaptureControl,
    *,
    run_id: UUID,
    product: NassProduct,
    decision: ReleaseDecision,
    capture_id: UUID | None,
) -> None:
    if capture_id is None:
        return
    control.quarantine(
        capture_id=capture_id,
        run_id=run_id,
        parser_version=product.parser_contract_version,
        error_code=decision.value,
        error=decision.value,
    )


def capture_product_release(
    connection_factory: Callable[[], Any],
    product: NassProduct,
    *,
    mode: SliceMode = "full",
    previous_release: NassReleaseContract | None = None,
    config: NassConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> CapturedNassRelease:
    """Preflight, capture, and reconcile one registered product extraction."""
    runtime_config = config or NassConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    planned = iter_slices(product, mode=mode)
    run_id = capture_control.start_run(
        watermark={"product_id": product.product_id, "slice_mode": mode}
    )
    active_request_id: UUID | None = None
    try:
        counts: list[NassSliceCount] = []
        count_capture_ids: dict[str, UUID] = {}
        for item in planned:
            parameters = count_parameters(product, item)
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=API_COUNT_PATH,
                parameters=parameters,
                max_attempts=runtime_config.request_max_attempts,
            )
            active_request_id = request.request_id
            response = fetch_slice_count(
                product,
                item,
                config=runtime_config,
                client=client,
                on_retry=lambda error, request_id=request.request_id: (
                    capture_control.record_request_retry(request_id, error=error)
                ),
            )
            capture_id = _capture(
                connection_factory,
                control=capture_control,
                run_id=run_id,
                endpoint=API_COUNT_PATH,
                parameters=dict(response.request_parameters),
                payload=response.raw_bytes,
                response_headers=dict(response.response_headers),
                http_status=response.http_status,
                parser_version=product.parser_contract_version,
                source_revision=None,
                request_id=request.request_id,
                persist_capture=persist_capture,
            )
            active_request_id = None
            count_capture_ids[item.slice_key] = capture_id
            counts.append(
                NassSliceCount(
                    slice_key=item.slice_key,
                    agg_level_desc=item.agg_level_desc,
                    year=item.year,
                    provider_count=response.count,
                    capture_id=str(capture_id),
                )
            )

        preflight = decide_preflight(product, runtime_config, counts, previous_release)
        if preflight is not ReleaseDecision.INGEST:
            release = _finish_without_retrieval(
                capture_control,
                run_id=run_id,
                product=product,
                mode=mode,
                counts=counts,
                count_capture_ids=count_capture_ids,
                decision=preflight,
                previous_release=previous_release,
            )
            return release

        captured: list[CapturedNassSlice] = []
        payloads: list[bytes] = []
        complete = True
        for item, count in zip(planned, counts, strict=True):
            count_capture_id = count_capture_ids[item.slice_key]
            if count.provider_count == 0:
                captured.append(
                    CapturedNassSlice(
                        slice_key=item.slice_key,
                        agg_level_desc=item.agg_level_desc,
                        year=item.year,
                        provider_count=0,
                        captured_row_count=0,
                        count_capture_id=count_capture_id,
                        data_capture_id=None,
                        status=SLICE_EMPTY,
                    )
                )
                continue
            parameters = data_parameters(product, item)
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=API_DATA_PATH,
                parameters=parameters,
                max_attempts=runtime_config.request_max_attempts,
            )
            active_request_id = request.request_id
            try:
                response = fetch_slice_records(
                    product,
                    item,
                    config=runtime_config,
                    client=client,
                    on_retry=lambda error, request_id=request.request_id: (
                        capture_control.record_request_retry(request_id, error=error)
                    ),
                )
            except NassOverLimitError as exc:
                capture_control.finish_request(
                    request.request_id, status="failed", error=exc
                )
                active_request_id = None
                complete = False
                captured.append(
                    CapturedNassSlice(
                        slice_key=item.slice_key,
                        agg_level_desc=item.agg_level_desc,
                        year=item.year,
                        provider_count=count.provider_count,
                        captured_row_count=0,
                        count_capture_id=count_capture_id,
                        data_capture_id=None,
                        status=SLICE_OVER_LIMIT,
                    )
                )
                continue
            data_capture_id = _capture(
                connection_factory,
                control=capture_control,
                run_id=run_id,
                endpoint=API_DATA_PATH,
                parameters=dict(response.request_parameters),
                payload=response.raw_bytes,
                response_headers=dict(response.response_headers),
                http_status=response.http_status,
                parser_version=product.parser_contract_version,
                source_revision=None,
                request_id=request.request_id,
                persist_capture=persist_capture,
            )
            active_request_id = None
            matched = response.row_count == count.provider_count
            complete = complete and matched
            payloads.append(response.raw_bytes)
            captured.append(
                CapturedNassSlice(
                    slice_key=item.slice_key,
                    agg_level_desc=item.agg_level_desc,
                    year=item.year,
                    provider_count=count.provider_count,
                    captured_row_count=response.row_count,
                    count_capture_id=count_capture_id,
                    data_capture_id=data_capture_id,
                    status=SLICE_CAPTURED if matched else SLICE_PARTIAL,
                )
            )

        contract = summarize_release(product, payloads=payloads, slice_counts=counts)
        decision = (
            decide_release(product, contract, previous_release)
            if complete
            else ReleaseDecision.PARTIAL_SLICE_QUARANTINE
        )
        capture_control.set_run_watermark(
            run_id,
            watermark={
                "product_id": product.product_id,
                "slice_mode": mode,
                "extraction_watermark": contract.extraction_watermark,
            },
        )
        if decision not in PUBLISHABLE_DECISIONS:
            _quarantine_release(
                capture_control,
                run_id=run_id,
                product=product,
                decision=decision,
                capture_id=next(
                    (
                        item.data_capture_id
                        for item in captured
                        if item.data_capture_id is not None
                    ),
                    next(iter(count_capture_ids.values()), None),
                ),
            )
            capture_control.finish_run(run_id, status="partial")
            return CapturedNassRelease(
                run_id,
                product.product_id,
                mode,
                tuple(captured),
                contract,
                decision,
                contract.total_row_count,
                False,
            )
        capture_control.finish_run(run_id, status="success")
        return CapturedNassRelease(
            run_id,
            product.product_id,
            mode,
            tuple(captured),
            contract,
            decision,
            contract.total_row_count,
            True,
        )
    except BaseException as exc:
        if active_request_id is not None:
            capture_control.finish_request(
                active_request_id, status="failed", error=exc
            )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise


def _finish_without_retrieval(
    control: CaptureControl,
    *,
    run_id: UUID,
    product: NassProduct,
    mode: SliceMode,
    counts: Sequence[NassSliceCount],
    count_capture_ids: dict[str, UUID],
    decision: ReleaseDecision,
    previous_release: NassReleaseContract | None,
) -> CapturedNassRelease:
    """Close a run whose preflight decided no records may be retrieved."""
    contract = NassReleaseContract(
        product_id=product.product_id,
        parser_contract_version=product.parser_contract_version,
        extraction_watermark=(
            previous_release.extraction_watermark
            if decision is ReleaseDecision.UNCHANGED and previous_release is not None
            else ""
        ),
        total_row_count=(
            previous_release.total_row_count
            if decision is ReleaseDecision.UNCHANGED and previous_release is not None
            else 0
        ),
        slice_counts=tuple((item.slice_key, item.provider_count) for item in counts),
        field_signature=(
            previous_release.field_signature
            if decision is ReleaseDecision.UNCHANGED and previous_release is not None
            else ()
        ),
    )
    slices = tuple(
        CapturedNassSlice(
            slice_key=item.slice_key,
            agg_level_desc=item.agg_level_desc,
            year=item.year,
            provider_count=item.provider_count,
            captured_row_count=0,
            count_capture_id=count_capture_ids[item.slice_key],
            data_capture_id=None,
            status=(
                SLICE_OVER_LIMIT
                if decision is ReleaseDecision.OVER_LIMIT_QUARANTINE
                and item.provider_count > 0
                else SLICE_SKIPPED
                if decision is ReleaseDecision.UNCHANGED
                else SLICE_PREFLIGHTED
            ),
        )
        for item in counts
    )
    if decision is ReleaseDecision.UNCHANGED:
        control.finish_run(run_id, status="success")
        return CapturedNassRelease(
            run_id,
            product.product_id,
            mode,
            slices,
            contract,
            decision,
            0,
            True,
        )
    _quarantine_release(
        control,
        run_id=run_id,
        product=product,
        decision=decision,
        capture_id=next(iter(count_capture_ids.values()), None),
    )
    control.finish_run(run_id, status="partial")
    return CapturedNassRelease(
        run_id,
        product.product_id,
        mode,
        slices,
        contract,
        decision,
        0,
        False,
    )


def resolve_slice_mode(logical_date: datetime, config: NassConfig) -> SliceMode:
    """Return the registered slice mode for one scheduled logical date.

    Ordinary business-day operation retrieves only the bounded recent window;
    the first days of a month sweep the whole registered history so revisions
    to earlier years are reconciled on a stable cadence.
    """
    if logical_date.day <= config.full_reconciliation_day_of_month:
        return "full"
    return "recent"
