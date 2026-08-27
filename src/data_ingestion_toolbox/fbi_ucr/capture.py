"""FBI UCR orchestration over the shared raw/control foundation."""

from __future__ import annotations

import json
from collections.abc import Callable
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
    fetch_agency_directory,
    fetch_summarized_observations,
    observation_parameters,
)
from .config import FbiUcrConfig
from .metadata import FbiRelease, ReleaseDecision, decide_release, parse_release
from .registry import FbiSubject, FbiUcrProduct, agency_directory_endpoint

SOURCE_CODE = "FBI_UCR"


@dataclass(frozen=True)
class CapturedFbiRelease:
    """Durable capture lineage for one release decision and slice set."""

    run_id: UUID
    product_id: str
    release: FbiRelease | None
    decision: ReleaseDecision
    release_capture_id: UUID
    directory_capture_ids: tuple[tuple[str, UUID], ...]
    observation_capture_ids: tuple[tuple[str, UUID], ...]
    complete: bool

    @property
    def release_key(self) -> str | None:
        return self.release.release_key if self.release is not None else None


def persist_release_state(
    connection_factory: Callable[[], Any],
    release: CapturedFbiRelease,
    product: FbiUcrProduct,
) -> None:
    """Persist one typed release/capture decision in the control plane."""
    captured = release.release
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.fbi_ucr_release (
                    run_id, product_id, ucr_program, offense_code,
                    period_start, period_end, refresh_date, max_data_month,
                    parser_contract_version, subject_scope,
                    release_capture_id, decision, status,
                    directory_slice_count, observation_slice_count, complete
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    decision = EXCLUDED.decision,
                    status = EXCLUDED.status,
                    directory_slice_count = EXCLUDED.directory_slice_count,
                    observation_slice_count = EXCLUDED.observation_slice_count,
                    complete = EXCLUDED.complete,
                    updated_at = NOW()
                """,
                (
                    str(release.run_id),
                    product.product_id,
                    product.ucr_program,
                    product.offense_code,
                    product.period_start,
                    product.period_end,
                    captured.refresh_date if captured is not None else None,
                    captured.max_data_month if captured is not None else None,
                    product.parser_contract_version,
                    json.dumps(
                        [subject.slice_key for subject in product.subjects],
                        sort_keys=False,
                    ),
                    str(release.release_capture_id),
                    release.decision.value,
                    (
                        # An incomplete slice set is quarantined even when the
                        # release decision itself was to ingest: a partial
                        # capture must never look publishable.
                        "captured"
                        if release.complete
                        and release.decision
                        in (ReleaseDecision.INGEST, ReleaseDecision.UNCHANGED)
                        else "quarantined"
                    ),
                    len(release.directory_capture_ids),
                    len(release.observation_capture_ids),
                    release.complete,
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


def capture_product_release(
    connection_factory: Callable[[], Any],
    product: FbiUcrProduct,
    *,
    previous_release: FbiRelease | None = None,
    config: FbiUcrConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> CapturedFbiRelease:
    """Capture one product release: probe, reference slices, then observations.

    The first registered subject doubles as the release probe because every
    summarized response carries the provider freshness block. Reference agency
    directories are captured before any agency observation, so an observation
    slice can never publish without the reference slice that gives its agency an
    identity, a type, and its county associations.
    """
    runtime_config = config or FbiUcrConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    subjects = product.subjects
    if not subjects:
        raise ValueError("registered product has no observation subjects")
    run_id = capture_control.start_run(watermark={"product_id": product.product_id})
    active_request_id: UUID | None = None
    try:
        probe_subject = subjects[0]
        probe_endpoint = product.observation_endpoint(probe_subject)
        parameters = observation_parameters(product)
        request = capture_control.start_request(
            run_id=run_id,
            endpoint=probe_endpoint,
            parameters=parameters,
            max_attempts=runtime_config.max_attempts,
        )
        active_request_id = request.request_id
        probe_response = fetch_summarized_observations(
            product,
            probe_subject,
            config=runtime_config,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
        )
        release: FbiRelease | None
        try:
            release = parse_release(probe_response.raw_bytes)
        except ValueError:
            release = None
        probe_capture_id = _capture(
            connection_factory,
            control=capture_control,
            run_id=run_id,
            endpoint=probe_endpoint,
            parameters=dict(probe_response.request_parameters),
            payload=probe_response.raw_bytes,
            response_headers=dict(probe_response.response_headers),
            http_status=probe_response.http_status,
            parser_version=product.parser_contract_version,
            source_revision=release.release_key if release is not None else None,
            request_id=request.request_id,
            persist_capture=persist_capture,
        )
        active_request_id = None
        if release is not None:
            capture_control.set_run_watermark(
                run_id,
                watermark={
                    "product_id": product.product_id,
                    "refresh_date": release.release_key,
                    "max_data_month": release.max_data_month,
                },
            )
        decision = decide_release(product, release, previous_release)
        if decision is ReleaseDecision.UNCHANGED:
            capture_control.finish_run(run_id, status="success")
            return CapturedFbiRelease(
                run_id, product.product_id, release, decision, probe_capture_id, (), (),
                True,
            )
        if decision is not ReleaseDecision.INGEST:
            capture_control.quarantine(
                capture_id=probe_capture_id,
                run_id=run_id,
                parser_version=product.parser_contract_version,
                error_code=decision.value,
                error=decision.value,
            )
            capture_control.finish_run(run_id, status="partial")
            return CapturedFbiRelease(
                run_id, product.product_id, release, decision, probe_capture_id, (), (),
                False,
            )

        directory_capture_ids: list[tuple[str, UUID]] = []
        for state_code in product.reference_states:
            endpoint = agency_directory_endpoint(state_code)
            directory_request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters={},
                max_attempts=runtime_config.max_attempts,
            )
            active_request_id = directory_request.request_id
            directory_response = fetch_agency_directory(
                state_code,
                config=runtime_config,
                client=client,
                on_retry=lambda error, request_id=directory_request.request_id: (
                    capture_control.record_request_retry(request_id, error=error)
                ),
            )
            directory_capture_ids.append(
                (
                    state_code,
                    _capture(
                        connection_factory,
                        control=capture_control,
                        run_id=run_id,
                        endpoint=endpoint,
                        parameters={},
                        payload=directory_response.raw_bytes,
                        response_headers=dict(directory_response.response_headers),
                        http_status=directory_response.http_status,
                        parser_version=product.parser_contract_version,
                        source_revision=release.release_key,
                        request_id=directory_request.request_id,
                        persist_capture=persist_capture,
                    ),
                )
            )
            active_request_id = None

        observation_capture_ids: list[tuple[str, UUID]] = [
            (probe_subject.slice_key, probe_capture_id)
        ]
        for subject in subjects[1:]:
            endpoint = product.observation_endpoint(subject)
            subject_request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=parameters,
                max_attempts=runtime_config.max_attempts,
            )
            active_request_id = subject_request.request_id
            response = fetch_summarized_observations(
                product,
                subject,
                config=runtime_config,
                client=client,
                on_retry=lambda error, request_id=subject_request.request_id: (
                    capture_control.record_request_retry(request_id, error=error)
                ),
            )
            observation_capture_ids.append(
                (
                    subject.slice_key,
                    _capture(
                        connection_factory,
                        control=capture_control,
                        run_id=run_id,
                        endpoint=endpoint,
                        parameters=dict(response.request_parameters),
                        payload=response.raw_bytes,
                        response_headers=dict(response.response_headers),
                        http_status=response.http_status,
                        parser_version=product.parser_contract_version,
                        source_revision=release.release_key,
                        request_id=subject_request.request_id,
                        persist_capture=persist_capture,
                    ),
                )
            )
            active_request_id = None

        complete = len(observation_capture_ids) == len(subjects) and len(
            directory_capture_ids
        ) == len(product.reference_states)
        capture_control.finish_run(
            run_id, status="success" if complete else "partial"
        )
        return CapturedFbiRelease(
            run_id,
            product.product_id,
            release,
            decision,
            probe_capture_id,
            tuple(directory_capture_ids),
            tuple(observation_capture_ids),
            complete,
        )
    except BaseException as exc:
        if active_request_id is not None:
            capture_control.finish_request(
                active_request_id, status="failed", error=exc
            )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise


def subject_from_slice_key(slice_key: str) -> FbiSubject:
    """Rebuild one subject identity from its stable slice key."""
    subject_type, _, subject_code = slice_key.partition(":")
    return FbiSubject(subject_type, subject_code)
