"""Checksum-backed, complete-slice offline replay for FBI UCR captures."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg2.extras import Json, execute_values

from ..registry import (
    COUNTED_ENTITY_BASES,
    MEASURE_FORMS,
    FbiUcrProduct,
    agency_directory_endpoint,
)
from .agency import parse_agency_directory
from .models import (
    FbiAgencyRecord,
    FbiObservation,
    FbiParticipation,
    QuarantinedRecord,
    ReplayResult,
)
from .offenses import FbiSubjectLabelError, parse_summarized_observations, subject_label
from .participation import parse_participation

SOURCE_CODE = "FBI_UCR"


class FbiReplayError(ValueError):
    """Captured slices cannot form a complete registered release."""


def json_evidence(value: Any) -> Json:
    """Adapt one source-evidence structure for JSONB storage.

    Captured numbers are decoded as exact decimals, which JSON cannot encode
    natively. Each is stored as its exact decimal literal so the evidence keeps
    full precision; the byte-exact payload always remains in ``raw_capture``.
    """

    def convert(node: Any) -> Any:
        if isinstance(node, Decimal):
            return format(node, "f")
        if isinstance(node, dict):
            return {key: convert(item) for key, item in node.items()}
        if isinstance(node, (list, tuple)):
            return [convert(item) for item in node]
        return node

    return Json(convert(value))


def slice_input_count(product: FbiUcrProduct) -> int:
    """Return how many inputs one observation slice must reconcile to."""
    periods = len(product.expected_periods)
    measures = len(MEASURE_FORMS) * len(COUNTED_ENTITY_BASES) * periods
    return measures + periods


@dataclass(frozen=True)
class CapturedSlice:
    """Exact bytes and identity of one captured provider response."""

    capture_id: UUID
    endpoint: str
    payload: bytes
    payload_checksum: str

    def document(self) -> Any:
        if hashlib.sha256(self.payload).hexdigest() != self.payload_checksum:
            raise FbiReplayError(f"FBI capture checksum mismatch: {self.capture_id}")
        try:
            # Numbers are decoded as exact decimals so a published measure value
            # never loses precision on the way into silver.
            return json.loads(self.payload, parse_float=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FbiReplayError("FBI capture is not valid JSON") from exc


def replay_slices(
    product: FbiUcrProduct,
    slices: dict[str, CapturedSlice],
    *,
    release_key: str,
) -> ReplayResult:
    """Replay a complete registered slice set without network access."""
    directory_endpoints = {
        state: agency_directory_endpoint(state) for state in product.reference_states
    }
    observation_endpoints = {
        subject: product.observation_endpoint(subject) for subject in product.subjects
    }
    missing = sorted(
        endpoint
        for endpoint in (
            *directory_endpoints.values(),
            *observation_endpoints.values(),
        )
        if endpoint not in slices
    )
    if missing:
        raise FbiReplayError(
            "FBI release is missing required capture slices: " + ", ".join(missing)
        )

    input_count = 0
    agencies: list[FbiAgencyRecord] = []
    observations: list[FbiObservation] = []
    participation: list[FbiParticipation] = []
    quarantined: list[QuarantinedRecord] = []

    for state, endpoint in directory_endpoints.items():
        captured = slices[endpoint]
        result = parse_agency_directory(
            captured.document(), state_code=state, slice_key=f"agency_directory:{state}"
        )
        input_count += result.input_count
        agencies.extend(
            replace(record, capture_id=captured.capture_id)
            for record in result.agencies
        )
        quarantined.extend(result.quarantined)

    agency_names = {record.ori: record.agency_name for record in agencies}

    for subject, endpoint in observation_endpoints.items():
        captured = slices[endpoint]
        document = captured.document()
        try:
            label = subject_label(subject, agency_names=agency_names)
        except FbiSubjectLabelError as exc:
            # The observation slice was captured, but its reference slice does
            # not identify the subject. Publishing it would guess which series
            # belongs to the agency, so the whole slice is quarantined instead.
            unusable = slice_input_count(product)
            input_count += unusable
            quarantined.extend(
                QuarantinedRecord(
                    subject.slice_key, index, "agency_reference_missing", str(exc)
                )
                for index in range(unusable)
            )
            continue
        measures = parse_summarized_observations(
            document,
            product=product,
            release_key=release_key,
            subject=subject,
            label=label,
            slice_key=subject.slice_key,
        )
        coverage = parse_participation(
            document if isinstance(document, dict) else {},
            product=product,
            release_key=release_key,
            subject=subject,
            subject_label=label,
            slice_key=subject.slice_key,
        )
        input_count += measures.input_count + coverage.input_count
        observations.extend(
            replace(item, capture_id=captured.capture_id)
            for item in measures.observations
        )
        participation.extend(
            replace(item, capture_id=captured.capture_id)
            for item in coverage.participation
        )
        quarantined.extend((*measures.quarantined, *coverage.quarantined))

    produced = len(agencies) + len(observations) + len(participation)
    if input_count != produced + len(quarantined):
        raise FbiReplayError("FBI replay reconciliation failed")
    return ReplayResult(
        input_count,
        tuple(observations),
        tuple(participation),
        tuple(agencies),
        tuple(quarantined),
    )


def load_captured_slices(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
) -> dict[str, CapturedSlice]:
    """Load exact captured bytes for one run, keyed by provider endpoint."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT capture.capture_id, capture.endpoint,
                       blob.payload, blob.payload_checksum
                FROM raw_capture.response_capture AS capture
                JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
                WHERE capture.run_id = %s AND capture.source_code = %s
                ORDER BY capture.endpoint, capture.retrieved_at DESC,
                         capture.capture_id
                """,
                (str(run_id), SOURCE_CODE),
            )
            rows = cursor.fetchall()
    finally:
        database_connection.close()
    slices: dict[str, CapturedSlice] = {}
    for capture_id, endpoint, payload, checksum in rows:
        # A revised response for the same endpoint is retained as its own
        # capture; the newest retrieval wins for this run's replay while the
        # earlier bytes stay queryable.
        slices.setdefault(
            endpoint, CapturedSlice(capture_id, endpoint, bytes(payload), checksum)
        )
    return slices


def replay_captured_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: FbiUcrProduct,
    release_key: str,
) -> ReplayResult:
    """Replay one complete run solely from durable capture bytes."""
    return replay_slices(
        product,
        load_captured_slices(connection_factory, run_id=run_id),
        release_key=release_key,
    )


def persist_replay_result(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: FbiUcrProduct,
    release_key: str,
    result: ReplayResult,
) -> None:
    """Persist capture-scoped source revisions and explicit quarantine rows."""
    produced = (
        len(result.agencies) + len(result.observations) + len(result.participation)
    )
    if result.input_count != produced + len(result.quarantined):
        raise FbiReplayError("FBI replay reconciliation failed before persistence")

    agencies = [
        (
            str(record.capture_id),
            record.source_row_index,
            str(run_id),
            product.product_id,
            release_key,
            record.ori,
            record.agency_name,
            record.agency_type,
            record.state_code,
            record.state_name,
            list(record.county_labels),
            record.is_nibrs,
            record.nibrs_start_date,
            record.latitude,
            record.longitude,
            json_evidence(record.source_row),
        )
        for record in result.agencies
    ]
    observations = [
        (
            str(item.capture_id),
            item.source_row_index,
            str(run_id),
            item.product_id,
            item.release_key,
            item.source_record_id,
            json_evidence(item.source_row),
            item.ucr_program,
            item.offense_code,
            item.offense_label,
            item.measure_id,
            item.measure_form,
            item.counted_entity_basis,
            item.unit,
            item.reported_status,
            item.subject_type,
            item.subject_code,
            item.subject_label,
            item.source_geo_level,
            item.period,
            item.period_start,
            item.period_end,
            item.value_source,
            item.value,
            item.value_status,
            item.population_denominator,
        )
        for item in result.observations
    ]
    coverage = [
        (
            str(item.capture_id),
            item.source_row_index,
            str(run_id),
            item.product_id,
            item.release_key,
            item.ucr_program,
            item.subject_type,
            item.subject_code,
            item.subject_label,
            item.source_geo_level,
            item.period,
            item.period_start,
            item.period_end,
            item.population,
            item.participated_population,
            item.coverage_percent,
            item.coverage_basis,
            item.participation_status,
            json_evidence(item.source_row),
        )
        for item in result.participation
    ]

    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            if agencies:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fbi.agency_revision (
                        capture_id, source_row_index, run_id, product_id,
                        release_key, ori, agency_name, agency_type, state_code,
                        state_name, county_labels, is_nibrs, nibrs_start_date,
                        latitude, longitude, source_record
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    agencies,
                )
            if observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fbi.observation_revision (
                        capture_id, source_row_index, run_id, product_id,
                        release_key, source_record_id, source_record,
                        ucr_program, offense_code, offense_label, measure_id,
                        measure_form, counted_entity_basis, unit,
                        reported_status, subject_type, subject_code,
                        subject_label, source_geo_level, period, period_start,
                        period_end, value_source, value, value_status,
                        population_denominator
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    observations,
                )
            if coverage:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fbi.participation_revision (
                        capture_id, source_row_index, run_id, product_id,
                        release_key, ucr_program, subject_type, subject_code,
                        subject_label, source_geo_level, period, period_start,
                        period_end, population, participated_population,
                        coverage_percent, coverage_basis, participation_status,
                        source_record
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    coverage,
                )
            if result.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fbi.slice_quarantine (
                        run_id, product_id, release_key, slice_key,
                        source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (
                        run_id, product_id, release_key, slice_key,
                        source_row_index, error_code
                    ) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            product.product_id,
                            release_key,
                            item.slice_key,
                            item.source_row_index,
                            item.error_code,
                            item.error_summary,
                        )
                        for item in result.quarantined
                    ],
                )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
