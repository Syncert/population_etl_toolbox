"""Capture-first orchestration, slice ordering, and control-plane state."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import pytest

from data_ingestion_toolbox.capture import CaptureReceipt
from data_ingestion_toolbox.fbi_ucr import capture as fbi_capture
from data_ingestion_toolbox.fbi_ucr.capture import (
    capture_product_release,
    subject_from_slice_key,
)
from data_ingestion_toolbox.fbi_ucr.client import CdeResponse, observation_parameters
from data_ingestion_toolbox.fbi_ucr.config import API_KEY_PARAMETER, FbiUcrConfig
from data_ingestion_toolbox.fbi_ucr.metadata import FbiRelease, ReleaseDecision
from data_ingestion_toolbox.fbi_ucr.registry import (
    SUMMARIZED_VIOLENT_CRIME,
    agency_directory_endpoint,
)

from ._doubles import API_KEY
from .conftest import load_bytes

pytestmark = pytest.mark.unit

PRODUCT = SUMMARIZED_VIOLENT_CRIME


class _Control:
    """Records every control-plane transition without touching a database."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []
        self.watermarks: list[dict] = []
        self.quarantined: list[str] = []
        self.run_status: str | None = None

    def start_run(self, *, watermark=None):  # noqa: ANN001, ANN202
        run_id = UUID(int=7)
        self.events.append(("run", run_id))
        return run_id

    def finish_run(self, run_id, *, status, error=None):  # noqa: ANN001
        self.run_status = status
        self.events.append(("finish_run", status))

    def set_run_watermark(self, run_id, *, watermark):  # noqa: ANN001
        self.watermarks.append(dict(watermark))

    def start_request(self, *, run_id, endpoint, parameters, max_attempts=1):  # noqa: ANN001, ANN202
        request_id = uuid4()
        self.events.append(("request", (endpoint, dict(parameters))))
        return _Request(request_id)

    def finish_request(self, request_id, *, status, error=None):  # noqa: ANN001
        self.events.append(("finish_request", status))

    def record_request_retry(self, request_id, *, error):  # noqa: ANN001
        self.events.append(("retry", str(error)))

    def quarantine(self, *, capture_id, run_id, parser_version, error_code, error):  # noqa: ANN001
        self.quarantined.append(error_code)

    @property
    def endpoints(self) -> list[str]:
        return [item[1][0] for item in self.events if item[0] == "request"]


class _Request:
    def __init__(self, request_id: UUID) -> None:
        self.request_id = request_id
        self.request_fingerprint = "fingerprint"


class _Capturer:
    """Collects the capture envelopes the orchestration would persist."""

    def __init__(self) -> None:
        self.captures: list[Any] = []

    def __call__(self, _factory, capture) -> CaptureReceipt:  # noqa: ANN001
        self.captures.append(capture)
        return CaptureReceipt(capture.capture_id, capture.payload_checksum)


def _response(endpoint: str, parameters: dict, name: str) -> CdeResponse:
    return CdeResponse(
        endpoint,
        parameters,
        load_bytes(name),
        {"content-type": "application/json"},
        200,
    )


def _install_provider(monkeypatch, *, national: str = "summarized_national_V") -> None:
    def observations(product, subject, **_kwargs):  # noqa: ANN001, ANN202
        names = {
            "national": national,
            "state": f"summarized_state_{subject.subject_code}_V",
            "agency": f"summarized_agency_{subject.subject_code}_V",
        }
        return _response(
            product.observation_endpoint(subject),
            observation_parameters(product),
            names[subject.subject_type],
        )

    def directory(state_code, **_kwargs):  # noqa: ANN001, ANN202
        return _response(
            agency_directory_endpoint(state_code), {}, f"agency_directory_{state_code}"
        )

    monkeypatch.setattr(fbi_capture, "fetch_summarized_observations", observations)
    monkeypatch.setattr(fbi_capture, "fetch_agency_directory", directory)


def _config() -> FbiUcrConfig:
    return FbiUcrConfig(cde_api_key=API_KEY, min_spacing_seconds=0.0)


def test_reference_slices_are_captured_before_agency_observations(
    monkeypatch,
) -> None:
    """Covers: ETL-024 — the agency reference slice precedes its observations."""
    _install_provider(monkeypatch)
    control = _Control()
    capturer = _Capturer()

    release = capture_product_release(
        lambda: None,
        PRODUCT,
        config=_config(),
        control=control,
        persist_capture=capturer,
    )

    endpoints = control.endpoints
    directory_index = endpoints.index(agency_directory_endpoint("WI"))
    agency_indexes = [
        index
        for index, endpoint in enumerate(endpoints)
        if endpoint.startswith("/summarized/agency/")
    ]

    assert release.decision is ReleaseDecision.INGEST
    assert release.complete
    assert agency_indexes
    assert directory_index < min(agency_indexes)
    assert len(release.observation_capture_ids) == len(PRODUCT.subjects)
    assert len(release.directory_capture_ids) == len(PRODUCT.reference_states)


def test_capture_commits_raw_bytes_before_any_parsing(monkeypatch) -> None:
    """Covers: ARC-002 — every response commits as a lossless capture."""
    _install_provider(monkeypatch)
    capturer = _Capturer()

    capture_product_release(
        lambda: None,
        PRODUCT,
        config=_config(),
        control=_Control(),
        persist_capture=capturer,
    )

    assert len(capturer.captures) == len(PRODUCT.subjects) + len(
        PRODUCT.reference_states
    )
    for capture in capturer.captures:
        assert capture.source_code == "FBI_UCR"
        assert capture.media_type == "application/json"
        assert capture.payload
        assert capture.payload_schema_version == PRODUCT.parser_contract_version


def test_captured_request_identity_never_carries_the_provider_key(
    monkeypatch,
) -> None:
    """Covers: ETL-038 — fingerprints and captures stay free of the secret."""
    _install_provider(monkeypatch)
    control = _Control()
    capturer = _Capturer()

    capture_product_release(
        lambda: None,
        PRODUCT,
        config=_config(),
        control=control,
        persist_capture=capturer,
    )

    for capture in capturer.captures:
        rendered = json.dumps(dict(capture.request_parameters))
        assert API_KEY not in rendered
        assert API_KEY_PARAMETER not in capture.request_parameters
        assert capture.request_fingerprint
    for _kind, payload in [item for item in control.events if item[0] == "request"]:
        assert API_KEY_PARAMETER not in payload[1]


def test_release_watermark_records_provider_freshness(monkeypatch) -> None:
    """Covers: ETL-026 — the run watermark carries the provider release."""
    _install_provider(monkeypatch)
    control = _Control()

    release = capture_product_release(
        lambda: None,
        PRODUCT,
        config=_config(),
        control=control,
        persist_capture=_Capturer(),
    )

    assert control.watermarks == [
        {
            "product_id": PRODUCT.product_id,
            "refresh_date": "2026-08-15",
            "max_data_month": "08/2026",
        }
    ]
    assert release.release_key == "2026-08-15"


def test_unchanged_release_captures_nothing_beyond_the_probe(monkeypatch) -> None:
    """Covers: DB-007 — an unchanged refresh does not recapture the release."""
    _install_provider(monkeypatch)
    control = _Control()
    capturer = _Capturer()
    from datetime import date

    release = capture_product_release(
        lambda: None,
        PRODUCT,
        previous_release=FbiRelease(date(2026, 8, 15), "08/2026"),
        config=_config(),
        control=control,
        persist_capture=capturer,
    )

    assert release.decision is ReleaseDecision.UNCHANGED
    assert release.observation_capture_ids == ()
    assert release.directory_capture_ids == ()
    assert len(capturer.captures) == 1
    assert control.run_status == "success"


def test_regressed_refresh_quarantines_the_capture(monkeypatch) -> None:
    """Covers: ETL-026 — a backward refresh is captured but not published."""
    _install_provider(monkeypatch)
    control = _Control()
    from datetime import date

    release = capture_product_release(
        lambda: None,
        PRODUCT,
        previous_release=FbiRelease(date(2026, 12, 1), "12/2026"),
        config=_config(),
        control=control,
        persist_capture=_Capturer(),
    )

    assert release.decision is ReleaseDecision.BACKWARD_REFRESH_QUARANTINE
    assert not release.complete
    assert control.quarantined == ["backward_refresh_quarantine"]
    assert control.run_status == "partial"


def test_unidentifiable_release_is_captured_then_quarantined(monkeypatch) -> None:
    """Covers: RES-002 — a payload with no release identity cannot publish."""
    _install_provider(monkeypatch, national="provider_error_body")
    control = _Control()
    capturer = _Capturer()

    release = capture_product_release(
        lambda: None,
        PRODUCT,
        config=_config(),
        control=control,
        persist_capture=capturer,
    )

    assert release.decision is ReleaseDecision.MISSING_RELEASE_QUARANTINE
    assert release.release is None
    assert not release.complete
    assert len(capturer.captures) == 1
    assert control.quarantined == ["missing_release_quarantine"]


def test_provider_failure_marks_the_request_and_run_failed(monkeypatch) -> None:
    """Covers: ETL-021 — a failed slice leaves explicit control-plane state."""

    def explode(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(fbi_capture, "fetch_summarized_observations", explode)
    control = _Control()

    with pytest.raises(RuntimeError, match="provider unavailable"):
        capture_product_release(
            lambda: None,
            PRODUCT,
            config=_config(),
            control=control,
            persist_capture=_Capturer(),
        )

    assert ("finish_request", "failed") in control.events
    assert control.run_status == "failed"


def test_slice_keys_round_trip_to_their_subject_identity() -> None:
    """Covers: ETL-002 — a slice key rebuilds exactly one subject."""
    for subject in PRODUCT.subjects:
        assert subject_from_slice_key(subject.slice_key) == subject
