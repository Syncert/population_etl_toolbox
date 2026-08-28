"""USDA NASS capture orchestration, preflight gating, and quarantine paths."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from data_ingestion_toolbox.usda_nass.capture import (
    SLICE_CAPTURED,
    SLICE_EMPTY,
    SLICE_OVER_LIMIT,
    SLICE_PARTIAL,
    SLICE_SKIPPED,
    capture_product_release,
    resolve_slice_mode,
)
from data_ingestion_toolbox.usda_nass.client import (
    API_COUNT_PATH,
    API_DATA_PATH,
    NassDataResponse,
    NassOverLimitError,
)
from data_ingestion_toolbox.usda_nass.metadata import ReleaseDecision
from data_ingestion_toolbox.usda_nass.registry import get_product, iter_slices

from ._doubles import (
    RecordingCapture,
    RecordingControl,
    deterministic_config,
    load_fixture,
)

pytestmark = pytest.mark.unit

PRODUCT = get_product("corn_survey_annual")
SECRET = "SECRET-KEY-9999-8888-7777-6666-5555"


class _Provider:
    """Serves the reviewed corn sample for every registered slice."""

    def __init__(self, *, over_limit_levels: tuple[str, ...] = ()) -> None:
        self.document = load_fixture("corn_survey_annual")
        self.over_limit_levels = over_limit_levels
        self.count_calls: list[str] = []
        self.data_calls: list[str] = []

    def rows(self, level: str) -> list[dict[str, Any]]:
        return self.document["slices"][level]["data"]["data"]

    def count(self, product: Any, item: Any, **_kwargs: Any) -> Any:
        from data_ingestion_toolbox.usda_nass.client import (
            NassCountResponse,
            count_parameters,
        )

        self.count_calls.append(item.slice_key)
        total = (
            10**6
            if item.agg_level_desc in self.over_limit_levels
            else len(self.rows(item.agg_level_desc))
        )
        payload = json.dumps({"count": str(total)}).encode("utf-8")
        return NassCountResponse(
            count_parameters(product, item),
            payload,
            {"content-type": "application/json"},
            200,
            total,
        )

    def records(self, product: Any, item: Any, **_kwargs: Any) -> NassDataResponse:
        from data_ingestion_toolbox.usda_nass.client import data_parameters

        self.data_calls.append(item.slice_key)
        rows = self.rows(item.agg_level_desc)
        payload = json.dumps({"data": rows}).encode("utf-8")
        return NassDataResponse(
            data_parameters(product, item),
            payload,
            {"content-type": "application/json"},
            200,
            len(rows),
        )


def _install(monkeypatch: pytest.MonkeyPatch, provider: _Provider) -> None:
    from data_ingestion_toolbox.usda_nass import capture as capture_module

    monkeypatch.setattr(capture_module, "fetch_slice_count", provider.count)
    monkeypatch.setattr(capture_module, "fetch_slice_records", provider.records)


def _run(
    monkeypatch: pytest.MonkeyPatch,
    provider: _Provider,
    **kwargs: Any,
) -> tuple[Any, RecordingControl, RecordingCapture]:
    _install(monkeypatch, provider)
    control = RecordingControl()
    sink = RecordingCapture()
    release = capture_product_release(
        lambda: None,
        PRODUCT,
        config=kwargs.pop("config", deterministic_config()),
        control=control,
        persist_capture=sink,
        **kwargs,
    )
    return release, control, sink


def test_every_response_commits_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-022 — every USDA NASS response commits before parsing."""
    provider = _Provider()
    release, control, sink = _run(monkeypatch, provider, mode="recent")

    expected = iter_slices(PRODUCT, mode="recent")
    assert provider.count_calls == [item.slice_key for item in expected]
    assert provider.data_calls == [item.slice_key for item in expected]
    assert len(sink.captures) == 2 * len(expected)
    assert sink.endpoints[:2] == [API_COUNT_PATH, API_COUNT_PATH]

    # The watermark, and therefore any parsing, is recorded only after the
    # final capture has been committed.
    committed = [name for name, _ in control.events]
    assert committed.index("set_run_watermark") > committed.index("finish_request")
    assert release.decision is ReleaseDecision.INGEST
    assert release.complete is True


def test_captured_parameters_and_watermark_never_carry_a_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-038 — captured parameters never carry the API key."""
    provider = _Provider()
    release, control, sink = _run(
        monkeypatch,
        provider,
        mode="recent",
        config=deterministic_config(usda_nass_api_key=SECRET),
    )

    for capture in sink.captures:
        assert "key" not in capture.request_parameters
        assert SECRET not in json.dumps(dict(capture.request_parameters))
        assert SECRET not in capture.payload.decode("utf-8")
    for parameters in control.request_parameter_sets():
        assert "key" not in parameters
    assert SECRET not in json.dumps(release.contract.slice_counts)


def test_a_preflighted_release_records_every_registered_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-020 — every registered slice is recorded, not just fetched."""
    provider = _Provider()
    release, _control, _sink = _run(monkeypatch, provider, mode="full")

    expected = iter_slices(PRODUCT, mode="full")
    assert [item.slice_key for item in release.slices] == [
        item.slice_key for item in expected
    ]
    assert {item.status for item in release.slices} == {SLICE_CAPTURED}
    assert release.slice_mode == "full"
    assert all(item.count_capture_id is not None for item in release.slices)
    assert all(item.data_capture_id is not None for item in release.slices)
    assert release.row_count == sum(item.captured_row_count for item in release.slices)


def test_an_over_limit_preflight_refuses_retrieval_entirely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: RES-002 — an over-limit slice never issues a data request."""
    provider = _Provider(over_limit_levels=("COUNTY",))
    release, control, _sink = _run(monkeypatch, provider, mode="recent")

    assert provider.data_calls == []
    assert release.decision is ReleaseDecision.OVER_LIMIT_QUARANTINE
    assert release.complete is False
    assert release.row_count == 0
    assert SLICE_OVER_LIMIT in {item.status for item in release.slices}
    assert control.quarantines[0]["error_code"] == "over_limit_quarantine"
    assert ("finish_run", "partial") in control.events


def test_a_provider_over_limit_refusal_marks_the_release_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: RES-002 — a refused retrieval cannot report a complete release."""
    provider = _Provider()

    def refuse(product: Any, item: Any, **kwargs: Any) -> Any:
        if item.agg_level_desc == "COUNTY":
            raise NassOverLimitError(API_DATA_PATH, code="record_limit_reached")
        return provider.records(product, item, **kwargs)

    from data_ingestion_toolbox.usda_nass import capture as capture_module

    _install(monkeypatch, provider)
    monkeypatch.setattr(capture_module, "fetch_slice_records", refuse)
    control = RecordingControl()
    release = capture_product_release(
        lambda: None,
        PRODUCT,
        mode="recent",
        config=deterministic_config(),
        control=control,
        persist_capture=RecordingCapture(),
    )

    assert release.decision is ReleaseDecision.PARTIAL_SLICE_QUARANTINE
    assert release.complete is False
    over_limit = [item for item in release.slices if item.status == SLICE_OVER_LIMIT]
    assert [item.agg_level_desc for item in over_limit] == ["COUNTY"]
    assert ("finish_request", "failed") in control.events


def test_a_short_retrieval_marks_the_slice_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: RES-002 — a retrieval short of its preflight cannot publish."""
    provider = _Provider()

    def short(product: Any, item: Any, **kwargs: Any) -> NassDataResponse:
        response = provider.records(product, item, **kwargs)
        if item.agg_level_desc != "STATE":
            return response
        rows = json.loads(response.raw_bytes)["data"][:1]
        return NassDataResponse(
            response.request_parameters,
            json.dumps({"data": rows}).encode("utf-8"),
            response.response_headers,
            response.http_status,
            len(rows),
        )

    from data_ingestion_toolbox.usda_nass import capture as capture_module

    _install(monkeypatch, provider)
    monkeypatch.setattr(capture_module, "fetch_slice_records", short)
    release = capture_product_release(
        lambda: None,
        PRODUCT,
        mode="recent",
        config=deterministic_config(),
        control=RecordingControl(),
        persist_capture=RecordingCapture(),
    )

    assert release.decision is ReleaseDecision.PARTIAL_SLICE_QUARANTINE
    assert release.complete is False
    assert SLICE_PARTIAL in {item.status for item in release.slices}


def test_an_empty_slice_is_recorded_without_a_data_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-020 — a zero-count slice issues no data request."""
    provider = _Provider()
    provider.document["slices"]["NATIONAL"]["data"]["data"] = []
    release, _control, _sink = _run(monkeypatch, provider, mode="recent")

    empty = [item for item in release.slices if item.status == SLICE_EMPTY]
    assert [item.agg_level_desc for item in empty] == ["NATIONAL"]
    assert all("NATIONAL" not in key for key in provider.data_calls)
    assert release.decision is ReleaseDecision.INGEST


def test_an_unchanged_preflight_skips_retrieval_entirely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-026 — an unchanged preflight retrieves nothing."""
    provider = _Provider()
    first, _control, _sink = _run(monkeypatch, provider, mode="recent")

    provider.data_calls.clear()
    second, control, sink = _run(
        monkeypatch, provider, mode="recent", previous_release=first.contract
    )

    assert second.decision is ReleaseDecision.UNCHANGED
    assert second.complete is True
    assert provider.data_calls == []
    assert {item.status for item in second.slices} == {SLICE_SKIPPED}
    assert ("finish_run", "success") in control.events
    assert len(sink.captures) == len(iter_slices(PRODUCT, mode="recent"))


def test_a_transport_failure_closes_the_run_and_the_open_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: RES-002 — a transport failure closes its run and request."""
    provider = _Provider()

    def explode(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("provider unavailable")

    _install(monkeypatch, provider)
    from data_ingestion_toolbox.usda_nass import capture as capture_module

    monkeypatch.setattr(capture_module, "fetch_slice_count", explode)
    control = RecordingControl()
    with pytest.raises(RuntimeError, match="provider unavailable"):
        capture_product_release(
            lambda: None,
            PRODUCT,
            mode="recent",
            config=deterministic_config(),
            control=control,
            persist_capture=RecordingCapture(),
        )
    assert ("finish_request", "failed") in control.events
    assert ("finish_run", "failed") in control.events


@pytest.mark.parametrize(
    ("day", "expected"),
    [(1, "full"), (2, "recent"), (15, "recent"), (28, "recent")],
)
def test_the_reconciliation_cadence_is_evidence_based(day: int, expected: str) -> None:
    """Covers: ETL-026 — the reconciliation cadence is registry-driven."""
    moment = datetime(2026, 4, day, 10, 0, tzinfo=timezone.utc)
    assert resolve_slice_mode(moment, deterministic_config()) == expected
    assert (
        resolve_slice_mode(
            moment, deterministic_config(full_reconciliation_day_of_month=28)
        )
        == "full"
    )
