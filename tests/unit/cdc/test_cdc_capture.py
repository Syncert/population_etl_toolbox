"""Capture-first CDC release orchestration contracts."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from data_ingestion_toolbox.cdc.capture import capture_asset_release
from data_ingestion_toolbox.cdc.config import CdcConfig
from data_ingestion_toolbox.cdc.metadata import MetadataDecision
from data_ingestion_toolbox.cdc.registry import CDI_ASSET
from tests.unit.cdc._doubles import ScriptedSocrataClient, socrata_response

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "cdc"


class FakeControl:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.run_id = UUID("00000000-0000-0000-0000-000000000201")
        self.request_index = 0
        self.finished_run: str | None = None
        self.watermark: dict[str, object] | None = None

    def start_run(self, *, watermark=None):  # noqa: ANN001
        self.events.append("start_run")
        return self.run_id

    def set_run_watermark(self, run_id, *, watermark):  # noqa: ANN001
        self.events.append("set_watermark")
        self.watermark = dict(watermark)

    def start_request(self, **_kwargs):  # noqa: ANN003
        from data_ingestion_toolbox.capture import ControlRequest

        self.request_index += 1
        self.events.append(f"start_request:{self.request_index}")
        request_id = UUID(f"00000000-0000-0000-0000-{self.request_index:012d}")
        return ControlRequest(request_id, "a" * 64)

    def record_request_retry(self, request_id, *, error):  # noqa: ANN001
        self.events.append("retry")

    def finish_request(self, request_id, *, status, error=None):  # noqa: ANN001
        self.events.append(f"finish_request:{status}")

    def quarantine(self, **_kwargs):  # noqa: ANN003
        self.events.append("quarantine")

    def finish_run(self, run_id, *, status, error=None):  # noqa: ANN001
        self.events.append(f"finish_run:{status}")
        self.finished_run = status


def test_metadata_and_pages_commit_before_parsing_or_exposure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-022 — every CDC response commits before parsing."""
    events: list[str] = []
    control = FakeControl(events)
    metadata = (FIXTURE_DIR / "cdi_metadata.json").read_bytes()
    observations = (FIXTURE_DIR / "cdi_observations.json").read_bytes()
    client = ScriptedSocrataClient(
        [socrata_response(200, raw=metadata), socrata_response(200, raw=observations)]
    )

    from data_ingestion_toolbox.cdc import capture as capture_module

    real_parse = capture_module.parse_metadata

    def parse_after_commit(payload, asset):  # noqa: ANN001
        assert events[-1] == "finish_request:captured"
        events.append("parse_metadata")
        return real_parse(payload, asset)

    monkeypatch.setattr(capture_module, "parse_metadata", parse_after_commit)

    def persist(_factory, response_capture):  # noqa: ANN001
        from data_ingestion_toolbox.capture import CaptureReceipt

        events.append("persist")
        return CaptureReceipt(
            response_capture.capture_id, response_capture.payload_checksum
        )

    release = capture_asset_release(
        lambda: None,
        CDI_ASSET,
        config=CdcConfig(
            socrata_app_token="",
            socrata_page_size=100,
            socrata_min_spacing_seconds=0,
        ),
        client=client,
        control=control,
        persist_capture=persist,
    )

    assert release.decision is MetadataDecision.INGEST
    assert release.complete is True
    assert release.row_count == 3
    assert len(release.page_capture_ids) == 1
    assert events.index("persist") < events.index("parse_metadata")
    assert control.watermark == {"asset_id": "cdi", "release_watermark": 1780605223}
    assert control.finished_run == "success"


def test_capture_failure_marks_request_and_run_failed_without_exposing_bytes() -> None:
    """Covers: RES-002 — an incomplete CDC release never reports success."""
    events: list[str] = []
    control = FakeControl(events)
    metadata = (FIXTURE_DIR / "cdi_metadata.json").read_bytes()
    client = ScriptedSocrataClient(
        [socrata_response(200, raw=metadata), socrata_response(200, raw=b"not-json")]
    )

    def persist(_factory, response_capture):  # noqa: ANN001
        from data_ingestion_toolbox.capture import CaptureReceipt

        return CaptureReceipt(
            response_capture.capture_id, response_capture.payload_checksum
        )

    with pytest.raises(Exception, match="invalid_json"):
        capture_asset_release(
            lambda: None,
            CDI_ASSET,
            config=CdcConfig(
                socrata_app_token="",
                socrata_min_spacing_seconds=0,
            ),
            client=client,
            control=control,
            persist_capture=persist,
        )

    assert "finish_request:failed" in events
    assert control.finished_run == "failed"
