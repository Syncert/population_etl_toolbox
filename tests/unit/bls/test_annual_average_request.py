"""BLS requests its own annual averages.

Covers: ETL-072
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from data_ingestion_toolbox.bls import ingest

pytestmark = pytest.mark.unit


class _Recorder:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.posted: list[dict[str, Any]] = []

    def __enter__(self) -> "_Recorder":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
        _Recorder.last = json
        return httpx.Response(
            200,
            json={"status": "REQUEST_SUCCEEDED", "Results": {"series": []}},
            request=httpx.Request("POST", url),
        )


def test_every_request_asks_bls_for_its_annual_averages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-072 — `annualaverage=true`, so BLS's `M13` rows arrive as provider facts."""
    monkeypatch.setattr(ingest.CONFIG, "bls_api_key", "fixture-key-not-a-secret")
    monkeypatch.setattr(ingest.CONFIG, "bls_api_min_spacing_seconds", 0.0)
    monkeypatch.setattr(ingest.httpx, "Client", _Recorder)
    monkeypatch.setattr(ingest.time, "sleep", lambda _seconds: None)
    ingest.fetch_bls_api(["LNU04000000"], 2024, 2024)
    assert _Recorder.last["annualaverage"] is True
    assert _Recorder.last["seriesid"] == ["LNU04000000"]
