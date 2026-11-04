"""A BLS slice is answered only for the request it was captured with.

Covers: ETL-113
"""

from __future__ import annotations

import hashlib
from typing import Any

import httpx
import pytest

from data_ingestion_toolbox.bls import ingest

pytestmark = pytest.mark.unit

_SERIES = ["LAUCN550250000000003", "LAUCN550250000000006", "LAUST550000000000003"]


def _legacy_digest(series_ids: list[str]) -> str:
    """The digest the DAG recorded before the request options were part of it."""
    return hashlib.sha256("|".join(sorted(series_ids)).encode("utf-8")).hexdigest()


def test_the_fingerprint_covers_the_request_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-113 — history captured without annual averages is not today's answer."""
    current = ingest.slice_fingerprint(_SERIES)

    # Order of the series does not matter; the options do.
    assert ingest.slice_fingerprint(list(reversed(_SERIES))) == current
    assert current != _legacy_digest(_SERIES)

    monkeypatch.setattr(ingest, "BLS_REQUEST_OPTIONS", {"annualaverage": False})
    assert ingest.slice_fingerprint(_SERIES) != current


class _Recorder:
    last: dict[str, Any] = {}

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        return None

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


def test_every_request_sends_the_fingerprinted_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-113 — the options fingerprinted are the options sent."""
    monkeypatch.setattr(ingest.CONFIG, "bls_api_key", "fixture-key-not-a-secret")
    monkeypatch.setattr(ingest.CONFIG, "bls_api_min_spacing_seconds", 0.0)
    monkeypatch.setattr(ingest.httpx, "Client", _Recorder)
    monkeypatch.setattr(ingest.time, "sleep", lambda _seconds: None)
    ingest.fetch_bls_api(["LAUCN550250000000003"], 1990, 2009)
    for key, value in ingest.BLS_REQUEST_OPTIONS.items():
        assert _Recorder.last[key] == value
    assert ingest.BLS_REQUEST_OPTIONS["annualaverage"] is True
