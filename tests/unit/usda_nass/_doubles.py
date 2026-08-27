"""Deterministic doubles for USDA NASS transport and control-plane tests."""

from __future__ import annotations

import hashlib
import json
import pathlib
from typing import Any
from uuid import UUID, uuid4

import httpx

from data_ingestion_toolbox.capture import CaptureReceipt, ResponseCapture
from data_ingestion_toolbox.usda_nass.config import NassConfig

FIXTURE_DIR = pathlib.Path(__file__).resolve().parents[2] / "fixtures" / "usda_nass"


def load_fixture(name: str) -> dict[str, Any]:
    """Load one reviewed USDA NASS fixture document by stem or filename."""
    path = FIXTURE_DIR / (name if name.endswith(".json") else name + ".json")
    return json.loads(path.read_text(encoding="utf-8"))


def deterministic_config(**overrides: Any) -> NassConfig:
    """Build a configuration that never sleeps and always has a valid key."""
    values: dict[str, Any] = {
        "usda_nass_api_key": "FIXTURE-KEY-0000-1111-2222-3333-4444",
        "request_min_spacing_seconds": 0.0,
        "request_max_attempts": 3,
    }
    values.update(overrides)
    return NassConfig(**values)


class RecordingClient:
    """An httpx-shaped client that replays a scripted response sequence."""

    def __init__(self, responses: list[httpx.Response | BaseException]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        self.calls.append(
            {"url": url, "headers": dict(headers or {}), "params": dict(params or {})}
        )
        if not self._responses:
            raise AssertionError(f"no scripted USDA NASS response for {url}")
        item = self._responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self) -> None:
        self.closed = True


def json_response(
    payload: object,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Build a synthetic JSON response with a correct content length."""
    body = json.dumps(payload).encode("utf-8")
    merged = {"content-type": "application/json"}
    merged.update(headers or {})
    return httpx.Response(
        status,
        request=httpx.Request("GET", "https://quickstats.invalid/api/api_GET"),
        headers=merged,
        content=body,
    )


class RecordingCapture:
    """Capture sink that records commit order without touching a database."""

    def __init__(self) -> None:
        self.captures: list[ResponseCapture] = []

    def __call__(
        self,
        _connection_factory: Any,
        capture: ResponseCapture,
    ) -> CaptureReceipt:
        self.captures.append(capture)
        return CaptureReceipt(capture.capture_id, capture.payload_checksum)

    @property
    def endpoints(self) -> list[str]:
        return [capture.endpoint for capture in self.captures]

    @property
    def payloads(self) -> list[bytes]:
        return [capture.payload for capture in self.captures]


class RecordingControl:
    """Control-plane double recording every committed run/request transition."""

    def __init__(self) -> None:
        self.source_code = "USDA_NASS"
        self.events: list[tuple[str, Any]] = []
        self.quarantines: list[dict[str, Any]] = []
        self.run_id: UUID | None = None

    def start_run(self, *, watermark: dict[str, Any] | None = None) -> UUID:
        self.run_id = uuid4()
        self.events.append(("start_run", watermark))
        return self.run_id

    def finish_run(self, run_id: UUID, *, status: str, error: object = None) -> None:
        self.events.append(("finish_run", status))

    def set_run_watermark(self, run_id: UUID, *, watermark: dict[str, Any]) -> None:
        self.events.append(("set_run_watermark", watermark))

    def start_request(
        self,
        *,
        run_id: UUID,
        endpoint: str,
        parameters: dict[str, Any],
        max_attempts: int = 1,
    ) -> Any:
        request_id = uuid4()
        self.events.append(("start_request", (endpoint, dict(parameters))))

        class _Request:
            def __init__(self, identifier: UUID) -> None:
                self.request_id = identifier
                self.request_fingerprint = hashlib.sha256(endpoint.encode()).hexdigest()

        return _Request(request_id)

    def finish_request(
        self, request_id: UUID, *, status: str, error: object = None
    ) -> None:
        self.events.append(("finish_request", status))

    def record_request_retry(self, request_id: UUID, *, error: object) -> None:
        self.events.append(("record_request_retry", str(error)))

    def quarantine(
        self,
        *,
        capture_id: UUID,
        run_id: UUID,
        parser_version: str,
        error_code: str,
        error: object,
    ) -> None:
        self.quarantines.append(
            {
                "capture_id": capture_id,
                "parser_version": parser_version,
                "error_code": error_code,
            }
        )
        self.events.append(("quarantine", error_code))

    def request_parameter_sets(self) -> list[dict[str, Any]]:
        return [payload[1] for name, payload in self.events if name == "start_request"]
