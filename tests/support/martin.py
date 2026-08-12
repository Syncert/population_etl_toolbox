"""Configuration and decoding helpers for disposable Martin contracts."""

from __future__ import annotations

import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MARTIN_VERSION = "1.11.0"
MARTIN_IMAGE = (
    "ghcr.io/maplibre/martin:1.11.0@"
    "sha256:0650e9025f5fcffdc686358114679421b5e6b0ca37b374ad8a66f14709d59d2b"
)
MARTIN_CONTAINER_NAME = "population-testing-martin-1"
MARTIN_DOCKER_NETWORK = "population-testing_default"
MARTIN_TEST_ROLE = "martin_test"
MARTIN_TEST_PASSWORD = "martin-test-readonly"
SEEDED_GEO_ID = "state:55|county:025"
SEEDED_LONGITUDE = -89.4
SEEDED_LATITUDE = 43.0667


def _safe_loopback_url(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError(f"{name} must use http or https")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError(f"{name} must target a loopback host")
    if parsed.username or parsed.password:
        raise RuntimeError(f"{name} must not contain credentials")
    return value.rstrip("/")


@dataclass(frozen=True)
class MartinTestConfig:
    """Loopback endpoints exposed by the disposable integration stack."""

    direct_url: str
    proxy_url: str
    container_name: str
    docker_network: str

    @classmethod
    def from_environment(cls) -> "MartinTestConfig | None":
        if os.environ.get("RUN_MARTIN_TESTS") != "1":
            return None
        return cls(
            direct_url=_safe_loopback_url("TEST_MARTIN_URL", "http://127.0.0.1:33000"),
            proxy_url=_safe_loopback_url(
                "TEST_MARTIN_PROXY_URL", "http://127.0.0.1:33001/tiles"
            ),
            container_name=os.environ.get(
                "TEST_MARTIN_CONTAINER_NAME", MARTIN_CONTAINER_NAME
            ),
            docker_network=os.environ.get(
                "TEST_MARTIN_DOCKER_NETWORK", MARTIN_DOCKER_NETWORK
            ),
        )


def request_json(url: str, timeout: float = 5.0) -> dict:
    """Fetch one local JSON contract with a bounded timeout."""
    response = httpx.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise AssertionError(f"Expected a JSON object from {url}")
    return payload


def tile_for_coordinate(
    longitude: float, latitude: float, zoom: int = 8
) -> tuple[int, int, int]:
    """Return the XYZ tile containing one WGS84 coordinate."""
    scale = 2**zoom
    x = int((longitude + 180.0) / 360.0 * scale)
    latitude_radians = math.radians(latitude)
    y = int((1.0 - math.asinh(math.tan(latitude_radians)) / math.pi) / 2.0 * scale)
    return zoom, x, y


def decode_mvt(payload: bytes) -> dict:
    """Decode an MVT payload through the pinned test dependency."""
    from mapbox_vector_tile import decode

    result = decode(payload)
    if not isinstance(result, dict):
        raise AssertionError("MVT decoder returned a non-object payload")
    return result


def docker(*arguments: str, timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
    """Run one bounded Docker inspection against the named test stack."""
    return subprocess.run(
        ["docker", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
