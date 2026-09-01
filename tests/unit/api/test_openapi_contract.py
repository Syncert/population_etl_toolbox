"""API unit tests: the public OpenAPI contract is pinned to a reviewed snapshot.

Covers: API-031 (public contract snapshot), API-032 (every public resource is
        served under the current version and nowhere else), API-033 (the
        unversioned deployment probes stay outside the version policy).

API-002 restructures routers, schemas, and services. That refactor is only safe
if a consumer-visible change cannot pass unnoticed, so the contract itself is
the characterization test: every operation, parameter bound, and response schema
is frozen in ``tests/fixtures/api/openapi_contract.json``.

The snapshot is reviewed evidence, not a cache. Regenerate it deliberately with
``python -m tests.support.regenerate_openapi_contract`` and read the diff; a
snapshot updated to make a red test green is exactly the failure this guards.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.versioning import CURRENT_VERSION, UNVERSIONED_PATHS
from tests.support.openapi_contract import contract_digest, describe_difference

SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[2] / "fixtures" / "api" / "openapi_contract.json"
)


def _reviewed_digest() -> dict:
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


@pytest.mark.unit
@pytest.mark.api
def test_public_contract_matches_the_reviewed_snapshot() -> None:
    """Covers: API-031 — no operation, bound, or schema field drifts unreviewed."""
    current = contract_digest(app.openapi())
    reviewed = _reviewed_digest()
    assert current == reviewed, (
        "The public API contract changed:\n"
        + describe_difference(reviewed, current)
        + "\n\nIf the change is intended, regenerate the snapshot and review the "
        "diff as part of the change."
    )


@pytest.mark.unit
@pytest.mark.api
def test_every_public_resource_is_served_only_under_the_current_version() -> None:
    """Covers: API-032 — one public surface, no unversioned aliases left.

    API-008 retired the ``/api`` aliases. Every ``/api`` path must now name a
    supported version; an unversioned data route reappearing would be a second
    surface that could drift from the promised one.
    """
    digest = contract_digest(app.openapi())
    served = {key.split(" ", 1)[1] for key in digest["operations"]}

    api_paths = {path for path in served if path.startswith("/api/")}
    assert api_paths, "no API routes are served"

    unversioned = sorted(
        path for path in api_paths if not path.startswith(f"/api/{CURRENT_VERSION}/")
    )
    assert not unversioned, f"unversioned API routes are served again: {unversioned}"


@pytest.mark.unit
@pytest.mark.api
def test_deployment_probes_stay_outside_the_version_policy() -> None:
    """Covers: API-033 — the container probes are not versioned resources."""
    client = TestClient(app)

    for path in sorted(UNVERSIONED_PATHS):
        probe = client.get(path)
        assert probe.status_code in {200, 503}, path
        assert "deprecation" not in probe.headers, path
        assert "sunset" not in probe.headers, path

    assert client.get("/health").json()["status"] == "ok"


@pytest.mark.unit
@pytest.mark.api
def test_no_response_carries_a_retirement_signal() -> None:
    """Covers: API-033 — nothing is deprecated, so nothing announces it."""
    client = TestClient(app)

    for path in (
        f"/api/{CURRENT_VERSION}/health",
        f"/api/{CURRENT_VERSION}/catalog/capabilities",
    ):
        response = client.get(path)
        assert response.status_code == 200, path
        assert "deprecation" not in response.headers, path
        assert "sunset" not in response.headers, path
        assert "successor-version" not in response.headers.get("link", ""), path


@pytest.mark.unit
@pytest.mark.api
def test_retired_aliases_are_not_served() -> None:
    """Covers: API-032 — a legacy path is a 404, not a quiet second surface."""
    client = TestClient(app)

    for legacy in ("/api/health", "/api/catalog/metrics", "/api/observations"):
        assert client.get(legacy).status_code == 404, legacy
