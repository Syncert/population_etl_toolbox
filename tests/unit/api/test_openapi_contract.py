"""API unit tests: the public OpenAPI contract is pinned to a reviewed snapshot.

Covers: API-031 (public contract snapshot), API-032 (versioned and legacy route
        parity), API-033 (deprecation signalling on legacy routes).

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
from apps.api.versioning import (
    CURRENT_VERSION,
    LEGACY_DEPRECATION_HEADER,
    LEGACY_SUNSET_HEADER,
    legacy_path_for,
    versioned_path_for,
)
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
def test_every_public_route_is_served_under_the_current_version() -> None:
    """Covers: API-032 — the versioned surface is complete, not partial."""
    digest = contract_digest(app.openapi())
    served = {key.split(" ", 1)[1] for key in digest["operations"]}

    legacy_paths = {path for path in served if path.startswith("/api/")}
    legacy_paths -= {path for path in legacy_paths if path.startswith("/api/v")}

    missing = {path for path in legacy_paths if versioned_path_for(path) not in served}
    assert not missing, (
        f"legacy routes with no /{CURRENT_VERSION} equivalent: {sorted(missing)}"
    )


@pytest.mark.unit
@pytest.mark.api
def test_versioned_and_legacy_routes_accept_identical_requests() -> None:
    """Covers: API-032 — an alias is the same operation, not a similar one."""
    digest = contract_digest(app.openapi())
    operations = digest["operations"]

    compared = 0
    for key, versioned in operations.items():
        method, path = key.split(" ", 1)
        if not path.startswith(f"/api/{CURRENT_VERSION}/"):
            continue
        legacy_key = f"{method} {legacy_path_for(path)}"
        if legacy_key not in operations:
            continue
        legacy = operations[legacy_key]
        assert versioned["parameters"] == legacy["parameters"], (
            f"{key} and {legacy_key} disagree on request parameters"
        )
        assert versioned["responses"] == legacy["responses"], (
            f"{key} and {legacy_key} disagree on response schemas"
        )
        compared += 1

    assert compared > 0, "no versioned/legacy pair was compared"


@pytest.mark.unit
@pytest.mark.api
def test_legacy_routes_announce_their_deprecation_and_successor() -> None:
    """Covers: API-033 — a legacy consumer learns it is on a retiring route."""
    client = TestClient(app)

    legacy = client.get("/api/health")
    assert legacy.status_code == 200
    assert legacy.headers[LEGACY_DEPRECATION_HEADER] == "true"
    assert legacy.headers[LEGACY_SUNSET_HEADER]
    assert f"/api/{CURRENT_VERSION}/health" in legacy.headers["link"]


@pytest.mark.unit
@pytest.mark.api
def test_versioned_routes_are_not_marked_deprecated() -> None:
    """Covers: API-033 — the successor must not inherit the retirement signal."""
    client = TestClient(app)

    versioned = client.get(f"/api/{CURRENT_VERSION}/health")
    assert versioned.status_code == 200
    assert LEGACY_DEPRECATION_HEADER not in versioned.headers
    assert LEGACY_SUNSET_HEADER not in versioned.headers


@pytest.mark.unit
@pytest.mark.api
def test_unversioned_health_alias_still_serves_deployment_probes() -> None:
    """Covers: API-033 — the container probe path is outside the version policy."""
    client = TestClient(app)

    probe = client.get("/health")
    assert probe.status_code == 200
    assert probe.json()["status"] == "ok"
    assert LEGACY_DEPRECATION_HEADER not in probe.headers
