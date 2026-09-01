"""API unit tests: health endpoint.

Migrated from apps/api/tests/test_health.py.
Covers: API-001 (health aliases), API-002 (security headers).
"""

import pytest
from fastapi.testclient import TestClient

from apps.api.main import app


@pytest.mark.unit
@pytest.mark.api
def test_health_route_returns_ok() -> None:
    """Covers: API-001 — /health returns the stable healthy contract."""
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "data-ingestion-toolbox-api"


@pytest.mark.unit
@pytest.mark.api
def test_health_returns_security_headers() -> None:
    """Covers: API-002 — health responses include security headers."""
    client = TestClient(app)
    response = client.get("/health")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"


@pytest.mark.unit
@pytest.mark.api
def test_api_health_alias_returns_ok() -> None:
    """Covers: API-001 — /api/v1/health returns the healthy contract."""
    client = TestClient(app)
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
