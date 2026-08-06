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
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "data-ingestion-toolbox-api"


@pytest.mark.unit
@pytest.mark.api
def test_health_returns_security_headers() -> None:
    """API-002: every response includes required security headers."""
    client = TestClient(app)
    response = client.get("/health")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"


@pytest.mark.unit
@pytest.mark.api
def test_api_health_alias_returns_ok() -> None:
    """API-001: /api/health alias returns 200."""
    client = TestClient(app)
    response = client.get("/api/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
