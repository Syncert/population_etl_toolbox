"""API unit tests: catalog sources endpoint.

Migrated from apps/api/tests/test_catalog_sources.py.
Covers: API-016 (database unavailable returns 503 without leaking detail).
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def execute(self, _query, _params=None):
        return _FakeResult(
            [
                {
                    "source_code": "BLS",
                    "source_name": "Bureau of Labor Statistics",
                    "source_type": "PRIMARY",
                    "reference_url": "https://www.bls.gov/",
                }
            ]
        )


class _FailingSession:
    def execute(self, _query, _params=None):
        raise SQLAlchemyError("******")


@pytest.mark.unit
@pytest.mark.api
def test_catalog_sources_route_without_real_db() -> None:
    """Covers: API-025 — catalog sources returns stable metadata with a fake DB."""

    def _override_db():
        yield _FakeSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/v1/catalog/sources")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["source_code"] == "BLS"


@pytest.mark.unit
@pytest.mark.api
def test_catalog_sources_db_error_is_sanitized() -> None:
    """Covers: API-016 — database errors return only the safe 503 detail."""

    def _failing_override_db():
        yield _FailingSession()

    app.dependency_overrides[get_db_session_dep] = _failing_override_db
    try:
        client = TestClient(app)
        response = client.get("/api/v1/catalog/sources")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    payload = response.json()
    assert payload["detail"] == "Database service is temporarily unavailable."
    assert "******" not in response.text
