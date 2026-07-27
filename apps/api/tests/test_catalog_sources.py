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


def _override_db():
    yield _FakeSession()


def test_catalog_sources_route_without_real_db() -> None:
    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/catalog/sources")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["source_code"] == "BLS"


class _FailingSession:
    def execute(self, _query, _params=None):
        raise SQLAlchemyError("password=super-secret")


def _override_failing_db():
    yield _FailingSession()


def test_catalog_sources_db_error_is_sanitized() -> None:
    app.dependency_overrides[get_db_session_dep] = _override_failing_db
    try:
        client = TestClient(app)
        response = client.get("/api/catalog/sources")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    payload = response.json()
    assert payload["detail"] == "Database service is temporarily unavailable."
    assert "password=super-secret" not in response.text
