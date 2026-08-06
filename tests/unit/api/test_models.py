"""API unit tests: models endpoint.

Migrated from apps/api/tests/test_models.py.
"""

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app


class _FakeResult:
    def __init__(self, scalar_value=None):
        self._scalar_value = scalar_value

    def scalar(self):
        return self._scalar_value


class _ModelsSession:
    def execute(self, query, params=None):
        relation_name = (params or {}).get("relation_name")
        if relation_name == "gold.v_metric_forecast":
            return _FakeResult(scalar_value=True)
        return _FakeResult(scalar_value=False)


@pytest.mark.unit
@pytest.mark.api
def test_models_status_surface() -> None:
    def _override_db():
        yield _ModelsSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/models/status")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert "status" in payload
    assert "models_enabled" in payload
    assert "details" in payload
