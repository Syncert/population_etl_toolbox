from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def one(self):
        return self._rows[0]

    def scalar(self):
        return self._scalar_value


class _DistributionSession:
    def execute(self, query, _params=None):
        sql = str(query).lower()

        if "min(value)" in sql and "max(value)" in sql:
            return _FakeResult(
                rows=[
                    {
                        "total": 3,
                        "min_value": 10.0,
                        "max_value": 40.0,
                    }
                ]
            )

        if "width_bucket" in sql:
            return _FakeResult(
                rows=[
                    {"bin_index": 1, "count": 1},
                    {"bin_index": 4, "count": 2},
                ]
            )

        return _FakeResult(rows=[])


def test_distribution_accepts_metric_id_alias() -> None:
    def _override_db():
        yield _DistributionSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/distribution/bins", params={"metric_id": "POP_TOTAL", "bin_count": 7})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert len(payload["items"]) == 2


def test_distribution_requires_metric_code_or_metric_id() -> None:
    def _override_db():
        yield _DistributionSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/distribution/bins", params={"bin_count": 7})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    payload = response.json()
    assert payload["detail"] == "metric_code or metric_id is required"
