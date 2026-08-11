"""API unit tests: distribution endpoint.

Migrated from apps/api/tests/test_distribution.py.
Covers: API-003 (required metric input), API-014 (distribution bins).
"""

import pytest
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
                rows=[{"total": 3, "min_value": 10.0, "max_value": 40.0}]
            )

        if "width_bucket" in sql:
            bin_count = int(_params["bin_count"])
            if bin_count == 1:
                return _FakeResult(rows=[{"bin_index": 1, "count": 3}])
            return _FakeResult(
                rows=[
                    {"bin_index": 1, "count": 1},
                    {"bin_index": bin_count, "count": 2},
                ]
            )

        return _FakeResult(rows=[])


@pytest.mark.unit
@pytest.mark.api
def test_distribution_accepts_metric_id_alias() -> None:
    """Covers: API-004, API-014 — a metric alias returns distribution bins."""

    def _override_db():
        yield _DistributionSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/distribution/bins",
            params={"metric_id": "POP_TOTAL", "bin_count": 7},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert len(payload["items"]) == 2


@pytest.mark.unit
@pytest.mark.api
def test_distribution_requires_metric_code_or_metric_id() -> None:
    """Covers: API-003 — distribution requires a metric identifier."""

    def _override_db():
        yield _DistributionSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/distribution/bins", params={"bin_count": 7})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code or metric_id is required"


@pytest.mark.unit
@pytest.mark.api
@pytest.mark.parametrize("bin_count", [1, 20])
def test_distribution_bin_boundaries_and_counts(bin_count: int) -> None:
    """Covers: API-014 — supported bin boundaries reconcile counts."""

    def _override_db():
        yield _DistributionSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        response = TestClient(app).get(
            "/api/distribution/bins",
            params={"metric_code": "POP_TOTAL", "bin_count": bin_count},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["bin_count"] == bin_count
    assert sum(item["count"] for item in payload["items"]) == payload["total"] == 3


@pytest.mark.unit
@pytest.mark.api
@pytest.mark.parametrize("bin_count", [0, 21])
def test_distribution_invalid_bin_counts_are_rejected(bin_count: int) -> None:
    """Covers: API-014 — invalid bin counts fail before database work."""

    def _override_db():
        yield _DistributionSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        response = TestClient(app).get(
            "/api/distribution/bins",
            params={"metric_code": "POP_TOTAL", "bin_count": bin_count},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422
