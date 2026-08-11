"""API unit tests: comparison endpoint.

Migrated from apps/api/tests/test_comparison.py.
Covers: API-003 (required metric input), API-015 (comparison results).
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

    def scalar(self):
        return self._scalar_value


class _ComparisonSession:
    def execute(self, query, _params=None):
        sql = str(query).lower()

        if "count(*)::int as total" in sql:
            return _FakeResult(scalar_value=2)

        if "from joined" in sql and "limit" in sql:
            return _FakeResult(
                rows=[
                    {
                        "geo_id": "06001",
                        "geo_level": "county",
                        "state_fips": "06",
                        "county_fips": "001",
                        "state_name": "California",
                        "county_name": "Alameda",
                        "metric_code_a": _params["metric_code_a"],
                        "metric_code_b": _params["metric_code_b"],
                        "value_a": 100.0,
                        "value_b": 10.0,
                        "difference": 90.0,
                        "ratio": 10.0,
                    }
                ]
            )

        return _FakeResult(rows=[])


@pytest.mark.unit
@pytest.mark.api
def test_comparison_accepts_metric_id_aliases() -> None:
    """Covers: API-004, API-015 — comparison aliases return paired values."""

    def _override_db():
        yield _ComparisonSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/comparison",
            params={
                "metric_id_a": "POP_TOTAL",
                "metric_id_b": "UNEMP_RATE",
                "limit": 10,
                "offset": 0,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert len(payload["items"]) == 1


@pytest.mark.unit
@pytest.mark.api
def test_comparison_requires_metric_a() -> None:
    """Covers: API-003 — comparison requires its first metric."""

    def _override_db():
        yield _ComparisonSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/comparison", params={"metric_id_b": "UNEMP_RATE"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code_a or metric_id_a is required"


@pytest.mark.unit
@pytest.mark.api
def test_comparison_requires_metric_b() -> None:
    """Covers: API-003 — comparison requires its second metric."""

    def _override_db():
        yield _ComparisonSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/comparison", params={"metric_id_a": "POP_TOTAL"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code_b or metric_id_b is required"


@pytest.mark.unit
@pytest.mark.api
@pytest.mark.parametrize(
    ("metric_a", "metric_b"),
    [
        ("FRED:UNRATE", "FRED:CIVPART"),
        ("ACS:acs5:B01003_001", "FRED:UNRATE"),
    ],
)
def test_comparison_aligns_same_and_cross_source_metrics(
    metric_a: str, metric_b: str
) -> None:
    """Covers: API-015 — metrics pair once by geography across sources."""

    def _override_db():
        yield _ComparisonSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        response = TestClient(app).get(
            "/api/comparison",
            params={"metric_code_a": metric_a, "metric_code_b": metric_b},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["metric_code_a"] == metric_a
    assert payload["metric_code_b"] == metric_b
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    assert payload["items"][0]["geo_id"] == "06001"
    assert payload["items"][0]["value_a"] == 100.0
    assert payload["items"][0]["value_b"] == 10.0
