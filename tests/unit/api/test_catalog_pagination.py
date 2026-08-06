"""API unit tests: catalog pagination.

Migrated from apps/api/tests/test_catalog_pagination.py.
Covers: API-006 (pagination totals from count query).
"""

import pytest
from datetime import datetime
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


class _CatalogPaginationSession:
    def execute(self, query, _params=None):
        sql = str(query).lower()
        if "count(*)" in sql:
            return _FakeResult(scalar_value=7)

        return _FakeResult(
            rows=[
                {
                    "metric_code": "POP_TOTAL",
                    "metric_display_name": "Population Total",
                    "source_code": "ACS",
                    "source_object_type": "TABLE",
                    "business_definition": "Total resident population",
                    "caveats": None,
                    "valid_geo_grains": ["state", "county"],
                    "valid_time_grains": ["year"],
                    "dashboard_suitability": "PRIMARY",
                    "comparability_group": None,
                    "do_not_compare_with": [],
                    "recommended_aggregation": "SUM",
                    "owner_team": "analytics",
                    "is_active": True,
                    "updated_at": datetime(2026, 1, 1, 0, 0, 0),
                }
            ]
        )


def _override_db():
    yield _CatalogPaginationSession()


@pytest.mark.unit
@pytest.mark.api
def test_catalog_metrics_total_comes_from_count_query() -> None:
    """API-006: total is independent of page size."""
    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/catalog/metrics?limit=1&offset=0")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 7
    assert len(payload["items"]) == 1
