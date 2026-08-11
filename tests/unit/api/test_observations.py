"""API unit tests: observation endpoints.

Migrated from apps/api/tests/test_observations.py.
Covers: API-003 (required metric input), API-004 (metric aliases),
        API-006 (pagination totals), API-007 (date-range validation),
        API-010 (filtering), API-011 (response schema).
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


def _observation_row(metric_code: str = "POP_TOTAL", geo_id: str = "US") -> dict:
    return {
        "source_code": "ACS",
        "source": "ACS",
        "observation_date": "2025-01-01",
        "period": "2025",
        "duration_start": None,
        "duration_end": None,
        "time_sk": 20250101,
        "as_of_date": "2025-02-01",
        "release_date": "2025-02-01",
        "updated_at": datetime(2025, 2, 1, 0, 0, 0),
        "geo_id": geo_id,
        "geo_level": "state",
        "geo_name": "California",
        "state_fips": "06",
        "county_fips": None,
        "state_name": "California",
        "county_name": None,
        "geo_latitude": 36.7783,
        "geo_longitude": -119.4179,
        "metric_code": metric_code,
        "metric_display_name": "Population Total",
        "dashboard_suitability": "PRIMARY",
        "value": "100.0",
        "value_type": "level",
        "units": "people",
        "unit": "people",
        "seasonal_adjustment_status": "NSA",
        "dataset_code": "acs5",
        "dataset": "acs5",
        "vintage_year": 2025,
        "vintage": "2025",
        "margin_of_error": "1.5",
        "margin_of_error_pct": "0.015",
    }


class _LatestForwardingSession:
    def __init__(self):
        self.params_seen = []

    def execute(self, query, params=None):
        sql = str(query).lower()
        self.params_seen.append(params or {})

        if "from gold.v_metric_latest_by_geo" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=5)

        if "from gold.v_metric_latest_by_geo" in sql:
            return _FakeResult(rows=[_observation_row()])

        return _FakeResult(rows=[])


class _LatestFallbackSession:
    def execute(self, query, _params=None):
        sql = str(query).lower()

        if "from gold.v_metric_latest_by_geo" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=0)

        if "from gold.v_metric_latest_by_geo" in sql:
            return _FakeResult(rows=[])

        if "with ranked" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=3)

        if "with ranked" in sql:
            return _FakeResult(
                rows=[_observation_row(metric_code="UNEMP", geo_id="06001")]
            )

        return _FakeResult(rows=[])


class _TimeseriesSession:
    def __init__(self):
        self.params_seen = []

    def execute(self, query, _params=None):
        sql = str(query).lower()
        self.params_seen.append(_params or {})

        if "from gold.v_metric_timeseries_by_geo" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=9)

        if "from gold.v_metric_timeseries_by_geo" in sql:
            return _FakeResult(
                rows=[_observation_row(metric_code="UNEMP", geo_id="06001")]
            )

        return _FakeResult(rows=[])


@pytest.mark.unit
@pytest.mark.api
def test_latest_forwards_filters_and_uses_count_total() -> None:
    """Covers: API-006, API-010 — filters forward and total comes from count."""
    fake = _LatestForwardingSession()

    def _override_db():
        yield fake

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/latest",
            params={
                "metric_code": "POP_TOTAL",
                "geo_level": "state",
                "state_fips": "06",
                "limit": 1,
                "offset": 0,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 5
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["period"] == "2025"
    assert item["source"] == "ACS"
    assert item["dataset"] == "acs5"
    assert item["vintage"] == "2025"
    assert item["release_date"] == "2025-02-01"
    assert item["unit"] == "people"
    assert item["geo_name"] == "California"
    assert item["margin_of_error"] == "1.5"
    assert item["margin_of_error_pct"] == "0.015"
    assert fake.params_seen[0]["geo_level"] == "state"
    assert fake.params_seen[0]["state_fips"] == "06"


@pytest.mark.unit
@pytest.mark.api
def test_latest_falls_back_to_rpt_when_mv_empty() -> None:
    """Covers: API-027 — latest falls back to durable reporting rows."""

    def _override_db():
        yield _LatestFallbackSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/latest",
            params={"metric_code": "UNEMP", "limit": 1, "offset": 0},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert len(payload["items"]) == 1
    assert payload["items"][0]["metric_code"] == "UNEMP"
    assert payload["items"][0]["dataset"] == "acs5"


@pytest.mark.unit
@pytest.mark.api
def test_timeseries_uses_count_total() -> None:
    """Covers: API-006 — timeseries total comes from an independent count."""

    def _override_db():
        yield _TimeseriesSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/timeseries",
            params={"metric_code": "UNEMP", "geo_id": "06001", "limit": 1},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 9
    assert len(payload["items"]) == 1
    assert payload["items"][0]["period"] == "2025"


@pytest.mark.unit
@pytest.mark.api
def test_timeseries_rejects_invalid_date_range() -> None:
    """Covers: API-007 — a reversed timeseries date range returns 422."""

    def _override_db():
        yield _TimeseriesSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/timeseries",
            params={
                "metric_code": "UNEMP",
                "geo_id": "06001",
                "start_date": "2025-02-01",
                "end_date": "2025-01-01",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert (
        response.json()["detail"] == "start_date must be less than or equal to end_date"
    )


@pytest.mark.unit
@pytest.mark.api
def test_latest_accepts_metric_id_alias() -> None:
    """Covers: API-004 — metric_id resolves to metric_code for latest."""
    fake = _LatestForwardingSession()

    def _override_db():
        yield fake

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/latest",
            params={
                "metric_id": "POP_TOTAL",
                "geo_level": "state",
                "state_fips": "06",
                "limit": 1,
                "offset": 0,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake.params_seen[0]["metric_code"] == "POP_TOTAL"


@pytest.mark.unit
@pytest.mark.api
def test_latest_resolves_product_friendly_population_alias() -> None:
    """Covers: API-004 — population resolves to its canonical metric."""
    fake = _LatestForwardingSession()

    def _override_db():
        yield fake

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/latest",
            params={"metric_code": "population", "geo_level": "county", "limit": 1},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake.params_seen[0]["metric_code"] == "ACS:acs5:B01003_001"


@pytest.mark.unit
@pytest.mark.api
def test_timeseries_accepts_metric_id_alias() -> None:
    """Covers: API-004 — timeseries forwards the metric_id alias."""
    fake = _TimeseriesSession()

    def _override_db():
        yield fake

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/timeseries",
            params={"metric_id": "UNEMP", "geo_id": "06001", "limit": 1},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake.params_seen[0]["metric_code"] == "UNEMP"


@pytest.mark.unit
@pytest.mark.api
def test_latest_requires_metric_code_or_metric_id() -> None:
    """Covers: API-003 — latest requires one metric identifier."""

    def _override_db():
        yield _LatestForwardingSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/latest",
            params={"limit": 1, "offset": 0},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code or metric_id is required"


@pytest.mark.unit
@pytest.mark.api
def test_timeseries_requires_metric_code_or_metric_id() -> None:
    """Covers: API-003 — timeseries requires one metric identifier."""

    def _override_db():
        yield _TimeseriesSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/observations/timeseries",
            params={"geo_id": "06001", "limit": 1},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code or metric_id is required"
