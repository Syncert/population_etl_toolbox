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
        "observation_date": "2025-01-01",
        "duration_start": None,
        "duration_end": None,
        "time_sk": 20250101,
        "as_of_date": "2025-02-01",
        "updated_at": datetime(2025, 2, 1, 0, 0, 0),
        "geo_id": geo_id,
        "geo_level": "state",
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
        "seasonal_adjustment_status": "NSA",
    }


class _LatestForwardingSession:
    def __init__(self):
        self.params_seen = []

    def execute(self, query, params=None):
        sql = str(query).lower()
        self.params_seen.append(params or {})

        if "from gold.mv_latest_dashboard" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=5)

        if "from gold.mv_latest_dashboard" in sql:
            return _FakeResult(rows=[_observation_row()])

        return _FakeResult(rows=[])


class _LatestFallbackSession:
    def execute(self, query, _params=None):
        sql = str(query).lower()

        if "from gold.mv_latest_dashboard" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=0)

        if "from gold.mv_latest_dashboard" in sql:
            return _FakeResult(rows=[])

        if "with ranked" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=3)

        if "with ranked" in sql:
            return _FakeResult(rows=[_observation_row(metric_code="UNEMP", geo_id="06001")])

        return _FakeResult(rows=[])


class _TimeseriesSession:
    def execute(self, query, _params=None):
        sql = str(query).lower()

        if "from gold.rpt_observation_dashboard" in sql and "count(*)" in sql:
            return _FakeResult(scalar_value=9)

        if "from gold.rpt_observation_dashboard" in sql:
            return _FakeResult(rows=[_observation_row(metric_code="UNEMP", geo_id="06001")])

        return _FakeResult(rows=[])


def test_latest_forwards_filters_and_uses_count_total() -> None:
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
    assert fake.params_seen[0]["geo_level"] == "state"
    assert fake.params_seen[0]["state_fips"] == "06"


def test_latest_falls_back_to_rpt_when_mv_empty() -> None:
    def _override_db():
        yield _LatestFallbackSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/observations/latest", params={"metric_code": "UNEMP", "limit": 1, "offset": 0})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert len(payload["items"]) == 1
    assert payload["items"][0]["metric_code"] == "UNEMP"


def test_timeseries_uses_count_total() -> None:
    def _override_db():
        yield _TimeseriesSession()

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/observations/timeseries", params={"metric_code": "UNEMP", "geo_id": "06001", "limit": 1})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 9
    assert len(payload["items"]) == 1


def test_timeseries_rejects_invalid_date_range() -> None:
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
    payload = response.json()
    assert payload["detail"] == "start_date must be less than or equal to end_date"
