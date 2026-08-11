"""API unit tests: source-specific observation endpoints.

Migrated from apps/api/tests/test_source_observations.py.
Covers: API-013 (source-specific endpoints return only their source),
        API-003 (required metric input), API-007 (date-range validation).
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

    def scalars(self):
        return self

    def __iter__(self):
        return iter(self._rows)


def _observation_row(
    metric_code: str = "BLS:LAU:UNEMP_RATE", geo_id: str = "state:06"
) -> dict:
    return {
        "source_code": "BLS",
        "source": "BLS",
        "observation_date": "2024-01-01",
        "period": "2024",
        "duration_start": None,
        "duration_end": None,
        "time_sk": 20240101,
        "as_of_date": "2024-02-01",
        "release_date": "2024-02-01",
        "updated_at": datetime(2024, 2, 1),
        "geo_id": geo_id,
        "geo_level": "STATE",
        "geo_name": "California",
        "state_fips": "06",
        "county_fips": None,
        "state_name": "California",
        "county_name": None,
        "geo_latitude": 36.7783,
        "geo_longitude": -119.4179,
        "metric_code": metric_code,
        "metric_display_name": "Unemployment Rate",
        "dashboard_suitability": "PUBLIC_SAFE",
        "value": "4.2",
        "value_type": "RATE",
        "units": "percent",
        "unit": "percent",
        "seasonal_adjustment_status": "SA",
        "dataset_code": None,
        "dataset": None,
        "vintage_year": None,
        "vintage": None,
        "margin_of_error": None,
        "margin_of_error_pct": None,
    }


class _SourceSchemaSession:
    def __init__(self, source_schema: str, rows: list):
        self._schema = source_schema
        self._rows = rows

    def execute(self, query, params=None):
        sql = str(query).lower()

        if "to_regclass" in sql:
            return _FakeResult(scalar_value=True)

        if "information_schema.columns" in sql:
            return _FakeResult(
                rows=[
                    "dataset_code",
                    "vintage_year",
                    "margin_of_error",
                    "margin_of_error_pct",
                ]
            )

        schema = self._schema.lower()
        source_tables = {
            "gold_bls": ("gold_bls.mv_bls_latest", "gold_bls.rpt_bls_observations"),
            "gold_census": (
                "gold_census.mv_acs_latest",
                "gold_census.rpt_acs_observations",
            ),
            "gold_fred": (
                "gold_fred.mv_fred_latest",
                "gold_fred.rpt_fred_observations",
            ),
        }
        latest_table, timeseries_table = source_tables[schema]
        latest_relations = (f"{schema}.v_metric_latest_by_geo", latest_table)
        timeseries_relations = (
            f"{schema}.v_metric_timeseries_by_geo",
            timeseries_table,
        )

        if any(f"from {r}" in sql for r in latest_relations) and "count(*)" in sql:
            return _FakeResult(scalar_value=len(self._rows))

        if any(f"from {r}" in sql for r in latest_relations):
            return _FakeResult(rows=self._rows)

        if any(f"from {r}" in sql for r in timeseries_relations) and "count(*)" in sql:
            return _FakeResult(scalar_value=len(self._rows))

        if any(f"from {r}" in sql for r in timeseries_relations):
            return _FakeResult(rows=self._rows)

        return _FakeResult(rows=[])


@pytest.mark.unit
@pytest.mark.api
def test_bls_latest_observations_returns_data() -> None:
    """Covers: API-011, API-013 — BLS latest returns BLS contract rows."""

    def _override_db():
        yield _SourceSchemaSession("gold_bls", [_observation_row()])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/bls/observations/latest",
            params={
                "metric_code": "BLS:LAU:UNEMP_RATE",
                "geo_level": "STATE",
                "limit": 10,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["source"] == "BLS"


@pytest.mark.unit
@pytest.mark.api
def test_bls_latest_requires_metric_code() -> None:
    """Covers: API-003 — BLS latest requires a metric identifier."""

    def _override_db():
        yield _SourceSchemaSession("gold_bls", [])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/bls/observations/latest", params={"limit": 5})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code or metric_id is required"


@pytest.mark.unit
@pytest.mark.api
def test_bls_timeseries_rejects_invalid_date_range() -> None:
    """Covers: API-007 — BLS history rejects a reversed date range."""

    def _override_db():
        yield _SourceSchemaSession("gold_bls", [])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/bls/observations/timeseries",
            params={
                "metric_code": "BLS:LAU:UNEMP_RATE",
                "geo_id": "state:06",
                "start_date": "2024-06-01",
                "end_date": "2024-01-01",
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
def test_census_latest_observations_returns_data() -> None:
    """Covers: API-011, API-013 — Census latest returns ACS contract rows."""
    row = _observation_row(metric_code="ACS:acs5:B01003_001", geo_id="state:06")
    row.update(
        source_code="CENSUS_ACS",
        source="CENSUS_ACS",
        dataset_code="acs5",
        dataset="acs5",
        vintage_year=2022,
        vintage="2022",
        margin_of_error="150.0",
        margin_of_error_pct="0.01",
    )

    def _override_db():
        yield _SourceSchemaSession("gold_census", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/census/observations/latest",
            params={"metric_code": "ACS:acs5:B01003_001", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["dataset"] == "acs5"
    assert payload["items"][0]["vintage"] == "2022"
    assert payload["items"][0]["margin_of_error"] == "150.0"


@pytest.mark.unit
@pytest.mark.api
def test_fred_latest_observations_returns_data() -> None:
    """Covers: API-011, API-013 — FRED latest returns FRED contract rows."""
    row = _observation_row(metric_code="FRED:UNRATE", geo_id="us:1")
    row.update(source_code="FRED", source="FRED")

    def _override_db():
        yield _SourceSchemaSession("gold_fred", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/fred/observations/latest",
            params={"metric_code": "FRED:UNRATE", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["source"] == "FRED"


@pytest.mark.unit
@pytest.mark.api
def test_fred_latest_accepts_metric_id_alias() -> None:
    """Covers: API-004 — FRED latest accepts the metric_id alias."""
    row = _observation_row(metric_code="FRED:UNRATE", geo_id="us:1")
    row.update(source_code="FRED", source="FRED")

    def _override_db():
        yield _SourceSchemaSession("gold_fred", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/fred/observations/latest",
            params={"metric_id": "FRED:UNRATE", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200


@pytest.mark.unit
@pytest.mark.api
@pytest.mark.parametrize(
    ("source_path", "source_schema", "metric_code", "source_code"),
    [
        ("bls", "gold_bls", "BLS:LAU:UNEMP_RATE", "BLS"),
        ("census", "gold_census", "ACS:acs5:B01003_001", "CENSUS_ACS"),
        ("fred", "gold_fred", "FRED:UNRATE", "FRED"),
    ],
)
def test_source_timeseries_routes_preserve_source_contract(
    source_path: str,
    source_schema: str,
    metric_code: str,
    source_code: str,
) -> None:
    """Covers: API-013 — source history routes return only their source."""
    row = _observation_row(metric_code=metric_code)
    row.update(source_code=source_code, source=source_code)

    def _override_db():
        yield _SourceSchemaSession(source_schema, [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        response = TestClient(app).get(
            f"/api/{source_path}/observations/timeseries",
            params={"metric_code": metric_code, "geo_id": row["geo_id"]},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert {item["source"] for item in payload["items"]} == {source_code}
