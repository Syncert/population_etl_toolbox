"""Tests for source-specific gold schema observation endpoints.

Covers /api/bls, /api/census, and /api/fred routes which target the
gold_bls, gold_census, and gold_fred schemas respectively.
"""
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


def _observation_row(metric_code: str = "BLS:LAU:UNEMP_RATE", geo_id: str = "state:06") -> dict:
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
    """Fake session that simulates per-source schema tables being present."""

    def __init__(self, source_schema: str, rows: list):
        self._schema = source_schema
        self._rows = rows

    def execute(self, query, params=None):
        sql = str(query).lower()

        # Schema existence check — signal the schema views are present.
        if "to_regclass" in sql:
            return _FakeResult(scalar_value=True)

        # information_schema check for MVP contract columns.
        if "information_schema.columns" in sql:
            return _FakeResult(rows=["dataset_code", "vintage_year", "margin_of_error", "margin_of_error_pct"])

        schema = self._schema.lower()
        source_tables = {
            "gold_bls": ("gold_bls.mv_bls_latest", "gold_bls.rpt_bls_observations"),
            "gold_census": ("gold_census.mv_acs_latest", "gold_census.rpt_acs_observations"),
            "gold_fred": ("gold_fred.mv_fred_latest", "gold_fred.rpt_fred_observations"),
        }
        latest_table, timeseries_table = source_tables[schema]
        latest_relations = (f"{schema}.v_metric_latest_by_geo", latest_table)
        timeseries_relations = (f"{schema}.v_metric_timeseries_by_geo", timeseries_table)

        if any(f"from {relation}" in sql for relation in latest_relations) and "count(*)" in sql:
            return _FakeResult(scalar_value=len(self._rows))

        if any(f"from {relation}" in sql for relation in latest_relations):
            return _FakeResult(rows=self._rows)

        if any(f"from {relation}" in sql for relation in timeseries_relations) and "count(*)" in sql:
            return _FakeResult(scalar_value=len(self._rows))

        if any(f"from {relation}" in sql for relation in timeseries_relations):
            return _FakeResult(rows=self._rows)

        return _FakeResult(rows=[])


# ─────────────────────────────────────────────────────────────────────────────
# BLS
# ─────────────────────────────────────────────────────────────────────────────

def test_bls_latest_observations_returns_data() -> None:
    rows = [_observation_row()]

    def _override_db():
        yield _SourceSchemaSession("gold_bls", rows)

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/bls/observations/latest",
            params={"metric_code": "BLS:LAU:UNEMP_RATE", "geo_level": "STATE", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert len(payload["items"]) == 1
    assert payload["items"][0]["source"] == "BLS"


def test_bls_timeseries_returns_data() -> None:
    rows = [_observation_row()]

    def _override_db():
        yield _SourceSchemaSession("gold_bls", rows)

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/bls/observations/timeseries",
            params={"metric_code": "BLS:LAU:UNEMP_RATE", "geo_id": "state:06", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1


def test_bls_latest_requires_metric_code() -> None:
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


def test_bls_timeseries_rejects_invalid_date_range() -> None:
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
    assert response.json()["detail"] == "start_date must be less than or equal to end_date"


# ─────────────────────────────────────────────────────────────────────────────
# Census
# ─────────────────────────────────────────────────────────────────────────────

def test_census_latest_observations_returns_data() -> None:
    row = _observation_row(metric_code="ACS:acs5:B01003_001", geo_id="state:06")
    row["source_code"] = "CENSUS_ACS"
    row["source"] = "CENSUS_ACS"
    row["dataset_code"] = "acs5"
    row["dataset"] = "acs5"
    row["vintage_year"] = 2022
    row["vintage"] = "2022"
    row["margin_of_error"] = "150.0"
    row["margin_of_error_pct"] = "0.01"

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


def test_census_timeseries_returns_data() -> None:
    row = _observation_row(metric_code="ACS:acs5:B01003_001", geo_id="state:06")

    def _override_db():
        yield _SourceSchemaSession("gold_census", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/census/observations/timeseries",
            params={"metric_code": "ACS:acs5:B01003_001", "geo_id": "state:06", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["total"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# FRED
# ─────────────────────────────────────────────────────────────────────────────

def test_fred_latest_observations_returns_data() -> None:
    row = _observation_row(metric_code="FRED:UNRATE", geo_id="us:1")
    row["source_code"] = "FRED"
    row["source"] = "FRED"

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


def test_fred_timeseries_returns_data() -> None:
    row = _observation_row(metric_code="FRED:UNRATE", geo_id="us:1")
    row["source_code"] = "FRED"
    row["source"] = "FRED"

    def _override_db():
        yield _SourceSchemaSession("gold_fred", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/fred/observations/timeseries",
            params={"metric_code": "FRED:UNRATE", "geo_id": "us:1", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["total"] == 1


def test_fred_latest_accepts_metric_id_alias() -> None:
    row = _observation_row(metric_code="FRED:UNRATE", geo_id="us:1")
    row["source_code"] = "FRED"
    row["source"] = "FRED"

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
