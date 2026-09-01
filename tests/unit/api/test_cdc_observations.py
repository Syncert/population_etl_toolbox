"""API unit tests: CDC illness and chronic-disease source-explorer endpoint."""

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app

pytestmark = [pytest.mark.unit, pytest.mark.api]


class _FakeResult:
    def __init__(
        self, rows: Optional[list[dict]] = None, scalar_value: Any = None
    ) -> None:
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[dict]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar_value


class _RecordingSession:
    """Record every executed statement so filters and relations are provable."""

    def __init__(self, rows: list[dict], total: int) -> None:
        self._rows = rows
        self._total = total
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        sql = str(query)
        self.calls.append((sql, dict(params or {})))
        if sql.lstrip().upper().startswith("SELECT COUNT(*)"):
            return _FakeResult(scalar_value=self._total)
        return _FakeResult(rows=self._rows)

    @property
    def list_sql(self) -> str:
        return next(sql for sql, _ in self.calls if "ORDER BY" in sql)

    @property
    def list_params(self) -> dict:
        return next(params for sql, params in self.calls if "ORDER BY" in sql)

    @property
    def count_params(self) -> dict:
        return next(
            params
            for sql, params in self.calls
            if sql.lstrip().upper().startswith("SELECT COUNT(*)")
        )


class _FailingSession:
    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        raise OperationalError(
            "SELECT 1",
            {"password": "cdc-warehouse-secret"},
            Exception("connection refused for user population"),
        )


def _cdi_row(**overrides: Any) -> dict:
    """A provider-published CDI state indicator with complete uncertainty."""
    row = {
        "dataset": "cdi",
        "dataset_title": "U.S. Chronic Disease Indicators",
        "release_watermark": "1756142400",
        "measure_id": "DIA01",
        "measure_label": "Diabetes among adults",
        "topic": "Diabetes",
        "value_type_id": "AGEADJPREV",
        "value_type_label": "Age-adjusted prevalence",
        "period_start": 2022,
        "period_end": 2022,
        "geo_id": "state:01",
        "geo_type": "state",
        "geography_status": "resolved",
        "value_source": "12.4",
        "value": "12.4",
        "value_status": "valid",
        "unit": "%",
        "adjustment_status": "age_adjusted",
        "confidence_lower": "11.6",
        "confidence_upper": "13.2",
        "footnote_code": None,
        "footnote_text": None,
        "stratum_id": "a" * 64,
        "strata": [["OVERALL", "Overall", "OVR", "Overall"]],
        "estimate_method": "provider_published",
        "population_basis": "indicator-specific surveillance population",
        "total_population": None,
        "population_18_plus": None,
        "methodology_url": "https://www.cdc.gov/cdi/about/index.html",
        "geography_basis": (
            "CDC location codes; US=59 and two-digit state/territory codes"
        ),
        "source_record_id": "b" * 64,
    }
    row.update(overrides)
    return row


def _places_row(**overrides: Any) -> dict:
    """A suppressed modeled PLACES county estimate."""
    row = {
        "dataset": "places_county",
        "dataset_title": (
            "PLACES: Local Data for Better Health, County Data, 2025 release"
        ),
        "release_watermark": "1756228800",
        "measure_id": "DIABETES",
        "measure_label": "Diagnosed diabetes among adults",
        "topic": "Health Outcomes",
        "value_type_id": "CrdPrv",
        "value_type_label": "Crude prevalence",
        "period_start": 2022,
        "period_end": 2022,
        "geo_id": "state:48|county:301",
        "geo_type": "county",
        "geography_status": "resolved",
        "value_source": None,
        "value": None,
        "value_status": "suppressed",
        "unit": "%",
        "adjustment_status": "crude",
        "confidence_lower": None,
        "confidence_upper": None,
        "footnote_code": "*",
        "footnote_text": "No data available",
        "stratum_id": "c" * 64,
        "strata": [["OVERALL", "Overall", "OVR", "Overall"]],
        "estimate_method": "model_based_small_area_estimate",
        "population_basis": "adults age 18 years and older",
        "total_population": "64",
        "population_18_plus": "57",
        "methodology_url": "https://www.cdc.gov/places/methodology/index.html",
        "geography_basis": "2020 Census counties and county equivalents",
        "source_record_id": "d" * 64,
    }
    row.update(overrides)
    return row


def _client(session: Any) -> TestClient:
    app.dependency_overrides[get_db_session_dep] = lambda: session
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_cdc_observations_expose_dataset_method_and_uncertainty_context() -> None:
    """Covers: API-028 — CDI and PLACES rows keep their interpretive context."""
    session = _RecordingSession([_cdi_row(), _places_row()], total=2)

    response = _client(session).get("/api/v1/cdc/observations")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    cdi, places = payload["items"]
    assert (cdi["dataset"], places["dataset"]) == ("cdi", "places_county")
    assert cdi["estimate_method"] == "provider_published"
    assert places["estimate_method"] == "model_based_small_area_estimate"
    assert (cdi["confidence_lower"], cdi["confidence_upper"]) == ("11.6", "13.2")
    assert cdi["unit"] == "%" and cdi["adjustment_status"] == "age_adjusted"
    assert cdi["population_basis"] == "indicator-specific surveillance population"
    assert places["population_basis"] == "adults age 18 years and older"
    assert places["methodology_url"].startswith("https://www.cdc.gov/places/")
    assert places["geography_basis"] == "2020 Census counties and county equivalents"
    assert cdi["strata"] == [["OVERALL", "Overall", "OVR", "Overall"]]


def test_cdc_suppressed_values_stay_null_and_typed() -> None:
    """Covers: API-028 — suppression is never rendered as a numeric value."""
    session = _RecordingSession([_places_row()], total=1)

    payload = _client(session).get("/api/v1/cdc/observations").json()

    suppressed = payload["items"][0]
    assert suppressed["value"] is None
    assert suppressed["value_source"] is None
    assert suppressed["value_status"] == "suppressed"
    assert suppressed["footnote_text"] == "No data available"


def test_cdc_missing_values_remain_distinct_from_suppressed_values() -> None:
    """Covers: API-028 — missing and suppressed remain separate states."""
    session = _RecordingSession(
        [
            _places_row(
                value=None,
                value_source="",
                value_status="missing",
                footnote_code=None,
                footnote_text=None,
            )
        ],
        total=1,
    )

    payload = _client(session).get("/api/v1/cdc/observations").json()

    assert payload["items"][0]["value_status"] == "missing"
    assert payload["items"][0]["value"] is None


def test_cdc_filters_reach_the_query_as_bound_parameters() -> None:
    """Covers: API-028 — every documented filter reaches the query exactly."""
    session = _RecordingSession([_places_row()], total=1)

    response = _client(session).get(
        "/api/v1/cdc/observations",
        params={
            "dataset": "places_county",
            "measure_id": "DIABETES",
            "value_type_id": "CrdPrv",
            "geo_id": "state:48|county:301",
            "geo_type": "county",
            "year_from": 2021,
            "year_to": 2023,
            "stratum_id": "c" * 64,
            "adjustment": "crude",
            "release": "1756228800",
            "limit": 25,
            "offset": 50,
        },
    )

    assert response.status_code == 200
    assert session.list_params == {
        "dataset": "places_county",
        "measure_id": "DIABETES",
        "value_type_id": "CrdPrv",
        "geo_id": "state:48|county:301",
        "geo_type": "county",
        "year_from": 2021,
        "year_to": 2023,
        "stratum_id": "c" * 64,
        "adjustment_status": "crude",
        "release": "1756228800",
        "limit": 25,
        "offset": 50,
    }
    assert "limit" not in session.count_params
    assert "offset" not in session.count_params


def test_cdc_dataset_filter_cannot_mix_products() -> None:
    """Covers: API-028 — a dataset filter binds one registered product only."""
    session = _RecordingSession([_cdi_row()], total=1)

    payload = (
        _client(session)
        .get("/api/v1/cdc/observations", params={"dataset": "cdi"})
        .json()
    )

    assert session.list_params["dataset"] == "cdi"
    assert {item["dataset"] for item in payload["items"]} == {"cdi"}


def test_cdc_default_request_uses_the_latest_release_projection() -> None:
    """Covers: API-029 — an omitted release reads the latest projection."""
    session = _RecordingSession([_cdi_row()], total=1)

    payload = _client(session).get("/api/v1/cdc/observations").json()

    assert "gold_cdc.latest_release_observation" in session.list_sql
    assert "gold_cdc.health_observation" not in session.list_sql
    assert payload["release_selection"] == "latest_release"
    assert payload["release"] is None


def test_cdc_named_release_reads_published_release_history() -> None:
    """Covers: API-029 — a named release reads durable published history."""
    session = _RecordingSession([_cdi_row(release_watermark="1740787200")], total=1)

    payload = (
        _client(session)
        .get("/api/v1/cdc/observations", params={"release": "1740787200"})
        .json()
    )

    assert "gold_cdc.health_observation" in session.list_sql
    assert "gold_cdc.latest_release_observation" not in session.list_sql
    assert session.list_params["release"] == "1740787200"
    assert payload["release_selection"] == "single_release"
    assert payload["items"][0]["release_watermark"] == "1740787200"


def test_cdc_pagination_total_is_independent_of_page_length() -> None:
    """Covers: API-006 — the CDC total comes from the count query."""
    session = _RecordingSession([_cdi_row()], total=417)

    payload = (
        _client(session)
        .get("/api/v1/cdc/observations", params={"limit": 1, "offset": 0})
        .json()
    )

    assert payload["total"] == 417
    assert len(payload["items"]) == 1
    assert (payload["limit"], payload["offset"]) == (1, 0)


def test_cdc_empty_result_returns_the_stable_empty_contract() -> None:
    """Covers: API-008 — an empty CDC page is 200 with an explicit total."""
    session = _RecordingSession([], total=0)

    response = _client(session).get(
        "/api/v1/cdc/observations", params={"measure_id": "NOT_PUBLISHED"}
    )

    assert response.status_code == 200
    assert response.json()["items"] == []
    assert response.json()["total"] == 0


@pytest.mark.parametrize(
    ("params", "expected_detail"),
    [
        (
            {"dataset": "places_zcta"},
            "dataset must be one of: cdi, places_county",
        ),
        (
            {"geo_type": "tract"},
            "geo_type must be one of: nation, state, county",
        ),
        (
            {"adjustment": "seasonally_adjusted"},
            "adjustment must be one of: crude, age_adjusted, source_specific",
        ),
        (
            {"year_from": 2024, "year_to": 2020},
            "year_from must be less than or equal to year_to",
        ),
    ],
)
def test_cdc_invalid_filters_fail_before_any_database_work(
    params: dict, expected_detail: str
) -> None:
    """Covers: API-030 — unknown CDC filters are rejected before the query."""
    session = _RecordingSession([_cdi_row()], total=1)

    response = _client(session).get("/api/v1/cdc/observations", params=params)

    assert response.status_code == 422
    assert response.json()["detail"] == expected_detail
    assert session.calls == []


@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 5001}, {"offset": -1}, {"year_from": 1899}],
)
def test_cdc_pagination_and_period_bounds_are_enforced(params: dict) -> None:
    """Covers: API-005 — out-of-range CDC paging and periods return 422."""
    session = _RecordingSession([_cdi_row()], total=1)

    response = _client(session).get("/api/v1/cdc/observations", params=params)

    assert response.status_code == 422
    assert session.calls == []


def test_cdc_injection_input_remains_a_bound_parameter() -> None:
    """Covers: API-017 — CDC filter text cannot alter query structure."""
    session = _RecordingSession([], total=0)
    injection = "cdi'; DROP TABLE silver_cdc.fact_health_observation; --"

    response = _client(session).get(
        "/api/v1/cdc/observations", params={"measure_id": injection}
    )

    assert response.status_code == 200
    assert session.list_params["measure_id"] == injection
    assert "DROP TABLE" not in session.list_sql


def test_cdc_database_failure_returns_a_sanitized_503() -> None:
    """Covers: API-016 — CDC database failure never leaks connection detail."""
    response = _client(_FailingSession()).get("/api/v1/cdc/observations")

    assert response.status_code == 503
    assert response.json() == {"detail": "Database service is temporarily unavailable."}
    assert "cdc-warehouse-secret" not in response.text
