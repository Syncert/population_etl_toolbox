"""API unit tests: the registry-dispatched distribution endpoint.

Covers: API-003 (required metric input), API-014 (distribution bins),
        API-052 (distribution dispatches to the metric's owning source's
        latest relation with one newest value per geography, declines
        stratified sources with their declared restriction, answers a
        stable 404 for unknown codes, and labels its bins as API-derived).
"""

from __future__ import annotations

import re
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import ALLOWED_OBSERVATION_RELATIONS

pytestmark = [pytest.mark.unit, pytest.mark.api]

_FRED_METRIC = {
    "metric_code": "FRED:UNRATE",
    "source_code": "FRED",
    "units": "Percent",
    "valid_time_grains": ["MONTHLY"],
    "valid_geo_grains": ["NATIONAL"],
    "aggregation_characteristic": None,
    "physical_lineage": {},
}

_NASS_METRIC = {
    "metric_code": "USDA_NASS:corn_survey_annual:41",
    "source_code": "USDA_NASS",
    "units": "BU",
    "valid_time_grains": ["ANNUAL"],
    "valid_geo_grains": ["COUNTY"],
    "aggregation_characteristic": "not_established",
    "physical_lineage": {
        "schema": "gold_nass",
        "relation": "crop_observation",
        "product_id": "corn_survey_annual",
        "statistic_sk": 41,
        "statisticcat_desc": "PRODUCTION",
        "unit_desc": "BU",
    },
}


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def one(self):
        return self._rows[0]

    def scalar(self):
        return self._scalar_value


class _DistributionSession:
    """Resolves the glossary lookup, then records dispatched queries."""

    def __init__(self, metric_row=None, stats=None, bins=None):
        self._metric_row = metric_row
        self._stats = stats or {"total": 3, "min_value": 10.0, "max_value": 40.0}
        self._bins = bins
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        self.parameters.append(dict(params or {}))
        if "gold_glossary.dim_metric" in rendered:
            return _FakeResult(rows=[self._metric_row] if self._metric_row else [])
        if "width_bucket" in rendered:
            bin_count = int((params or {})["bin_count"])
            if self._bins is not None:
                return _FakeResult(rows=self._bins)
            if bin_count == 1:
                return _FakeResult(rows=[{"bin_index": 1, "count": 3}])
            return _FakeResult(
                rows=[
                    {"bin_index": 1, "count": 1},
                    {"bin_index": bin_count, "count": 2},
                ]
            )
        if "MIN(value)" in rendered:
            return _FakeResult(rows=[dict(self._stats)])
        return _FakeResult(rows=[])


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def _dispatched(session: _DistributionSession) -> list[str]:
    return [
        statement
        for statement in session.statements
        if "gold_glossary.dim_metric" not in statement
        and "to_regclass" not in statement
    ]


def _relations_in(sql: str) -> set[str]:
    return set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))


def test_distribution_requires_metric_code() -> None:
    """Covers: API-003 — metric_code is required; the metric_id alias is gone."""
    client = _client_with(_DistributionSession())
    try:
        response = client.get("/api/v1/distribution/bins", params={"bin_count": 7})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    body = response.json()["detail"]
    assert any(
        error["loc"][-1] == "metric_code" and error["type"] == "missing"
        for error in body
    ), body


@pytest.mark.parametrize("bin_count", [1, 20])
def test_distribution_bin_boundaries_and_counts(bin_count: int) -> None:
    """Covers: API-014 — supported bin boundaries reconcile counts."""
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": bin_count},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["bin_count"] == bin_count
    assert sum(item["count"] for item in payload["items"]) == payload["total"] == 3
    assert payload["items"][0]["lower_bound"] == 10.0
    assert payload["items"][-1]["upper_bound"] == 40.0


@pytest.mark.parametrize("bin_count", [0, 21])
def test_distribution_invalid_bin_counts_are_rejected(bin_count: int) -> None:
    """Covers: API-014 — invalid bin counts fail before database work."""
    client = _client_with(_DistributionSession(metric_row=dict(_FRED_METRIC)))
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": bin_count},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422


def test_unknown_distribution_metric_is_a_stable_404() -> None:
    """Covers: API-052 — an unknown code is explained, no query runs."""
    session = _DistributionSession(metric_row=None)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins", params={"metric_code": "NO:SUCH"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "metric_code not found"}
    assert not _dispatched(session)


def test_stratified_source_distribution_is_declined_with_its_reason() -> None:
    """Covers: API-052 — NASS bins would collapse domains; the API says so."""
    session = _DistributionSession(metric_row=dict(_NASS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": _NASS_METRIC["metric_code"]},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert "multi-dimensional" in response.json()["detail"]
    assert not _dispatched(session), "a declined source must not reach SQL"


def test_distribution_dispatches_to_the_owning_sources_latest_relation() -> None:
    """Covers: API-052 — bins compute over one newest value per geography."""
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={
                "metric_code": "FRED:UNRATE",
                "geo_level": "NATIONAL",
                "bin_count": 7,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_code"] == "FRED"
    assert payload["units"] == "Percent"
    assert payload["derived"] is True

    queries = _dispatched(session)
    assert queries
    for sql in queries:
        assert "FROM gold_fred.mv_fred_latest" in sql
        assert "recency_rank = 1" in sql
        assert "value IS NOT NULL" in sql
        assert _relations_in(sql) <= ALLOWED_OBSERVATION_RELATIONS
    bound = session.parameters[-1]
    assert bound["metric_code_value"] == "FRED:UNRATE"
    assert bound["geo_level"] == "NATIONAL"


def test_distribution_filter_unsupported_by_the_source_is_rejected() -> None:
    """Covers: API-052 — PEP declares no state_fips filter; the API says so."""
    pep_metric = {
        "metric_code": "CENSUS_PEP:POP",
        "source_code": "CENSUS_PEP",
        "units": "people",
        "valid_time_grains": ["ANNUAL"],
        "valid_geo_grains": ["STATE"],
        "aggregation_characteristic": None,
        "physical_lineage": {
            "schema": "gold_pep",
            "relation": "population_estimate_revision",
            "key": "POP",
        },
    }
    session = _DistributionSession(metric_row=pep_metric)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "CENSUS_PEP:POP", "state_fips": "06"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "state_fips" in detail
    assert "CENSUS_PEP" in detail
    assert not _dispatched(session)
