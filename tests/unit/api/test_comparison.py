"""API unit tests: the aligned, policy-guarded comparison endpoint.

Covers: API-003 (required metric input), API-004 (metric aliases),
        API-015 (comparison results),
        API-051 (registry-dispatched alignment: each side reduces to one
        newest value per geography inside its own reviewed relation before
        the join, incompatible pairs are rejected with the failed rules,
        unknown codes answer a stable 404, and inputs travel with every
        derived value).
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


def _metric(code: str, source: str, **overrides) -> dict:
    metric = {
        "metric_code": code,
        "source_code": source,
        "units": "Percent",
        "valid_time_grains": ["MONTHLY"],
        "valid_geo_grains": ["COUNTY"],
        "aggregation_characteristic": None,
        "physical_lineage": {},
    }
    metric.update(overrides)
    return metric


_JOINED_ROW = {
    "geo_id": "county:06001",
    "geo_level": "county",
    "state_fips": "06",
    "county_fips": "001",
    "state_name": "California",
    "county_name": "Alameda",
    "period_a": "2026-07-01",
    "period_b": "2026-06-01",
    "value_a": 100.0,
    "value_b": 10.0,
    "difference": 90.0,
    "ratio": 10.0,
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

    def scalar(self):
        return self._scalar_value


class _ComparisonSession:
    """Resolves glossary lookups per code, records the dispatched SQL."""

    def __init__(self, metric_rows: dict[str, dict], rows=None, total=0):
        self._metric_rows = metric_rows
        self._rows = rows or []
        self._total = total
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        self.parameters.append(dict(params or {}))
        if "gold_glossary.dim_metric" in rendered:
            row = self._metric_rows.get((params or {}).get("metric_code"))
            return _FakeResult(rows=[row] if row else [])
        if "COUNT(*)::INT FROM joined" in rendered:
            return _FakeResult(scalar_value=self._total)
        return _FakeResult(rows=self._rows)


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def _dispatched(session: _ComparisonSession) -> list[str]:
    return [
        statement
        for statement in session.statements
        if "gold_glossary.dim_metric" not in statement
        and "to_regclass" not in statement
    ]


def _relations_in(sql: str) -> set[str]:
    return set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))


def test_comparison_requires_metric_a() -> None:
    """Covers: API-003 — comparison requires its first metric."""
    client = _client_with(_ComparisonSession({}))
    try:
        response = client.get("/api/comparison", params={"metric_id_b": "X"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code_a or metric_id_a is required"


def test_comparison_requires_metric_b() -> None:
    """Covers: API-003 — comparison requires its second metric."""
    client = _client_with(_ComparisonSession({}))
    try:
        response = client.get("/api/comparison", params={"metric_id_a": "X"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json()["detail"] == "metric_code_b or metric_id_b is required"


def test_unknown_comparison_metric_is_a_stable_404() -> None:
    """Covers: API-051 — an unknown code names its parameter, no query runs."""
    session = _ComparisonSession({"FRED:UNRATE": _metric("FRED:UNRATE", "FRED")})
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/comparison",
            params={"metric_code_a": "FRED:UNRATE", "metric_code_b": "NO:SUCH"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "metric_code_b not found"}
    assert not _dispatched(session)


def test_incompatible_pair_is_rejected_before_any_serving_query() -> None:
    """Covers: API-051 — the route enforces exactly the preflight verdict."""
    rows = {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED"),
        "BLS:LNS14000000": _metric("BLS:LNS14000000", "BLS", units="Persons"),
    }
    session = _ComparisonSession(rows)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/comparison",
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "BLS:LNS14000000",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "units differ" in detail
    assert "preflight" in detail
    assert not _dispatched(session), "an incompatible pair must not reach SQL"


def test_stratified_source_comparison_is_declined_with_its_reason() -> None:
    """Covers: API-051 — a CDC metric explains itself instead of collapsing."""
    rows = {
        "CDC:cdi:X:crude": _metric("CDC:cdi:X:crude", "CDC", units="percent"),
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED", units="percent"),
    }
    session = _ComparisonSession(rows)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/comparison",
            params={
                "metric_code_a": "CDC:cdi:X:crude",
                "metric_code_b": "FRED:UNRATE",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert "stratified" in response.json()["detail"]
    assert not _dispatched(session)


def test_comparison_accepts_metric_id_aliases() -> None:
    """Covers: API-004, API-015 — comparison aliases return paired values."""
    rows = {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED"),
        "FRED:CIVPART": _metric("FRED:CIVPART", "FRED"),
    }
    session = _ComparisonSession(rows, rows=[dict(_JOINED_ROW)], total=1)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/comparison",
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_id_b": "FRED:CIVPART",
                "limit": 10,
                "offset": 0,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert len(payload["items"]) == 1


def test_comparable_pair_aligns_one_newest_value_per_geography() -> None:
    """Covers: API-015, API-051 — ranked sides join once per geography.

    Each side reduces to ``recency_rank = 1`` inside its own relation before
    the join, which is what makes a multi-period latest surface safe to
    align: no Cartesian rows, and each row's ``period_a``/``period_b`` state
    exactly which publications were combined.
    """
    rows = {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED"),
        "BLS:LNS14000000": _metric("BLS:LNS14000000", "BLS"),
    }
    session = _ComparisonSession(rows, rows=[dict(_JOINED_ROW)], total=1)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/comparison",
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "BLS:LNS14000000",
                "geo_level": "county",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_code_a"] == "FRED"
    assert payload["source_code_b"] == "BLS"
    assert payload["units_a"] == payload["units_b"] == "Percent"
    assert payload["derivations"] == ["difference", "ratio"]
    (item,) = payload["items"]
    assert item["period_a"] == "2026-07-01"
    assert item["period_b"] == "2026-06-01"
    assert item["difference"] == 90.0

    sql = _dispatched(session)[-1]
    assert "FROM gold_fred.mv_fred_latest" in sql
    assert "FROM gold_bls.mv_bls_latest" in sql
    assert sql.count("recency_rank = 1") == 2, (
        "both sides must reduce to one newest value per geography"
    )
    assert "JOIN side_b USING (geo_id)" in sql
    assert "ORDER BY geo_id" in sql
    assert _relations_in(sql) <= ALLOWED_OBSERVATION_RELATIONS

    bound = session.parameters[-1]
    assert bound["a_metric_code_value"] == "FRED:UNRATE"
    assert bound["b_metric_code_value"] == "BLS:LNS14000000"
    assert bound["geo_level"] == "county"


def test_comparison_filter_unsupported_by_either_side_is_rejected() -> None:
    """Covers: API-051 — a filter one source cannot honor is explained."""
    rows = {
        "CENSUS_PEP:POP": _metric(
            "CENSUS_PEP:POP",
            "CENSUS_PEP",
            units="people",
            valid_time_grains=["ANNUAL"],
            physical_lineage={
                "schema": "gold_pep",
                "relation": "population_estimate_revision",
                "key": "POP",
            },
        ),
        "CENSUS_ACS:acs5:B01003_001E": _metric(
            "CENSUS_ACS:acs5:B01003_001E",
            "CENSUS_ACS",
            units="people",
            valid_time_grains=["ANNUAL"],
            physical_lineage={
                "schema": "gold_census",
                "relation": "fact_acs_observation",
                "key": "acs5:B01003_001E",
            },
        ),
    }
    session = _ComparisonSession(rows, rows=[], total=0)
    client = _client_with(session)
    try:
        rejected = client.get(
            "/api/v1/comparison",
            params={
                "metric_code_a": "CENSUS_ACS:acs5:B01003_001E",
                "metric_code_b": "CENSUS_PEP:POP",
                "state_fips": "06",
            },
        )
        served = client.get(
            "/api/v1/comparison",
            params={
                "metric_code_a": "CENSUS_ACS:acs5:B01003_001E",
                "metric_code_b": "CENSUS_PEP:POP",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert rejected.status_code == 422
    assert "state_fips" in rejected.json()["detail"]
    assert "CENSUS_PEP" in rejected.json()["detail"]

    # Without the unsupported filter the cross-relation pair serves, PEP side
    # bound through its published lineage key.
    assert served.status_code == 200
    sql = _dispatched(session)[-1]
    assert "FROM gold_pep.population_estimate_latest" in sql
    assert "FROM gold_census.mv_acs_latest" in sql
    bound = session.parameters[-1]
    assert bound["a_lineage_key"] == "ACS:acs5:B01003_001E"
    assert bound["b_lineage_key"] == "POP"
