"""API unit tests: the API-derived correlation over a comparable pair.

Covers: API-130 (``GET /comparison/correlation`` answers the comparison's
        own pairs and the comparison's own refusals), API-131 (a coefficient
        the pairs cannot support is null with its reason, and every answer
        carries its caveat set, its coverage, its contemporaneity, and the
        optional same-year pin).

The session double answers the one statement the route executes, which is
what lets these tests assert the *shape* of the reduction — that both sides
rank to one newest value per geography before the join, that the year pin
constrains the same period expression the reduction ranks on, and that the
statistics are measured over the join rather than over a page.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.ratelimit import RateLimitMiddleware
from apps.api.registry import ALLOWED_OBSERVATION_RELATIONS
from apps.api.services.compatibility import (
    CORRELATION_CAUSATION_CAVEAT,
    CORRELATION_DERIVATIONS,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

CORRELATION = "/api/v1/comparison/correlation"


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


def _statistics(
    n: int = 10,
    geographies_a: int = 10,
    geographies_b: int = 10,
    contemporaneous_pairs: Optional[int] = None,
    pearson_r: Optional[float] = 0.5,
    spearman_rho: Optional[float] = 0.4,
    distinct_a: int = 9,
    distinct_b: int = 9,
    period_count_a: int = 1,
    period_count_b: int = 1,
    period_a: Optional[str] = "2023",
    period_b: Optional[str] = "2023",
) -> dict[str, Any]:
    return {
        "n": n,
        "geographies_a": geographies_a,
        "geographies_b": geographies_b,
        "contemporaneous_pairs": n if contemporaneous_pairs is None else contemporaneous_pairs,
        "pearson_r": pearson_r,
        "spearman_rho": spearman_rho,
        "distinct_a": distinct_a,
        "distinct_b": distinct_b,
        "period_count_a": period_count_a,
        "period_count_b": period_count_b,
        "period_a": period_a,
        "period_b": period_b,
    }


class _CorrelationSession:
    """Resolves glossary lookups per code, records the dispatched SQL."""

    def __init__(self, metric_rows: dict[str, dict], statistics: dict | None = None):
        self._metric_rows = metric_rows
        self._statistics = statistics if statistics is not None else _statistics()
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        self.parameters.append(dict(params or {}))
        if "gold_glossary.dim_metric" in rendered:
            row = self._metric_rows.get((params or {}).get("metric_code"))
            return _FakeResult(rows=[row] if row else [])
        return _FakeResult(rows=[dict(self._statistics)])


class _FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def one(self):
        return self._rows[0]

    def first(self):
        return self._rows[0] if self._rows else None


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def _dispatched(session: _CorrelationSession) -> list[str]:
    return [
        statement
        for statement in session.statements
        if "gold_glossary.dim_metric" not in statement
        and "to_regclass" not in statement
    ]


def _relations_in(sql: str) -> set[str]:
    return set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))


def _fred_pair() -> dict[str, dict]:
    return {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED"),
        "FRED:CIVPART": _metric("FRED:CIVPART", "FRED"),
    }


# --------------------------------------------------------------------------
# Refusal parity with /comparison (criterion 1)
# --------------------------------------------------------------------------


def test_unknown_metric_is_the_same_404_the_comparison_gives() -> None:
    """Covers: API-130 — an unknown code names its parameter, no query runs."""
    session = _CorrelationSession({"FRED:UNRATE": _metric("FRED:UNRATE", "FRED")})
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={"metric_code_a": "FRED:UNRATE", "metric_code_b": "NO:SUCH"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "metric_code_b not found"}
    assert not _dispatched(session)


def test_incomparable_pair_is_refused_with_its_failed_rules() -> None:
    """Covers: API-130 — the correlation enforces exactly the preflight verdict."""
    rows = {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED"),
        "BLS:LNS14000000": _metric("BLS:LNS14000000", "BLS", units="Persons"),
    }
    session = _CorrelationSession(rows)
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
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
    assert not _dispatched(session), "an incomparable pair must not reach SQL"


@pytest.mark.parametrize(
    ("code", "source", "expected"),
    [
        ("CDC:cdi:X:crude", "CDC", "stratified"),
        ("USDA_NASS:CORN:YIELD", "USDA_NASS", "USDA NASS"),
        ("FBI_UCR:V:rate", "FBI_UCR", "FBI UCR"),
    ],
)
def test_analysis_refused_source_is_declined_with_the_apis_own_reason(
    code: str, source: str, expected: str
) -> None:
    """Covers: API-130 — the three refusals are the ones /comparison gives."""
    rows = {
        code: _metric(code, source, units="percent"),
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED", units="percent"),
    }
    session = _CorrelationSession(rows)
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={"metric_code_a": code, "metric_code_b": "FRED:UNRATE"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert expected in response.json()["detail"]
    assert not _dispatched(session)


# --------------------------------------------------------------------------
# The pairs are the comparison's pairs (criterion 2)
# --------------------------------------------------------------------------


def test_the_pairs_are_the_reduction_the_comparison_pages() -> None:
    """Covers: API-130 — same per-side reduction, same inner join, unpaged."""
    session = _CorrelationSession(_fred_pair())
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
                "geo_level": "county",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    sql = _dispatched(session)[-1]
    assert sql.count("recency_rank = 1") == 2, (
        "both sides must reduce to one newest value per geography"
    )
    assert "JOIN side_b USING (geo_id)" in sql
    assert _relations_in(sql) <= ALLOWED_OBSERVATION_RELATIONS
    assert "LIMIT" not in sql.upper(), "the statistic is over the join, not a page"

    bound = session.parameters[-1]
    assert bound["a_metric_code_value"] == "FRED:UNRATE"
    assert bound["b_metric_code_value"] == "FRED:CIVPART"
    assert bound["geo_level"] == "COUNTY"


def test_the_reduction_is_the_comparisons_own_not_a_copy_of_it() -> None:
    """Covers: API-130 — both routes emit the identical per-side reduction.

    The plan's reduction-parity criterion, at the tier that can prove it
    without a warehouse: the two routes must not merely rank alike, they must
    emit the same text, because two reductions that rank by the same
    expression and break ties differently answer different published rows for
    one geography (API-083). Asserting the rendered CTEs are equal makes a
    divergence a failing test rather than a discrepancy someone notices in
    two answers.
    """
    from apps.api.services.comparison_service import (
        _analysis_dispatch,
        _side_conditions,
        ranked_latest_cte,
    )

    session = _CorrelationSession(_fred_pair())
    client = _client_with(session)
    try:
        client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
                "geo_level": "county",
            },
        )
    finally:
        app.dependency_overrides.clear()
    correlation_sql = _dispatched(session)[-1]

    comparison_session = _CorrelationSession(_fred_pair())
    comparison_client = _client_with(comparison_session)
    try:
        comparison_client.get(
            "/api/v1/comparison",
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
                "geo_level": "county",
            },
        )
    finally:
        app.dependency_overrides.clear()

    # Rebuilt from the same inputs both routes build from, then found in each
    # route's own statement: the comparison's session double answers its two
    # statements from one row shape, so the CTE text is the shared artefact to
    # compare rather than the whole query.
    metrics = _fred_pair()
    for code, prefix in (("FRED:UNRATE", "a_"), ("FRED:CIVPART", "b_")):
        metric = metrics[code]
        dispatch = _analysis_dispatch(metric)
        conditions, _ = _side_conditions(
            comparison_session,
            dispatch,
            code,
            metric,
            {"geo_level": "county", "state_fips": None},
            prefix,
        )
        cte = ranked_latest_cte(dispatch, conditions)
        assert cte in correlation_sql
        assert any(cte in statement for statement in _dispatched(comparison_session))


def test_a_pair_with_a_null_side_is_excluded_and_never_zero() -> None:
    """Covers: API-130 — ``n`` counts pairs where both sides published a number."""
    session = _CorrelationSession(_fred_pair())
    client = _client_with(session)
    try:
        client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    sql = _dispatched(session)[-1]
    assert "side_a.value IS NOT NULL AND side_b.value IS NOT NULL" in sql, (
        "a pair with a missing side must be excluded, not coerced to zero"
    )


def test_the_answer_carries_both_coefficients_and_its_inputs() -> None:
    """Covers: API-130 — the derived statistic travels with the inputs' identity."""
    session = _CorrelationSession(
        _fred_pair(),
        _statistics(n=3016, geographies_a=3143, geographies_b=3016),
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
                "geo_level": "county",
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["derived"] is True
    assert payload["derivations"] == list(CORRELATION_DERIVATIONS)
    assert payload["metric_code_a"] == "FRED:UNRATE"
    assert payload["source_code_a"] == "FRED"
    assert payload["units_a"] == payload["units_b"] == "Percent"
    assert payload["n"] == 3016
    assert payload["geographies_a"] == 3143
    assert payload["geographies_b"] == 3016
    assert payload["pearson_r"] == 0.5
    assert payload["spearman_rho"] == 0.4
    # The vocabulary word, not the caller's (API-094).
    assert payload["geo_level"] == "COUNTY"


# --------------------------------------------------------------------------
# Null coefficients, with their reason (criterion 3)
# --------------------------------------------------------------------------


def test_too_few_pairs_answers_null_coefficients_with_the_reason() -> None:
    """Covers: API-131 — ``n < 3`` is null, never ``0``, and says why."""
    session = _CorrelationSession(
        _fred_pair(),
        _statistics(n=2, geographies_a=2, geographies_b=2, pearson_r=1.0, spearman_rho=1.0),
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["pearson_r"] is None
    assert payload["spearman_rho"] is None
    assert any("2 paired geographies" in caveat for caveat in payload["caveats"])


@pytest.mark.parametrize("side", ["a", "b"])
def test_a_constant_side_answers_null_coefficients_with_the_reason(side: str) -> None:
    """Covers: API-131 — a side with one distinct value has no correlation."""
    session = _CorrelationSession(
        _fred_pair(),
        _statistics(
            n=50,
            distinct_a=1 if side == "a" else 9,
            distinct_b=1 if side == "b" else 9,
            pearson_r=None,
            spearman_rho=None,
        ),
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["pearson_r"] is None
    assert payload["spearman_rho"] is None
    assert any(
        f"metric_code_{side} publishes one distinct value" in caveat
        for caveat in payload["caveats"]
    )


def test_no_pairs_answers_nulls_rather_than_a_coefficient_of_zero() -> None:
    """Covers: API-131 — an empty intersection is nulls and a coverage caveat."""
    session = _CorrelationSession(
        _fred_pair(),
        _statistics(
            n=0,
            geographies_a=3143,
            geographies_b=50,
            pearson_r=None,
            spearman_rho=None,
            distinct_a=0,
            distinct_b=0,
            period_count_a=0,
            period_count_b=0,
            period_a=None,
            period_b=None,
        ),
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["n"] == 0
    assert payload["pearson_r"] is None
    assert payload["spearman_rho"] is None
    assert payload["periods_differ"] is False
    assert payload["period_a"] is None


# --------------------------------------------------------------------------
# The same-year pin (criterion 4)
# --------------------------------------------------------------------------


def test_the_year_pin_constrains_the_period_the_reduction_ranks_on() -> None:
    """Covers: API-131 — ``year`` reduces each side within that year."""
    session = _CorrelationSession(_fred_pair())
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
                "year": 2023,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["year"] == 2023
    sql = _dispatched(session)[-1]
    assert sql.count("FROM 1 FOR 4) = :year_pin") == 2, (
        "both sides must be pinned to the requested year"
    )
    assert session.parameters[-1]["year_pin"] == "2023"


def test_without_a_year_the_reduction_is_the_newest_overall() -> None:
    """Covers: API-131 — the year pin is opt-in, and absent by default."""
    session = _CorrelationSession(_fred_pair())
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.json()["year"] is None
    assert "year_pin" not in session.parameters[-1]


def test_contemporaneity_is_counted_and_stated() -> None:
    """Covers: API-131 — how many pairs shared a period, and whether any did not."""
    session = _CorrelationSession(
        _fred_pair(),
        _statistics(
            n=100,
            contemporaneous_pairs=60,
            period_count_a=1,
            period_count_b=3,
            period_a="2023",
            period_b=None,
        ),
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["contemporaneous_pairs"] == 60
    assert payload["period_a"] == "2023"
    assert payload["period_b"] is None
    assert payload["periods_differ"] is True
    assert any("40 of 100" in caveat for caveat in payload["caveats"])


# --------------------------------------------------------------------------
# The caveat set (criterion 5)
# --------------------------------------------------------------------------


def test_association_is_never_presented_as_causation() -> None:
    """Covers: API-131 — the causation caveat leads every answer."""
    session = _CorrelationSession(_fred_pair())
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    caveats = response.json()["caveats"]
    assert caveats[0] == CORRELATION_CAUSATION_CAVEAT
    assert "causation" in caveats[0]


def test_an_unverifiable_rule_and_a_published_margin_both_travel() -> None:
    """Covers: API-131 — the preflight's unknowns and the uncertainty caveat."""
    rows = {
        "CENSUS_ACS:acs5:B01003_001E": _metric(
            "CENSUS_ACS:acs5:B01003_001E",
            "CENSUS_ACS",
            units=None,
            valid_time_grains=["ANNUAL"],
            valid_geo_grains=["COUNTY"],
            physical_lineage={
                "schema": "gold_census",
                "relation": "fact_acs_observation",
                "key": "acs5:B01003_001E",
            },
        ),
        "CENSUS_PEP:POP": _metric(
            "CENSUS_PEP:POP",
            "CENSUS_PEP",
            units=None,
            valid_time_grains=["ANNUAL"],
            valid_geo_grains=["COUNTY"],
            physical_lineage={
                "schema": "gold_pep",
                "relation": "population_estimate_revision",
                "key": "POP",
            },
        ),
    }
    session = _CorrelationSession(rows)
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "CENSUS_ACS:acs5:B01003_001E",
                "metric_code_b": "CENSUS_PEP:POP",
            },
        )
    finally:
        app.dependency_overrides.clear()

    caveats = response.json()["caveats"]
    assert caveats[0] == CORRELATION_CAUSATION_CAVEAT
    assert any("publish no units" in caveat for caveat in caveats)
    assert any("margin_of_error" in caveat for caveat in caveats)


def test_coverage_below_either_side_is_stated() -> None:
    """Covers: API-131 — a partial pairing is a caveat, not silence."""
    session = _CorrelationSession(
        _fred_pair(),
        _statistics(n=50, geographies_a=3143, geographies_b=50),
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert any(
        "50 of the 3143" in caveat for caveat in response.json()["caveats"]
    )


def test_full_coverage_states_no_coverage_caveat() -> None:
    """Covers: API-131 — a caveat names a real shortfall, not every answer."""
    session = _CorrelationSession(
        _fred_pair(), _statistics(n=50, geographies_a=50, geographies_b=50)
    )
    client = _client_with(session)
    try:
        response = client.get(
            CORRELATION,
            params={
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:CIVPART",
            },
        )
    finally:
        app.dependency_overrides.clear()

    caveats = response.json()["caveats"]
    assert not any("paired with" in caveat for caveat in caveats)
    assert not any("were contemporaneous" in caveat for caveat in caveats)


# --------------------------------------------------------------------------
# Caching and rate-limit class (criterion 6)
# --------------------------------------------------------------------------


def test_the_route_is_a_public_cache_target_and_costs_analysis_budget() -> None:
    """Covers: API-130 — both hold by construction; this is what asserts it.

    The cache targets are derived from the cacheable routers' own paths
    (API-076) and the limiter classifies everything that is not a catalog
    path as analysis, so neither is a list to edit. A route added to the
    comparison router inherits both, and this test is the evidence.
    """
    from apps.api.main import PUBLIC_CACHE_TARGETS

    assert PUBLIC_CACHE_TARGETS.covers("/api/v1/comparison/correlation")

    limiter = RateLimitMiddleware(
        app=None, catalog_per_minute=10, analysis_per_minute=5
    )
    assert limiter._classify("/api/v1/comparison/correlation") == ("analysis", 5)
