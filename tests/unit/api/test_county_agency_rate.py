"""API unit tests: a county crime rate computed from agency reports (ADR-0012).

Covers: API-181 — an ``agency_rate`` pairing is served from the gold view
that computed it, with its coverage on every row and its refusals in words,
read only for counties, never compared across places, and never offered as
a per-capita view of a count.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import cross_level_service
from apps.api.services.derived_ratio_service import (
    agency_rate_row,
    list_derived_observations,
)
from apps.api.services.neutral_observations_service import NeutralQueryError
from data_ingestion_toolbox.semantics.derived_ratios import (
    SERVE_DRAFTS_ENV,
    RatioRegistryError,
    load_registry,
    pairing_for,
    parse_registry,
)

pytestmark = pytest.mark.unit

RATIO = "violent_crime_county_agency_rate"
DANE = "state:55|county:025"


def _row(**overrides: Any) -> dict[str, Any]:
    row = {
        "product_id": "return_a_violent_crime",
        "measure_id": "V:offense:absolute_total",
        "release_key": "2026-08-15",
        "geo_id": DANE,
        "county_name": "Dane County",
        "period_start": "2023-01-01",
        "period_end": "2023-01-31",
        "rate": "11.2410071942446043",
        "unavailable_reason": None,
        "offenses": "13",
        "covered_population": "115648",
        "linked_population": "115648",
        "coverage_share": "1.00000000000000000000",
        "coverage_floor": "0.5",
        "contributing_oris": ["WI0130000", "WI0137000"],
        "excluded_multi_county_oris": ["WI0540300"],
        "non_reporting_oris": [],
        "unknown_population_oris": [],
        "not_loaded_oris": [],
        "group_total": 1,
    }
    row.update(overrides)
    return row


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return len(self._rows)


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.statements: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query, params=None):
        self.statements.append((str(query), dict(params or {})))
        return _Result(self.rows)


@pytest.fixture
def drafts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")


def test_a_rate_row_carries_its_coverage_and_inputs() -> None:
    """Covers: API-181 — the rate, its offenses and whom it covers (rule 3)."""
    row = agency_rate_row(pairing_for(RATIO), _row())

    assert row.derived is True
    assert row.value == "11.24"
    assert row.unit == "per 100,000 people covered by reporting agencies in this county"
    assert row.geo_level == "COUNTY" and row.geo_name == "Dane County"
    assert row.unavailable_reason is None
    assert row.numerator.metric_code == (
        "FBI_UCR:return_a_violent_crime:V:offense:absolute_total"
    )
    assert row.numerator.value == "13"
    assert row.denominator is None and row.provider_rate is None
    coverage = row.coverage
    assert (coverage.covered_population, coverage.linked_population) == (
        "115648",
        "115648",
    )
    assert coverage.coverage_share == "1.0000"
    assert coverage.coverage_floor == "0.5"
    assert coverage.contributing_agencies == ["WI0130000", "WI0137000"]
    assert coverage.excluded_multi_county_agencies == ["WI0540300"]


@pytest.mark.parametrize(
    "reason",
    ["no_reporting_agency", "only_multi_county_agencies", "coverage_below_floor"],
)
def test_a_refused_month_has_no_value_but_keeps_its_coverage(reason: str) -> None:
    """Covers: API-181 — a refusal is a reason, never a zero, and shows why."""
    row = agency_rate_row(
        pairing_for(RATIO),
        _row(
            rate=None,
            unavailable_reason=reason,
            offenses="7",
            covered_population="30000",
            coverage_share="0.25940786524626452684",
            non_reporting_oris=["WI0130000"],
        ),
    )

    assert row.value is None
    assert row.unavailable_reason == reason
    assert row.coverage.covered_population == "30000"
    assert row.coverage.coverage_share == "0.2594"
    assert row.coverage.non_reporting_agencies == ["WI0130000"]


def test_an_unloaded_agency_refuses_the_month_and_is_named() -> None:
    """Covers: API-181 — an agency linked to the county alone whose reports are
    not loaded refuses the month; coverage is never judged without it."""
    row = agency_rate_row(
        pairing_for(RATIO),
        _row(
            rate=None,
            unavailable_reason="agencies_not_loaded",
            coverage_share=None,
            not_loaded_oris=["WI0130100"],
        ),
    )

    assert row.value is None
    assert row.unavailable_reason == "agencies_not_loaded"
    assert row.coverage.coverage_share is None
    assert row.coverage.not_loaded_agencies == ["WI0130100"]
    assert _row().get("not_loaded_oris") == []


def test_the_read_is_the_gold_view_bound_and_reduced(drafts) -> None:
    """Covers: API-181 — the API divides nothing; every filter is bound."""
    session = _Session([_row()])
    page = list_derived_observations(
        session,
        ratio_id=RATIO,
        scope="latest",
        release=None,
        filters={"geo_id": DANE, "state_fips": "55", "year_from": 2023},
        limit=1,
        offset=0,
        newest_per_geography=True,
    )

    sql, params = session.statements[0]
    assert "FROM gold_fbi.latest_county_agency_rate" in sql
    assert "DISTINCT ON (geo_id)" in sql
    assert "ORDER BY geo_id, period_start DESC, release_key DESC" in sql
    assert "geo_id = :geo_id" in sql and "state_fips = :state_fips" in sql
    assert "UPPER(" not in sql and DANE not in sql
    assert params["product_id"] == "return_a_violent_crime"
    assert params["measure_id"] == "V:offense:absolute_total"
    assert page.total == 1 and page.ratio.kind == "agency_rate"
    assert page.ratio.reads_beside == "FBI_UCR:summarized_violent_crime:V:offense:rate"
    assert page.items[0].value == "11.24"

    session = _Session([])
    list_derived_observations(
        session,
        ratio_id=RATIO,
        scope="as_released",
        release="2026-08-15",
        filters={},
        limit=10,
        offset=0,
        period_start="2023",
    )
    sql, params = session.statements[0]
    assert "FROM gold_fbi.county_agency_rate" in sql
    assert params["release"] == "2026-08-15" and params["period_year"] == 2023


def test_a_state_or_nation_is_refused(drafts) -> None:
    """Covers: API-181 — the FBI publishes its own rate above the county."""
    session = _Session([_row()])
    with pytest.raises(NeutralQueryError, match="counties only"):
        list_derived_observations(
            session,
            ratio_id=RATIO,
            scope="latest",
            release=None,
            filters={"geo_level": "STATE"},
            limit=1,
            offset=0,
        )
    assert session.statements == []


def test_the_route_serves_it_only_when_served(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-181 — a draft agency rate is refused like any draft (rule 4)."""
    session = _Session([_row()])

    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        client = TestClient(app, raise_server_exceptions=False)
        if pairing_for(RATIO).status != "approved":
            monkeypatch.delenv(SERVE_DRAFTS_ENV, raising=False)
            refused = client.get(
                "/api/v1/observations/derived", params={"ratio_id": RATIO}
            )
            assert refused.status_code == 422
        monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
        response = client.get(
            "/api/v1/observations/derived",
            params={"ratio_id": RATIO, "geo_id": DANE, "newest_per_geography": True},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["value"] == "11.24"
    assert item["coverage"]["covered_population"] == "115648"


def test_it_is_never_compared_or_offered_as_a_count_view(drafts) -> None:
    """Covers: API-181 — no reviewed rule reads it against another place."""
    offered = cross_level_service.count_alternatives(
        "FBI_UCR:return_a_violent_crime:V:offense:absolute_total"
    )
    assert RATIO not in {alternative.ratio_id for alternative in offered}
    with pytest.raises(NeutralQueryError, match="not compared across places"):
        cross_level_service.compare_places(
            object(), "state:55|county:025", "state:55|county:009", ratio_id=RATIO
        )


def test_the_registry_holds_an_agency_rate_to_its_rules() -> None:
    """Covers: API-181 — counties only, per 100,000, FBI on both sides."""
    for ratio_id in (RATIO, "property_crime_county_agency_rate"):
        pairing = load_registry()[ratio_id]
        assert pairing.kind == "agency_rate" and pairing.derived
        assert pairing.geo_levels == ("COUNTY",) and pairing.scale == 100000
        assert pairing.reads_beside.endswith(":offense:rate")

    entry = {
        "ratio_id": "x",
        "kind": "agency_rate",
        "scale": 100000,
        "unit": "u",
        "label": "l",
        "numerator": "FBI_UCR:summarized_violent_crime:V:offense:absolute_total",
        "reads_beside": "FBI_UCR:summarized_violent_crime:V:offense:rate",
        "geo_levels": ["COUNTY"],
        "denominator_label": "d",
        "universe": "u",
        "status": "draft",
        "reviewer": "Nick",
        "version": 1,
        "citations": ["https://example.org"],
    }
    assert parse_registry({"entries": [entry]})["x"].reads_beside
    for broken in (
        {"denominator": "CENSUS_PEP:POPESTIMATE"},
        {"geo_levels": ["STATE"]},
        {"scale": 1000},
        {"reads_beside": None},
    ):
        with pytest.raises(RatioRegistryError, match="ADR-0012"):
            parse_registry({"entries": [{**entry, **broken}]})
