"""API unit tests: per-capita and share values, a declared derived product.

Covers: API-173 (a derived value divides the numerator row by the
        denominator row of the same geography, period and release, with the
        Bureau's margin of error and a reason, never zero, when it cannot be
        computed), API-174 (the pairing catalog and the serving gate: only an
        approved pairing is served, a provider's own rate is served as the
        provider's, and the route refuses what it cannot answer), and the
        minutes-of-work time cost (cost-of-living-section CL-3): a regional
        price over the reader's own area's hourly wage, matched by calendar
        year, both areas named.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.schemas.observations import (
    NeutralObservation,
    NeutralObservationListResponse,
    ObservationUncertainty,
)
from apps.api.services import derived_ratio_service
from apps.api.services.derived_ratio_service import (
    derive_row,
    derived_margin,
    pair_rows,
    time_cost_rows,
)
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    observations_for_geographies,
)
from data_ingestion_toolbox.semantics import derived_ratios
from data_ingestion_toolbox.semantics.derived_ratios import (
    SERVE_DRAFTS_ENV,
    load_registry,
    pairing_for,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

POVERTY = "CENSUS_ACS:acs5:B17001_002"
POVERTY_UNIVERSE = "CENSUS_ACS:acs5:B17001_001"


def _acs(
    metric_code: str,
    value: str | None,
    moe: str | None,
    geo_id: str = "0500000US48453",
    geo_level: str = "COUNTY",
    period: str = "2019-01-01",
    release: str = "2023",
    status: str | None = "valid",
) -> NeutralObservation:
    return NeutralObservation(
        source_code="CENSUS_ACS",
        metric_code=metric_code,
        geo_id=geo_id,
        geo_level=geo_level,
        period_start=period,
        period_end="2023-12-31",
        release=release,
        value=value,
        value_status=status,
        uncertainty=ObservationUncertainty(margin_of_error=moe),
    )


@pytest.fixture
def poverty():
    return pairing_for("poverty_share:acs5")


# ---------------------------------------------------------------------------
# API-173 — the computation
# ---------------------------------------------------------------------------


def test_the_proportion_formula_gives_the_bureaus_margin() -> None:
    """Covers: API-173 — MOE(p) = sqrt(MOE_X^2 - p^2 MOE_Y^2) / Y."""
    margin, method = derived_margin(
        Decimal("0.2"), Decimal(100), Decimal(5000), Decimal(200), subset=True
    )
    assert method == "proportion"
    # sqrt(100^2 - 0.2^2 * 200^2) / 5000 = sqrt(8400) / 5000
    assert abs(margin - Decimal("0.0183303")) < Decimal("0.0000001")


def test_a_negative_radicand_falls_back_to_the_ratio_formula() -> None:
    """Covers: API-173 — the Bureau's instruction when the proportion radicand < 0."""
    margin, method = derived_margin(
        Decimal("0.5"), Decimal(10), Decimal(200), Decimal(100), subset=True
    )
    assert method == "ratio_fallback"
    # sqrt(10^2 + 0.5^2 * 100^2) / 200 = sqrt(2600) / 200
    assert abs(margin - Decimal("0.2549510")) < Decimal("0.0000001")


def test_a_ratio_adds_the_denominator_term() -> None:
    """Covers: API-173 — a ratio that is not a proportion adds both terms."""
    margin, method = derived_margin(
        Decimal("4"), Decimal(30), Decimal(100), Decimal(10), subset=False
    )
    assert method == "ratio"
    # sqrt(30^2 + 4^2 * 10^2) / 100 = sqrt(2500) / 100
    assert margin == Decimal(50) / Decimal(100)


def test_a_share_is_scaled_rounded_and_labelled(poverty) -> None:
    """Covers: API-173 — the value names both inputs, the pairing and its MOE method."""
    row = derive_row(
        poverty,
        _acs(POVERTY, "1000", "100"),
        _acs(POVERTY_UNIVERSE, "5000", "200"),
        denominator_elsewhere=True,
    )
    assert row.value == "20.00"
    assert row.margin_of_error == "1.83"
    assert row.margin_of_error_method == "proportion"
    assert row.derived is True
    assert row.unit == "percent" and row.scale == 100
    assert row.ratio_id == "poverty_share:acs5"
    assert row.pairing_status == poverty.status
    assert row.numerator.metric_code == POVERTY
    assert row.denominator.metric_code == POVERTY_UNIVERSE
    assert row.denominator.release == "2023"
    assert row.numerator.metric_label  # the plain label rides along (A-2)
    assert row.unavailable_reason is None


@pytest.mark.parametrize(
    ("numerator", "denominator", "elsewhere", "reason"),
    [
        (("1000", "100", "valid"), None, False, "denominator_missing"),
        (("1000", "100", "valid"), None, True, "different_period"),
        (("1000", "100", "valid"), ("0", "10", "valid"), True, "denominator_zero"),
        ((None, None, "sentinel"), ("5000", "200", "valid"), True, "suppressed_input"),
        (("1000", "100", "valid"), (None, None, "sentinel"), True, "suppressed_input"),
        ((None, None, "withheld"), ("5000", "200", "valid"), True, "suppressed_input"),
        (("n/a", "4", "valid"), ("5000", "200", "valid"), True, "suppressed_input"),
    ],
)
def test_a_row_that_cannot_be_computed_says_why_and_is_never_zero(
    poverty, numerator, denominator, elsewhere, reason
) -> None:
    """Covers: API-173 — ADR-0009 rule 2: missing, suppressed and zero inputs."""
    num = _acs(POVERTY, numerator[0], numerator[1], status=numerator[2])
    den = (
        None
        if denominator is None
        else _acs(
            POVERTY_UNIVERSE, denominator[0], denominator[1], status=denominator[2]
        )
    )
    row = derive_row(poverty, num, den, denominator_elsewhere=elsewhere)
    assert row.value is None
    assert row.margin_of_error is None
    assert row.unavailable_reason == reason


def test_periods_and_releases_are_never_mixed(poverty) -> None:
    """Covers: API-173 — a denominator from another release or period is not matched."""
    numerators = [
        _acs(POVERTY, "1000", "100", geo_id="A", release="2023"),
        _acs(POVERTY, "900", "90", geo_id="B", release="2023"),
        _acs(POVERTY, "800", "80", geo_id="C", release="2023"),
    ]
    denominators = [
        _acs(POVERTY_UNIVERSE, "5000", "200", geo_id="A", release="2023"),
        _acs(POVERTY_UNIVERSE, "4000", "200", geo_id="B", release="2022"),
    ]
    rows = pair_rows(poverty, numerators, denominators)
    assert [row.value for row in rows] == ["20.00", None, None]
    assert [row.unavailable_reason for row in rows] == [
        None,
        "different_period",
        "denominator_missing",
    ]


def test_a_controlled_denominator_margin_is_zero_only_where_declared() -> None:
    """Covers: API-173 — the Bureau's '*****' is zero margin; elsewhere a null is unknown."""
    pairing = pairing_for("under_18_share:acs5")
    num = "CENSUS_ACS:acs5:B09001_001"
    den = "CENSUS_ACS:acs5:B01003_001"
    county = derive_row(pairing, _acs(num, "250", "30"), _acs(den, "1000", None), True)
    assert county.value == "25.00"
    assert county.margin_of_error == "3.00"  # sqrt(30^2 - 0) / 1000 * 100
    assert county.margin_of_error_method == "proportion"
    assert county.denominator.margin_of_error_controlled is True

    place = derive_row(
        pairing,
        _acs(num, "250", "30", geo_level="PLACE"),
        _acs(den, "1000", None, geo_level="PLACE"),
        True,
    )
    assert place.value == "25.00"
    assert place.margin_of_error is None
    assert place.margin_of_error_method == "unavailable"
    assert place.denominator.margin_of_error_controlled is False


def test_a_null_numerator_margin_leaves_the_margin_unavailable(poverty) -> None:
    """Covers: API-173 — an unpublished margin is not taken as zero."""
    row = derive_row(
        poverty,
        _acs(POVERTY, "1000", None),
        _acs(POVERTY_UNIVERSE, "5000", "200"),
        True,
    )
    assert row.value == "20.00"
    assert row.margin_of_error is None
    assert row.margin_of_error_method == "unavailable"


# ---------------------------------------------------------------------------
# The denominator read
# ---------------------------------------------------------------------------


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return 1


class _Session:
    def __init__(self, metric_row):
        self.metric_row = metric_row
        self.statements: list[tuple[str, dict[str, Any]]] = []
        self.info: dict = {}

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append((rendered, dict(params or {})))
        if "gold_glossary.dim_metric" in rendered:
            return _Result([self.metric_row] if self.metric_row else [])
        if "to_regclass" in rendered or "information_schema" in rendered:
            return _Result([{"exists": True}])
        return _Result(
            [
                {
                    "release": "2023",
                    "period_start": "2019-01-01",
                    "period_end": "2023-12-31",
                    "geo_id": "0500000US48453",
                    "geo_level": "COUNTY",
                    "value": "5000",
                    "value_status": "valid",
                    "u_margin_of_error": "200",
                }
            ]
        )


def test_the_denominator_is_read_for_exactly_the_pages_geographies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-173 — bound geography list, same release pin, every period."""
    monkeypatch.setattr(
        "apps.api.services.neutral_observations_service.require_relation",
        lambda db, relation: None,
    )
    session = _Session(
        {
            "metric_code": POVERTY_UNIVERSE,
            "metric_display_name": "Estimate!!Total:",
            "source_code": "CENSUS_ACS",
        }
    )
    rows = observations_for_geographies(
        session, POVERTY_UNIVERSE, "as_released", "2023", ["0500000US48453"]
    )
    assert [row.value for row in rows] == ["5000"]
    assert rows[0].uncertainty.margin_of_error == "200"
    sql, params = session.statements[-1]
    assert "gold_census.rpt_acs_observations" in sql
    assert "geo_id = ANY(:geo_ids)" in sql
    assert params["geo_ids"] == ["0500000US48453"]
    # An ACS release is pinned on the vintage column (API-182).
    assert params["release_number"] == 2023
    assert "vintage_year = :release_number" in sql
    assert params["metric_code_value"] == POVERTY_UNIVERSE
    assert "0500000US48453" not in sql  # bound, never interpolated

    assert (
        observations_for_geographies(session, POVERTY_UNIVERSE, "latest", None, [])
        == []
    )
    with pytest.raises(NeutralQueryError):
        observations_for_geographies(session, POVERTY_UNIVERSE, "other", None, ["x"])


# ---------------------------------------------------------------------------
# API-174 — the routes
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    def _override():
        yield object()

    app.dependency_overrides[get_db_session_dep] = _override
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def _page(metric_code: str, items: list[NeutralObservation]):
    return NeutralObservationListResponse(
        metric_code=metric_code,
        source_code=items[0].source_code if items else "CENSUS_ACS",
        scope="latest",
        total=len(items),
        limit=100,
        offset=0,
        items=items,
    )


def test_the_catalog_lists_pairings_and_says_which_are_served(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — a card learns which views its count offers."""
    monkeypatch.delenv(SERVE_DRAFTS_ENV, raising=False)
    response = client.get(
        "/api/v1/catalog/derived-ratios", params={"numerator": POVERTY}
    )
    assert response.status_code == 200
    payload = response.json()
    assert [item["ratio_id"] for item in payload["items"]] == ["poverty_share:acs5"]
    item = payload["items"][0]
    assert item["denominator"] == POVERTY_UNIVERSE
    assert item["universe"] == "people whose poverty status is determined"
    assert item["served"] is (item["status"] == "approved")

    everything = client.get("/api/v1/catalog/derived-ratios").json()
    assert everything["total"] == len(load_registry())
    served = client.get(
        "/api/v1/catalog/derived-ratios", params={"served_only": "true"}
    ).json()
    assert all(item["served"] for item in served["items"])


def test_a_draft_pairing_is_refused_unless_opted_in(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — ADR-0009 rule 1: only approved pairings are served."""
    drafts = {
        ratio_id: dataclasses.replace(pairing, status="draft")
        for ratio_id, pairing in derived_ratios.load_registry().items()
    }
    monkeypatch.setattr(derived_ratios, "load_registry", lambda *args: drafts)
    monkeypatch.delenv(SERVE_DRAFTS_ENV, raising=False)
    calls: list = []
    monkeypatch.setattr(
        derived_ratio_service,
        "list_neutral_observations",
        lambda *args, **kwargs: calls.append(kwargs),
    )
    response = client.get(
        "/api/v1/observations/derived", params={"ratio_id": "poverty_share:acs5"}
    )
    assert response.status_code == 422
    assert "approved" in response.json()["detail"]
    assert calls == []  # refused before any read


def test_an_unknown_pairing_is_not_found(client) -> None:
    """Covers: API-174 — an unknown pairing is a 404."""
    response = client.get(
        "/api/v1/observations/derived", params={"ratio_id": "no_such"}
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "ratio_id not found"


def test_the_route_divides_the_numerators_page_by_its_denominators(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — the numerator is read as /observations reads it."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    seen: dict[str, Any] = {}

    def numerator_page(db, **kwargs):
        seen["numerator"] = kwargs
        return _page(POVERTY, [_acs(POVERTY, "1000", "100")])

    def denominators(db, metric_code, scope, release, geo_ids):
        seen["denominator"] = (metric_code, scope, release, geo_ids)
        return [_acs(POVERTY_UNIVERSE, "5000", "200")]

    monkeypatch.setattr(
        derived_ratio_service, "list_neutral_observations", numerator_page
    )
    monkeypatch.setattr(
        derived_ratio_service, "observations_for_geographies", denominators
    )
    response = client.get(
        "/api/v1/observations/derived",
        params={
            "ratio_id": "poverty_share:acs5",
            "geo_level": "county",
            "state_fips": "48",
            "newest_per_geography": "true",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert seen["numerator"]["metric_code"] == POVERTY
    assert seen["numerator"]["filters"]["state_fips"] == "48"
    assert seen["numerator"]["newest_per_geography"] is True
    assert seen["denominator"] == (POVERTY_UNIVERSE, "latest", None, ["0500000US48453"])
    assert payload["ratio"]["ratio_id"] == "poverty_share:acs5"
    assert payload["total"] == 1
    row = payload["items"][0]
    assert row["value"] == "20.00"
    assert row["margin_of_error"] == "1.83"
    assert row["derived"] is True
    assert row["pairing_status"] == "approved"


def test_a_provider_rate_is_served_as_the_providers(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — ADR-0009 rule 5: no recomputed value beside it."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    rate = "FBI_UCR:summarized_violent_crime:V:offense:rate"
    seen: dict[str, Any] = {}

    def rate_page(db, **kwargs):
        seen.update(kwargs)
        return _page(
            rate,
            [
                NeutralObservation(
                    source_code="FBI_UCR",
                    metric_code=rate,
                    geo_id="0400000US55",
                    geo_level="STATE",
                    period_start="2023-01-01",
                    release="2024",
                    value="295.2",
                    # FBI UCR's own word for a published number (not "valid").
                    value_status="reported",
                )
            ],
        )

    def no_denominator(*args, **kwargs):
        raise AssertionError("a provider rate reads no denominator")

    monkeypatch.setattr(derived_ratio_service, "list_neutral_observations", rate_page)
    monkeypatch.setattr(
        derived_ratio_service, "observations_for_geographies", no_denominator
    )
    response = client.get(
        "/api/v1/observations/derived", params={"ratio_id": "violent_crime_rate"}
    )
    assert response.status_code == 200, response.text
    assert seen["metric_code"] == rate
    row = response.json()["items"][0]
    assert row["derived"] is False
    assert row["value"] == "295.2"
    assert row["unit"] == "per 100,000 people"
    assert row["provider_rate"]["metric_code"] == rate
    assert row["numerator"] is None and row["denominator"] is None
    assert row["margin_of_error_method"] == "none"


def test_the_route_refuses_what_the_numerator_read_refuses(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — the numerator read's refusals are the route's."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")

    def refused(db, **kwargs):
        raise NeutralQueryError("filters not supported")

    monkeypatch.setattr(derived_ratio_service, "list_neutral_observations", refused)
    response = client.get(
        "/api/v1/observations/derived", params={"ratio_id": "poverty_share:acs5"}
    )
    assert response.status_code == 422
    reversed_years = client.get(
        "/api/v1/observations/derived",
        params={"ratio_id": "poverty_share:acs5", "year_from": 2020, "year_to": 2010},
    )
    assert reversed_years.status_code == 422


def test_a_denominator_the_catalog_does_not_publish_is_explained(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — registry drift is explained, not answered empty."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    monkeypatch.setattr(
        derived_ratio_service,
        "list_neutral_observations",
        lambda db, **kwargs: _page(POVERTY, [_acs(POVERTY, "1", "1")]),
    )
    monkeypatch.setattr(
        derived_ratio_service, "observations_for_geographies", lambda *a: None
    )
    response = client.get(
        "/api/v1/observations/derived", params={"ratio_id": "poverty_share:acs5"}
    )
    assert response.status_code == 422
    assert POVERTY_UNIVERSE in response.json()["detail"]


@pytest.mark.parametrize("status", ["reported", None, "valid", "estimated"])
def test_a_published_number_is_usable_in_any_sources_vocabulary(status) -> None:
    """Covers: API-174 — usability is the neutral contract's: a non-null number."""
    pairing = pairing_for("violent_crime_rate")
    row = derived_ratio_service.provider_row(
        pairing,
        NeutralObservation(
            source_code="FBI_UCR",
            metric_code=pairing.provider_rate,
            geo_id="0400000US02",
            geo_level="STATE",
            period_start="2022-01-01",
            value="59.43",
            value_status=status,
        ),
    )
    assert row.value == "59.43"
    assert row.unavailable_reason is None


def test_a_provider_row_without_a_number_is_unavailable() -> None:
    """Covers: API-174 — an unreported rate is never zero."""
    pairing = pairing_for("violent_crime_rate")
    row = derived_ratio_service.provider_row(
        pairing,
        NeutralObservation(
            source_code="FBI_UCR",
            metric_code=pairing.provider_rate,
            geo_id="0400000US02",
            value=None,
            value_status="not_reported",
        ),
    )
    assert row.value is None
    assert row.unavailable_reason == "suppressed_input"


# ---------------------------------------------------------------------------
# Minutes of work: a time cost (ADR-0009 amendment, 2026-11-06)
# ---------------------------------------------------------------------------

EGGS_MIDWEST = "BLS:APU0200708111"
WAGE = "BLS_QCEW:annual_avg_weekly_wage:10:0"
DANE = "state:55|county:025"


def _price(value: str | None, period: str = "2025-09-01") -> NeutralObservation:
    return NeutralObservation(
        source_code="BLS",
        metric_code=EGGS_MIDWEST,
        geo_id="region:2",
        geo_level="CENSUS_REGION",
        geo_name="Midwest Region",
        period_start=period,
        period_end=period,
        value=value,
        value_status="valid" if value is not None else "missing",
    )


def _wage(
    value: str | None, year: int, geo_id: str = DANE, name: str = "Dane County"
) -> NeutralObservation:
    return NeutralObservation(
        source_code="BLS_QCEW",
        metric_code=WAGE,
        geo_id=geo_id,
        geo_level="COUNTY",
        geo_name=name,
        period_start=f"{year}-01-01",
        period_end=f"{year}-12-31",
        release=f"{year}",
        value=value,
    )


def test_minutes_of_work_divide_a_price_by_the_named_areas_hourly_wage() -> None:
    """Covers: API-173 — $3.00 at a $1,200 week of 40 hours is 6 minutes."""
    pairing = pairing_for("minutes_of_work_eggs:0200")
    assert pairing.kind == "time_cost" and pairing.numerator == EGGS_MIDWEST
    assert pairing.denominator == WAGE and pairing.hours_per_week == 40
    [row] = time_cost_rows(pairing, [_price("3.00")], [_wage("1200", 2025)], DANE)
    assert row.value == "6.00"
    assert row.unit == "minutes of work" and row.derived is True
    assert row.geo_id == "region:2"  # the price's area
    assert (row.wage_geo_id, row.wage_geo_name) == (DANE, "Dane County")
    assert row.wage_year == "same_year"
    assert row.numerator.metric_code == EGGS_MIDWEST
    assert row.denominator.metric_code == WAGE and row.denominator.value == "1200"


def test_a_price_newer_than_every_wage_takes_the_newest_year_and_says_so() -> None:
    """Covers: API-173 — QCEW lags the price; the substitution is stated."""
    pairing = pairing_for("minutes_of_work_eggs:0200")
    wages = [_wage("1100", 2023), _wage("1200", 2024)]
    newer, same, older = time_cost_rows(
        pairing,
        [
            _price("3.00", "2026-09-01"),
            _price("2.75", "2023-06-01"),
            _price("2", "2019-01-01"),
        ],
        wages,
        DANE,
    )
    assert newer.value == "6.00" and newer.wage_year == "newest_published_year"
    assert newer.denominator.period_start == "2024-01-01"
    assert same.value == "6.00" and same.wage_year == "same_year"
    # Older than every published wage: never another year's wage.
    assert older.value is None and older.unavailable_reason == "different_period"


def test_a_missing_or_withheld_input_is_never_zero_minutes() -> None:
    """Covers: API-173 — no wage, a withheld wage, a missing price."""
    pairing = pairing_for("minutes_of_work_eggs:0200")
    [none] = time_cost_rows(pairing, [_price("3.00")], [], DANE)
    assert none.value is None and none.unavailable_reason == "denominator_missing"
    [withheld] = time_cost_rows(pairing, [_price("3.00")], [_wage(None, 2025)], DANE)
    assert withheld.unavailable_reason == "suppressed_input"
    [zero] = time_cost_rows(pairing, [_price("3.00")], [_wage("0", 2025)], DANE)
    assert zero.unavailable_reason == "denominator_zero"
    [missing] = time_cost_rows(pairing, [_price(None)], [_wage("1200", 2025)], DANE)
    assert missing.unavailable_reason == "suppressed_input"
    # Another area's wage is never used.
    [other] = time_cost_rows(
        pairing, [_price("3.00")], [_wage("1200", 2025, geo_id="state:55")], DANE
    )
    assert other.unavailable_reason == "denominator_missing"


def test_the_route_reads_the_wage_for_the_named_area_without_the_price_pin(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-174 — the wage area is the reader's; QCEW is never pinned to BLS's release."""
    seen: dict[str, Any] = {}

    def price_page(db, **kwargs):
        seen["price"] = kwargs
        return _page(EGGS_MIDWEST, [_price("3.00")])

    def wages(db, metric_code, scope, release, geo_ids):
        seen["wage"] = (metric_code, scope, release, geo_ids)
        return [_wage("1200", 2025)]

    monkeypatch.setattr(derived_ratio_service, "list_neutral_observations", price_page)
    monkeypatch.setattr(derived_ratio_service, "observations_for_geographies", wages)
    response = client.get(
        "/api/v1/observations/derived",
        params={
            "ratio_id": "minutes_of_work_eggs:0200",
            "geo_id": "region:2",
            "wage_geo_id": DANE,
            "release": "2025-10",
            "newest_per_geography": "true",
        },
    )
    assert response.status_code == 200, response.text
    assert seen["price"]["metric_code"] == EGGS_MIDWEST
    assert seen["price"]["release"] == "2025-10"
    assert seen["wage"] == (WAGE, "latest", None, [DANE])
    payload = response.json()
    assert payload["ratio"]["kind"] == "time_cost"
    assert payload["ratio"]["area"] == "0200"
    row = payload["items"][0]
    assert row["value"] == "6.00" and row["wage_geo_id"] == DANE


@pytest.mark.parametrize(
    ("params", "detail"),
    [
        ({"ratio_id": "minutes_of_work_eggs:0200"}, "wage_geo_id"),
        (
            {"ratio_id": "minutes_of_work_eggs:0200", "wage_geo_id": "cbsa:31540"},
            "county, state, national",
        ),
        (
            {"ratio_id": "poverty_share:acs5", "wage_geo_id": DANE},
            "only to a time_cost",
        ),
    ],
)
def test_the_route_refuses_a_time_cost_without_a_wage_area_it_can_read(
    client, monkeypatch: pytest.MonkeyPatch, params: dict, detail: str
) -> None:
    """Covers: API-174 — QCEW publishes no metro wage; the area is required."""
    monkeypatch.setattr(
        derived_ratio_service,
        "list_neutral_observations",
        lambda db, **kwargs: _page(EGGS_MIDWEST, [_price("3.00")]),
    )
    response = client.get("/api/v1/observations/derived", params=params)
    assert response.status_code == 422
    assert detail in response.text


def test_minutes_of_work_are_not_offered_for_a_comparison_across_places(
    client,
) -> None:
    """Covers: API-174 — each place would need its own wage area."""
    response = client.get(
        "/api/v1/comparison/places",
        params={
            "ratio_id": "minutes_of_work_eggs:0200",
            "geo_id_a": "state:55",
            "geo_id_b": "state:17",
        },
    )
    assert response.status_code == 422
    assert "not compared across places" in response.text


def test_a_county_childcare_price_is_hours_of_the_countys_own_wage() -> None:
    """Covers: ETL-123 — $293 a week at Dane's $1,250 week of 40 hours is 562.56 minutes."""
    pairing = pairing_for("minutes_of_work_childcare_center_infant")
    assert pairing.numerator == "DOL_CHILDCARE:center_infant"
    assert pairing.wage_geo_levels == ("COUNTY",) and pairing.area is None
    price = NeutralObservation(
        source_code="DOL_CHILDCARE",
        metric_code="DOL_CHILDCARE:center_infant",
        geo_id=DANE,
        geo_level="COUNTY",
        geo_name="Dane County",
        period_start="2022-01-01",
        period_end="2022-12-31",
        value="293",
        value_status="valid",
    )
    [row] = time_cost_rows(pairing, [price], [_wage("1250", 2022)], DANE)
    assert row.value == "562.56" and row.wage_year == "same_year"
    assert row.geo_id == DANE and row.denominator.value == "1250"


# ---------------------------------------------------------------------------
# Starting a family (starting-a-family-collection)
# ---------------------------------------------------------------------------


def _nces(
    metric_code: str,
    value: str | None,
    completeness: str,
    release: str,
    geo_id: str = DANE,
    period: str = "2024-07-01",
) -> NeutralObservation:
    return NeutralObservation(
        source_code="NCES_CCD",
        metric_code=metric_code,
        geo_id=geo_id,
        geo_level="COUNTY",
        geo_name="Dane County",
        period_start=period,
        period_end="2025-06-30",
        release=release,
        value=value,
        value_status="valid" if value is not None else "missing",
        dimensions={"completeness": completeness},
    )


def test_students_per_teacher_divides_two_whole_sums_across_their_files() -> None:
    """Covers: API-173 — NCES membership and staff are separate files of one school year."""
    pairing = pairing_for("students_per_teacher")
    assert pairing.kind == "ratio" and pairing.scale == 1
    assert pairing.complete_inputs_only and not pairing.match_release
    students = "NCES_CCD:student_membership"
    teachers = "NCES_CCD:teacher_fte"
    numerators = [
        _nces(students, "75000", "complete", "CCD membership read 1"),
        _nces(students, "60000", "partial", "CCD membership read 1", geo_id="B"),
        _nces(students, "50000", "complete", "CCD membership read 1", geo_id="C"),
        _nces(students, "40000", "complete", "CCD membership read 1", geo_id="D"),
    ]
    denominators = [
        _nces(teachers, "5000", "complete", "CCD staff read 2"),
        _nces(teachers, "4000", "complete", "CCD staff read 2", geo_id="B"),
        _nces(teachers, "4000", "partial", "CCD staff read 2", geo_id="C"),
        # Another school year is still never matched.
        _nces(teachers, "4000", "complete", "CCD staff read 2", "D", "2023-07-01"),
    ]
    whole, partial_students, partial_teachers, other_year = pair_rows(
        pairing, numerators, denominators
    )
    assert whole.value == "15.00" and whole.unavailable_reason is None
    assert whole.numerator.release == "CCD membership read 1"
    assert whole.denominator.release == "CCD staff read 2"
    assert partial_students.value is None
    assert partial_students.unavailable_reason == "partial_input"
    assert partial_teachers.unavailable_reason == "partial_input"
    assert other_year.value is None
    assert other_year.unavailable_reason == "different_period"


def test_a_release_matched_pairing_still_ignores_completeness() -> None:
    """Covers: API-173 — only a pairing marked complete_inputs_only reads completeness."""
    share = pairing_for("households_with_children_share:acs5")
    assert share.kind == "share" and share.match_release
    assert not share.complete_inputs_only
    [row] = pair_rows(
        share,
        [_acs("CENSUS_ACS:acs5:B11005_002", "60000", "900")],
        [_acs("CENSUS_ACS:acs5:B11005_001", "240000", "1200")],
    )
    assert row.value == "25.00"


def test_a_three_bedroom_fair_market_rent_is_hours_of_the_countys_own_wage() -> None:
    """Covers: API-173 — $2,000 a month at a $1,250 week of 40 hours is 3,840 minutes."""
    pairing = pairing_for("minutes_of_work_rent_3br")
    assert pairing.kind == "time_cost" and pairing.numerator == "HUD_FMR_IL:fmr_3br"
    assert pairing.wage_geo_levels == ("COUNTY",) and pairing.area is None
    rent = NeutralObservation(
        source_code="HUD_FMR_IL",
        metric_code="HUD_FMR_IL:fmr_3br",
        geo_id=DANE,
        geo_level="COUNTY",
        geo_name="Dane County",
        # FY2027 starts on 2026-10-01; QCEW's newest year here is 2025.
        period_start="2026-10-01",
        period_end="2027-09-30",
        release="FY2027",
        value="2000",
        value_status="valid",
    )
    [row] = time_cost_rows(pairing, [rent], [_wage("1250", 2025)], DANE)
    assert row.value == "3840.00"
    assert row.wage_year == "newest_published_year"
    assert row.denominator.period_start == "2025-01-01"


def test_a_summed_numerator_adds_its_tables_lines_before_dividing() -> None:
    """Covers: API-173 — DP03's arithmetic: B23008_004 + _010 + _013 over _002."""
    pairing = pairing_for("young_children_all_parents_in_labor_force_share:acs5")
    assert pairing is not None
    both = _acs("CENSUS_ACS:acs5:B23008_004", "18000", "900")
    father = _acs("CENSUS_ACS:acs5:B23008_010", "1000", "300")
    mother = _acs("CENSUS_ACS:acs5:B23008_013", "5000", "600")
    under6 = _acs("CENSUS_ACS:acs5:B23008_002", "32000", "400")
    [row] = pair_rows(pairing, [both], [under6], [[father], [mother]])
    assert row.unavailable_reason is None
    assert Decimal(row.value) == Decimal("75")
    # The sum's margin is the root of the squared margins: sqrt(900²+300²+600²).
    assert Decimal(row.numerator.value) == Decimal("24000")
    assert Decimal(row.numerator.margin_of_error).quantize(Decimal("0.01")) == Decimal(
        "1122.50"
    )
    assert row.numerator.metric_code == (
        "CENSUS_ACS:acs5:B23008_004 + CENSUS_ACS:acs5:B23008_010 + "
        "CENSUS_ACS:acs5:B23008_013"
    )
    assert row.margin_of_error is not None

    # A line missing, or published with no number: a partial sum is not divided.
    for parts in ([[father], []], [[father], [_acs(mother.metric_code, None, None)]]):
        [row] = pair_rows(pairing, [both], [under6], parts)
        assert row.unavailable_reason == "partial_input"
        assert row.value is None
