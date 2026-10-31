"""API unit tests: per-capita and share values, a declared derived product.

Covers: API-173 (a derived value divides the numerator row by the
        denominator row of the same geography, period and release, with the
        Bureau's margin of error and a reason, never zero, when it cannot be
        computed), API-174 (the pairing catalog and the serving gate: only an
        approved pairing is served, a provider's own rate is served as the
        provider's, and the route refuses what it cannot answer).
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
