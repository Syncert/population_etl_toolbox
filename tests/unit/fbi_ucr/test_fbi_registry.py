"""Frozen FBI CDE product, offense, and subject contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import (
    ALL_PRODUCTS,
    SUMMARIZED_OFFENSES,
    SUMMARIZED_VIOLENT_CRIME,
    UNSUPPORTED_STATE_CODES,
    FbiSubject,
    FbiUcrProduct,
    agency_directory_endpoint,
    canonical_state_fips,
    enabled_products,
    get_product,
    published_state_label,
)

pytestmark = pytest.mark.unit


def test_registered_product_freezes_the_documented_request_shape() -> None:
    """Covers: ETL-030 — the frozen product renders documented CDE paths."""
    product = SUMMARIZED_VIOLENT_CRIME

    assert product.offense_code in SUMMARIZED_OFFENSES
    assert product.offense_label == "Violent Crime"
    assert product.period_parameters == {"from": "01-1990", "to": "06-2023"}
    assert product.expected_periods[0] == "01-1990"
    assert product.expected_periods[-1] == "06-2023"
    # 33 full years (1990-2022) plus January-June 2023, every month exactly once.
    assert len(product.expected_periods) == 33 * 12 + 6
    assert len(set(product.expected_periods)) == len(product.expected_periods)
    assert product.observation_endpoint(FbiSubject("national", "US")) == (
        "/summarized/national/V"
    )
    assert product.observation_endpoint(FbiSubject("state", "WI")) == (
        "/summarized/state/WI/V"
    )
    assert product.observation_endpoint(FbiSubject("agency", "WI0130000")) == (
        "/summarized/agency/WI0130000/V"
    )
    assert agency_directory_endpoint("WI") == "/agency/byStateAbbr/WI"


def test_expected_periods_cross_the_year_boundary_without_gaps() -> None:
    """Covers: ETL-013 — month windows enumerate every period exactly once."""
    product = FbiUcrProduct(
        product_id="boundary",
        label="boundary",
        ucr_program="SRS_AND_SUMMARIZED_NIBRS",
        offense_code="V",
        period_start="11-2022",
        period_end="02-2023",
        state_scope=(),
        agency_scope=(),
        parser_contract_version="test",
        documentation_url="https://example.invalid/docs",
        methodology_url="https://example.invalid/method",
        reported_status="reported",
        counted_entity_note="test",
    )

    assert product.expected_periods == ("11-2022", "12-2022", "01-2023", "02-2023")


@pytest.mark.parametrize(
    ("start", "end"),
    [("13-2023", "06-2023"), ("01-2023", "00-2023"), ("2023-01", "2023-06")],
)
def test_undocumented_period_format_is_rejected(start: str, end: str) -> None:
    """Covers: ETL-030 — a period outside the documented format is rejected."""
    with pytest.raises(ValueError, match="mm-yyyy"):
        FbiUcrProduct(
            product_id="invalid",
            label="invalid",
            ucr_program="SRS_AND_SUMMARIZED_NIBRS",
            offense_code="V",
            period_start=start,
            period_end=end,
            state_scope=(),
            agency_scope=(),
            parser_contract_version="test",
            documentation_url="https://example.invalid/docs",
            methodology_url="https://example.invalid/method",
            reported_status="reported",
            counted_entity_note="test",
        )


def test_reversed_period_window_is_rejected() -> None:
    """Covers: ETL-030 — an end period before its start is rejected."""
    with pytest.raises(ValueError, match="period_end must not precede"):
        FbiUcrProduct(
            product_id="invalid",
            label="invalid",
            ucr_program="SRS_AND_SUMMARIZED_NIBRS",
            offense_code="V",
            period_start="06-2023",
            period_end="01-2023",
            state_scope=(),
            agency_scope=(),
            parser_contract_version="test",
            documentation_url="https://example.invalid/docs",
            methodology_url="https://example.invalid/method",
            reported_status="reported",
            counted_entity_note="test",
        )


@pytest.mark.parametrize(
    "ori", ["WI013000", "WI01300000", "wi0130000", "", "WI-013000"]
)
def test_malformed_ori_is_rejected(ori: str) -> None:
    """Covers: ETL-010 — an ORI outside the published form is rejected."""
    with pytest.raises(ValueError, match="invalid ORI"):
        FbiSubject("agency", ori)


def test_undocumented_offense_or_state_is_rejected() -> None:
    """Covers: ETL-030 — offenses and states stay inside the documented sets."""
    with pytest.raises(ValueError, match="undocumented state code"):
        FbiSubject("state", "ZZ")
    with pytest.raises(ValueError, match="undocumented state code"):
        agency_directory_endpoint("ZZ")
    with pytest.raises(ValueError, match="undocumented agency query type"):
        agency_directory_endpoint("WI", query="byName")


def test_subject_keeps_the_source_native_geography_level() -> None:
    """Covers: ETL-002 — agency subjects never borrow a Census level."""
    assert FbiSubject("national", "US").source_geo_level == "us:1"
    assert FbiSubject("state", "WI").source_geo_level == "state:WI"
    assert FbiSubject("agency", "WI0130000").source_geo_level == (
        "fbi_agency:WI0130000"
    )
    assert FbiSubject("agency", "WI0130000").slice_key == "agency:WI0130000"


def test_documented_states_without_a_census_code_never_resolve() -> None:
    """Covers: ETL-003 — non-state provider codes stay unsupported."""
    assert canonical_state_fips("WI") == "55"
    assert published_state_label("WI") == "Wisconsin"
    for code in UNSUPPORTED_STATE_CODES:
        assert canonical_state_fips(code) is None
        assert published_state_label(code) is None


def test_agency_scope_derives_its_own_reference_states() -> None:
    """Covers: ETL-030 — every scoped agency requires its directory slice."""
    product = SUMMARIZED_VIOLENT_CRIME

    assert product.reference_states == ("WI",)
    assert {subject.subject_type for subject in product.subjects} == {
        "national",
        "state",
        "agency",
    }
    assert len(product.subjects) == 1 + len(product.state_scope) + len(
        product.agency_scope
    )


def test_measure_identity_separates_form_and_counted_entity() -> None:
    """Covers: ETL-023 — totals, rates, and clearances never share identity."""
    product = SUMMARIZED_VIOLENT_CRIME
    identities = {
        product.measure_id(basis, form)
        for basis in ("offense", "clearance")
        for form in ("absolute_total", "rate")
    }

    assert len(identities) == 4
    assert product.measure_id("offense", "absolute_total") == "V:offense:absolute_total"


def test_registry_lookup_is_stable_and_rejects_unknown_products() -> None:
    """Covers: ETL-030 — product lookup is explicit and deterministic."""
    assert enabled_products() == [
        product for product in ALL_PRODUCTS if product.enabled
    ]
    assert get_product("summarized_violent_crime") is SUMMARIZED_VIOLENT_CRIME
    with pytest.raises(KeyError):
        get_product("summarized_hate_crime")
