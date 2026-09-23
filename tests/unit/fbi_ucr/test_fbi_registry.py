"""Frozen FBI CDE product, offense, and subject contracts."""

from __future__ import annotations

import dataclasses

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import (
    ALL_PRODUCTS,
    SUMMARIZED_OFFENSES,
    STATE_CODE_CONTRACT,
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


#: Every contract field of the first published product, frozen as published.
#: Web templates and saved analyses address ``FBI_UCR:summarized_violent_crime``
#: measures, and v1 identities are promised, so none of this may drift.
VIOLENT_CRIME_CONTRACT = {
    "product_id": "summarized_violent_crime",
    "label": "Summarized violent crime offenses and clearances",
    "ucr_program": "SRS_AND_SUMMARIZED_NIBRS",
    "offense_code": "V",
    "period_start": "01-1990",
    "period_end": "06-2023",
    "state_scope": tuple(STATE_CODE_CONTRACT),
    "agency_scope": (
        "WI0130000",
        "WI0137000",
        "WI0540300",
        "WI0050700",
        "WI0400100",
        "WIWSP0000",
    ),
    "parser_contract_version": "fbi-cde-summarized-v1",
    "documentation_url": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi",
    "methodology_url": (
        "https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr"
    ),
    "reported_status": "reported",
    "counted_entity_note": (
        "Offense series count reported offenses; clearance series count cleared "
        "offenses. The two are different counted entities and are never added."
    ),
    "include_national": True,
    "enabled": True,
    "media_type": "application/json",
}

EXPECTED_PRODUCT_IDS = {
    "V": "summarized_violent_crime",
    "ASS": "summarized_assault",
    "BUR": "summarized_burglary",
    "LAR": "summarized_larceny",
    "MVT": "summarized_motor_vehicle_theft",
    "HOM": "summarized_homicide",
    "RPE": "summarized_rape",
    "ROB": "summarized_robbery",
    "ARS": "summarized_arson",
    "P": "summarized_property_crime",
}


def test_every_documented_summarized_offense_is_one_registered_product() -> None:
    """Covers: ETL-052 — one frozen product per documented offense code."""
    assert [product.offense_code for product in ALL_PRODUCTS] == list(
        SUMMARIZED_OFFENSES
    )
    assert {
        product.offense_code: product.product_id for product in ALL_PRODUCTS
    } == EXPECTED_PRODUCT_IDS
    assert len({product.product_id for product in ALL_PRODUCTS}) == len(ALL_PRODUCTS)
    for product in ALL_PRODUCTS:
        assert get_product(product.product_id) is product


def test_violent_crime_contract_is_unchanged_by_the_new_products() -> None:
    """Covers: ETL-052 — the first published product keeps every field."""
    assert dataclasses.asdict(SUMMARIZED_VIOLENT_CRIME) == VIOLENT_CRIME_CONTRACT
    assert {
        SUMMARIZED_VIOLENT_CRIME.measure_id(basis, form)
        for basis in ("offense", "clearance")
        for form in ("absolute_total", "rate")
    } == {
        "V:offense:absolute_total",
        "V:offense:rate",
        "V:clearance:absolute_total",
        "V:clearance:rate",
    }


@pytest.mark.parametrize("product", ALL_PRODUCTS, ids=lambda item: item.product_id)
def test_every_product_shares_one_scope_and_window(product: FbiUcrProduct) -> None:
    """Covers: ETL-052 — ten products cannot drift apart in scope or window."""
    shared = {
        field: value
        for field, value in VIOLENT_CRIME_CONTRACT.items()
        if field not in {"product_id", "label", "offense_code"}
    }

    assert {
        field: value
        for field, value in dataclasses.asdict(product).items()
        if field in shared
    } == shared
    assert product.label.startswith("Summarized ")
    assert product.label.endswith(" offenses and clearances")
    assert product.observation_endpoint(FbiSubject("state", "WI")) == (
        f"/summarized/state/WI/{product.offense_code}"
    )
    assert product.measure_id("offense", "rate") == (
        f"{product.offense_code}:offense:rate"
    )


def test_reference_states_come_from_the_agency_scope_only() -> None:
    """Covers: ETL-052 — a state subject never requires an agency directory.

    A state observation is labelled from the registry's published state label,
    not from the Agency directory, so widening the state scope must not add a
    directory capture per state per product.
    """
    widened = dataclasses.replace(
        SUMMARIZED_VIOLENT_CRIME, state_scope=("MN", "PA", "VI", "WI")
    )
    states_only = dataclasses.replace(SUMMARIZED_VIOLENT_CRIME, agency_scope=())
    cross_state = dataclasses.replace(
        SUMMARIZED_VIOLENT_CRIME, state_scope=("WI",), agency_scope=("MN0270000",)
    )

    assert widened.reference_states == ("WI",)
    assert states_only.reference_states == ()
    assert cross_state.reference_states == ("MN",)


@pytest.mark.parametrize("product", ALL_PRODUCTS, ids=lambda item: item.product_id)
def test_every_product_covers_every_documented_state(product: FbiUcrProduct) -> None:
    """Covers: ETL-052 — national plus all 52 documented states, six agencies.

    Only two states carry captured fixtures (Wisconsin and Pennsylvania, plus
    the Virgin Islands territory); every other state is proved here, by its
    canonical code and its documented endpoint, rather than by 50 fixtures.
    """
    subjects = product.subjects
    states = [
        subject.subject_code for subject in subjects if subject.subject_type == "state"
    ]

    assert len(STATE_CODE_CONTRACT) == 52
    assert states == list(STATE_CODE_CONTRACT)
    assert not set(states) & UNSUPPORTED_STATE_CODES
    assert [subject.subject_type for subject in subjects].count("national") == 1
    assert [
        subject.subject_code for subject in subjects if subject.subject_type == "agency"
    ] == list(VIOLENT_CRIME_CONTRACT["agency_scope"])
    assert product.reference_states == ("WI",)
    for state in states:
        fips = canonical_state_fips(state)
        assert fips is not None and len(fips) == 2 and fips.isdigit()
        assert published_state_label(state)
        assert product.observation_endpoint(FbiSubject("state", state)) == (
            f"/summarized/state/{state}/{product.offense_code}"
        )
    assert canonical_state_fips("VI") == "78"
    assert len({canonical_state_fips(state) for state in states}) == 52
