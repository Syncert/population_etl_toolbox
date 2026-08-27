"""USDA NASS classification, geography, and period identity contracts."""

from __future__ import annotations

from datetime import date

import pytest

from data_ingestion_toolbox.usda_nass.registry import get_product
from data_ingestion_toolbox.usda_nass.silver_nass.dimensions import (
    GEO_TYPE_BY_AGG_LEVEL,
    NassIdentityError,
    commodity_identity,
    domain_identity,
    geography_identity,
    period_identity,
    source_record_id,
    statistic_identity,
)

from ._doubles import load_fixture

pytestmark = pytest.mark.unit

PRODUCT = get_product("corn_survey_annual")
CENSUS_PRODUCT = get_product("corn_census_county")
CORN = load_fixture("corn_survey_annual")
CENSUS = load_fixture("corn_census_county")
BOUNDARY = load_fixture("boundary_records")["records"]


def _row(document: dict, level: str, index: int = 0) -> dict:
    return document["slices"][level]["data"]["data"][index]


def test_commodity_identity_uses_the_complete_classification() -> None:
    """Covers: ETL-024 — commodity identity is never reduced to one field."""
    planted = _row(CORN, "NATIONAL", 0)
    harvested = _row(CORN, "NATIONAL", 1)

    assert planted["commodity_desc"] == harvested["commodity_desc"] == "CORN"
    assert planted["class_desc"] != harvested["class_desc"]

    first = commodity_identity(planted)
    second = commodity_identity(harvested)
    assert first.commodity_sk != second.commodity_sk
    assert first.sector_desc == "CROPS"
    assert first.group_desc == "FIELD CROPS"
    assert first.prodn_practice_desc == "ALL PRODUCTION PRACTICES"
    assert first.util_practice_desc == "ALL UTILIZATION PRACTICES"
    assert commodity_identity(dict(planted)).commodity_sk == first.commodity_sk


def test_commodity_identity_requires_a_commodity() -> None:
    """Covers: RES-002 — a record without a commodity cannot get an identity."""
    with pytest.raises(NassIdentityError, match="no commodity_desc"):
        commodity_identity({**_row(CORN, "NATIONAL"), "commodity_desc": ""})


def test_statistic_identity_carries_registered_source_semantics() -> None:
    """Covers: ETL-024 — statistic identity carries declared source semantics."""
    yield_row = next(
        row
        for row in CORN["slices"]["NATIONAL"]["data"]["data"]
        if row["statisticcat_desc"] == "YIELD"
    )
    identity = statistic_identity(yield_row, PRODUCT)

    assert identity.statisticcat_desc == "YIELD"
    assert identity.unit_desc == "BU / ACRE"
    assert identity.short_desc.endswith("YIELD, MEASURED IN BU / ACRE")
    assert identity.value_kind == "rate"
    assert identity.additive_behavior == "non_additive"
    assert identity.additive_behavior_known is True
    assert identity.source_desc == "SURVEY"


def test_unregistered_statistics_and_units_cannot_get_an_identity() -> None:
    """Covers: RES-002 — unregistered statistics and units are refused."""
    with pytest.raises(NassIdentityError, match="is not registered for product"):
        statistic_identity(BOUNDARY["unregistered_statistic"], PRODUCT)
    with pytest.raises(NassIdentityError, match="is not registered for statistic"):
        statistic_identity(BOUNDARY["unregistered_unit"], PRODUCT)


def test_survey_and_census_statistics_never_share_an_identity() -> None:
    """Covers: ETL-024 — survey and census values remain separate."""
    survey = next(
        row
        for row in CORN["slices"]["STATE"]["data"]["data"]
        if row["statisticcat_desc"] == "AREA HARVESTED"
    )
    census = next(
        row
        for row in CENSUS["slices"]["STATE"]["data"]["data"]
        if row["statisticcat_desc"] == "AREA HARVESTED"
    )
    assert survey["short_desc"] == census["short_desc"]
    assert survey["unit_desc"] == census["unit_desc"]

    survey_identity = statistic_identity(survey, PRODUCT)
    census_identity = statistic_identity(census, CENSUS_PRODUCT)
    assert survey_identity.statistic_sk != census_identity.statistic_sk
    assert survey_identity.source_desc == "SURVEY"
    assert census_identity.source_desc == "CENSUS"
    assert census_identity.calculation_basis == "census_of_agriculture_enumeration"


def test_domain_total_is_an_explicit_source_member() -> None:
    """Covers: ETL-024 — a TOTAL domain category is an explicit member."""
    identity = domain_identity(_row(CORN, "COUNTY"))
    assert identity.domain_desc == "TOTAL"
    assert identity.domaincat_desc == "NOT SPECIFIED"
    assert (
        identity.domain_sk
        != domain_identity(
            {
                **_row(CORN, "COUNTY"),
                "domaincat_desc": "AREA OPERATED: (1.0 TO 9.9 ACRES)",
            }
        ).domain_sk
    )


def test_domain_identity_requires_a_domain() -> None:
    """Covers: RES-002 — a record without a domain cannot get an identity."""
    with pytest.raises(NassIdentityError, match="no domain_desc"):
        domain_identity({**_row(CORN, "COUNTY"), "domain_desc": ""})


@pytest.mark.parametrize(
    ("level", "geo_type", "geo_id", "geo_source_code"),
    [
        ("NATIONAL", "nation", "us:1", "US"),
        ("STATE", "state", "state:01", "01"),
        ("COUNTY", "county", "state:01|county:001", "01001"),
    ],
)
def test_geography_resolves_only_from_exact_provider_codes(
    level: str, geo_type: str, geo_id: str, geo_source_code: str
) -> None:
    """Covers: ETL-024 — geography resolves from exact codes, never names."""
    identity = geography_identity(_row(CORN, level))
    assert identity.geo_type == geo_type
    assert identity.geo_id == geo_id
    assert identity.geo_source_code == geo_source_code
    assert GEO_TYPE_BY_AGG_LEVEL[level] == geo_type


def test_geography_falls_back_to_the_alternate_provider_code_field() -> None:
    """Covers: ETL-024 — the alternate exact code field is honoured."""
    row = dict(_row(CORN, "COUNTY"))
    row["state_fips_code"] = ""
    row["county_ansi"] = ""
    identity = geography_identity(row)
    assert identity.geo_id == "state:01|county:001"
    assert identity.state_fips == "01"
    assert identity.county_fips == "001"


def test_a_county_without_an_exact_code_is_refused_not_name_matched() -> None:
    """Covers: ETL-024 — a county without an exact code is never name-matched."""
    with pytest.raises(NassIdentityError, match="no exact county"):
        geography_identity(BOUNDARY["county_without_exact_code"])

    stateless = dict(_row(CORN, "STATE"))
    stateless["state_ansi"] = ""
    stateless["state_fips_code"] = ""
    with pytest.raises(NassIdentityError, match="no exact state"):
        geography_identity(stateless)


def test_unsupported_levels_keep_their_evidence_without_an_identity() -> None:
    """Covers: ETL-024 — unsupported levels keep evidence, not a geo_id."""
    identity = geography_identity(BOUNDARY["unsupported_aggregate_level"])
    assert identity.geo_type == "unsupported"
    assert identity.geo_id is None
    assert identity.asd_code == "40"
    assert identity.geo_source_code == "ALABAMA, SOUTHEAST"

    watershed = geography_identity(BOUNDARY["unsupported_watershed_level"])
    assert watershed.geo_type == "unsupported"
    assert watershed.watershed_code == "03150201"


def test_period_normalization_preserves_every_source_field() -> None:
    """Covers: ETL-025 — period normalization preserves every source field."""
    identity = period_identity(_row(CORN, "NATIONAL"))
    assert identity.year == 2024
    assert identity.freq_desc == "ANNUAL"
    assert identity.begin_code == "00"
    assert identity.end_code == "00"
    assert identity.reference_period_desc == "YEAR"
    assert identity.week_ending is None

    weekly = {**_row(CORN, "NATIONAL"), "week_ending": "2024-09-15"}
    assert period_identity(weekly).week_ending == date(2024, 9, 15)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("year", "2024/25", "unparseable year"),
        ("year", "1500", "outside the warehouse range"),
        ("freq_desc", "", "no freq_desc"),
        ("week_ending", "09/15/2024", "unparseable week_ending"),
    ],
)
def test_unusable_period_fields_are_explicit_failures(
    field: str, value: str, message: str
) -> None:
    """Covers: RES-002 — an unusable period field is an explicit failure."""
    with pytest.raises(NassIdentityError, match=message):
        period_identity({**_row(CORN, "NATIONAL"), field: value})


def test_source_record_identity_covers_the_complete_quick_stats_grain() -> None:
    """Covers: ETL-026 — source identity spans the complete source grain."""
    row = _row(CORN, "COUNTY")
    baseline = source_record_id(row)

    assert source_record_id(dict(row)) == baseline
    for field in (
        "short_desc",
        "class_desc",
        "domaincat_desc",
        "county_ansi",
        "year",
        "reference_period_desc",
        "agg_level_desc",
    ):
        assert source_record_id({**row, field: "CHANGED"}) != baseline, field
    # Value and load_time are revision attributes, not grain attributes.
    assert source_record_id({**row, "Value": "1"}) == baseline
    assert source_record_id({**row, "load_time": "2030-01-01 00:00:00"}) == baseline
