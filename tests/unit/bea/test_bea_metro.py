"""Metro areas summed from BEA's counties, offline.

Covers: ETL-101
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.bea.registry import COMBINED_AREAS, get_table
from data_ingestion_toolbox.bea.silver_bea.metro import (
    RATIO_DERIVATION,
    SUM_DERIVATION,
    derive_metro_observations,
    metro_units,
)
from data_ingestion_toolbox.bea.silver_bea.parse import parse_table

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bea"


def _parsed(code: str):
    return parse_table((FIXTURES / f"{code}.zip").read_bytes(), table=get_table(code))


def _by_key(derived):
    return {(item.geo_id, item.line_code, item.year): item for item in derived}


def _value(parsed, code: str, line: str, year: int) -> Decimal:
    for item in (*parsed.observations, *parsed.combined):
        if (item.geo_source_code, item.line_code, item.year) == (code, line, year):
            return item.value
    raise KeyError(code)


def test_a_metro_is_the_sum_of_its_counties_and_per_capita_is_a_ratio() -> None:
    """Covers: ETL-101 — income and population add up; per capita is income over population."""
    parsed = _parsed("CAINC1")
    derived = _by_key(
        derive_metro_observations(
            get_table("CAINC1"),
            parsed.observations,
            parsed.combined,
            {"cbsa:99901": ["10001", "10003"], "cbsa:20100": ["10001"]},
            delineation_vintage=2023,
        )
    )
    income = derived[("cbsa:99901", "1", 2024)]
    population = derived[("cbsa:99901", "2", 2024)]
    assert income.value == _value(parsed, "10001", "1", 2024) + _value(
        parsed, "10003", "1", 2024
    )
    assert population.value == _value(parsed, "10001", "2", 2024) + _value(
        parsed, "10003", "2", 2024
    )
    assert income.derivation == SUM_DERIVATION and income.value_status == "valid"
    assert income.value_source == "sum of 2 member areas (2023 CBSA delineation)"
    per_capita = derived[("cbsa:99901", "3", 2024)]
    assert per_capita.derivation == RATIO_DERIVATION
    assert per_capita.value == (income.value * 1000 / population.value).quantize(
        Decimal(1)
    )
    # A one-county metro reproduces BEA's own county per capita figure.
    assert derived[("cbsa:20100", "3", 2024)].value == _value(
        parsed, "10001", "3", 2024
    )
    assert {item.year for item in derived.values()} == set(range(1969, 2025))


def test_a_combined_area_stands_for_its_counties_only_when_all_are_inside() -> None:
    """Covers: ETL-101 — Maui and Kalawao are one BEA area; a split one leaves no figure."""
    assert metro_units(["15009", "15005"]) == ("15901",)
    assert metro_units(["51003", "51540", "51065"]) == ("51065", "51901")
    assert metro_units(["51003", "51065"]) is None
    assert all(len(members) >= 2 for members in COMBINED_AREAS.values())
    parsed = _parsed("CAINC1")
    derived = _by_key(
        derive_metro_observations(
            get_table("CAINC1"),
            parsed.observations,
            parsed.combined,
            {"cbsa:27980": ["15009", "15005"], "cbsa:16820": ["51003", "51065"]},
            delineation_vintage=2023,
        )
    )
    assert derived[("cbsa:27980", "1", 2024)].value == _value(
        parsed, "15901", "1", 2024
    )
    split = derived[("cbsa:16820", "1", 2024)]
    assert (split.value, split.value_status) == (None, "not_available")
    assert "partly outside" in split.value_source
    assert derived[("cbsa:16820", "3", 2024)].value is None


def test_a_metro_missing_a_member_has_no_figure_rather_than_a_partial_sum() -> None:
    """Covers: ETL-101 — a member with no value makes the metro unavailable, never smaller."""
    parsed = _parsed("CAINC1")
    derived = _by_key(
        derive_metro_observations(
            get_table("CAINC1"),
            parsed.observations,
            parsed.combined,
            {"cbsa:41540": ["10005", "24045"]},
            delineation_vintage=2023,
        )
    )
    salisbury = derived[("cbsa:41540", "1", 2024)]
    assert (salisbury.value, salisbury.value_status) == (None, "not_available")
    assert salisbury.value_source == "1 of 2 member areas have no value"
    gdp = _parsed("CAGDP2")
    withheld = next(
        item for item in gdp.observations if item.value_status == "withheld"
    )
    sector = _by_key(
        derive_metro_observations(
            get_table("CAGDP2"),
            gdp.observations,
            gdp.combined,
            {"cbsa:99902": [withheld.geo_source_code]},
            delineation_vintage=None,
        )
    )[("cbsa:99902", withheld.line_code, withheld.year)]
    assert (sector.value, sector.value_status) == (None, "not_available")


def test_chained_dollars_and_price_levels_are_never_summed() -> None:
    """Covers: ETL-101 — real GDP is chained per area, so only current-dollar GDP is derived."""
    parsed = _parsed("CAGDP1")
    derived = derive_metro_observations(
        get_table("CAGDP1"),
        parsed.observations,
        parsed.combined,
        {"cbsa:20100": ["10001"]},
        delineation_vintage=2023,
    )
    assert {item.line_code for item in derived} == {"3"}
    parities = _parsed("MARPP")
    assert (
        derive_metro_observations(
            get_table("MARPP"),
            parities.observations,
            parities.combined,
            {"cbsa:20100": ["10001"]},
            delineation_vintage=2023,
        )
        == []
    )


def test_the_price_deflator_is_read_from_the_parities_zip() -> None:
    """Covers: ETL-101 — MAIRPD is its own table, published beside the metro parities."""
    table = get_table("MAIRPD")
    assert table.path == "/MARPP.zip" and table.lines == {"1": "price_index"}
    parsed = _parsed_from("MARPP", table)
    assert parsed.member == "MAIRPD_MSA_2008_2024.csv"
    assert str(parsed.release_date) == "2026-02-19"
    dover = {o.year: o.value for o in parsed.observations if o.geo_id == "cbsa:20100"}
    assert dover[2024] == Decimal("120.583")
    nation = {o.year: o.value for o in parsed.observations if o.geo_id == "us:1"}
    assert nation[2017] == Decimal("100.000")
    # The parities in the same zip still parse as their own table.
    assert _parsed("MARPP").member == "MARPP_MSA_2008_2024.csv"


def _parsed_from(archive: str, table):
    return parse_table((FIXTURES / f"{archive}.zip").read_bytes(), table=table)
