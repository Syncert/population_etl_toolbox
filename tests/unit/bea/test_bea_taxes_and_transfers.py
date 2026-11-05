"""Offline contracts for the BEA transfer receipts, personal taxes and
disposable income tables (taxes-and-public-money, T-1).

The fixtures hold BEA's own rows, read from the bulk files on 2026-11-05:
CAINC35 for the nation and Dane County, Wisconsin, and the nation's rows of
SAINC35, SAINC50 and SAINC51 from the one state zip, each with its three
newest year columns, its header and its footer.

Covers: ETL-115
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.bea.client import every_area_member
from data_ingestion_toolbox.bea.registry import ADDITIVE_BASES, TABLES, get_table
from data_ingestion_toolbox.bea.silver_bea.parse import parse_table
from data_ingestion_toolbox.quality.sources import BEA_COMPONENT_SUMS
from data_ingestion_toolbox.semantics.derived_ratios import load_registry

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bea"
DANE = "state:55|county:025"


def _parsed(code: str):
    table = get_table(code)
    payload = (FIXTURES / table.path.lstrip("/")).read_bytes()
    return parse_table(payload, table=table)


def _values(code: str, geo_id: str, year: int) -> dict[str, Decimal | None]:
    return {
        item.line_code: item.value
        for item in _parsed(code).observations
        if item.geo_id == geo_id and item.year == year
    }


def test_the_new_tables_are_registered_where_bea_publishes_them() -> None:
    """Covers: ETL-115 — CAINC35 is its own zip; the state tables share SAINC.zip."""
    assert get_table("CAINC35").path == "/CAINC35.zip"
    assert get_table("CAINC35").geography == "county"
    for code in ("SAINC35", "SAINC50", "SAINC51"):
        table = get_table(code)
        assert table.path == "/SAINC.zip"
        assert table.geography == "state"
        assert table.member_prefix == f"{code}__ALL_AREAS_"
    assert "46" in get_table("CAINC4").lines
    # Every new line is a current-dollar total or BEA's own per-person figure:
    # a county line adds into its metro, and nothing is a chained figure.
    for code in ("CAINC35", "SAINC35", "SAINC50"):
        assert set(get_table(code).lines.values()) <= ADDITIVE_BASES
    assert get_table("SAINC51").lines == {
        "51": "current_dollars",
        "53": "per_capita_current_dollars",
    }


def test_each_state_table_reads_its_own_member_of_the_shared_zip() -> None:
    """Covers: ETL-115 — one zip, three tables, never the wrong member."""
    payload = (FIXTURES / "SAINC.zip").read_bytes()
    members = {
        code: every_area_member(payload, "/SAINC.zip", get_table(code))[0]
        for code in ("SAINC35", "SAINC50", "SAINC51")
    }
    assert members == {
        "SAINC35": "SAINC35__ALL_AREAS_1929_2025.csv",
        "SAINC50": "SAINC50__ALL_AREAS_1948_2025.csv",
        "SAINC51": "SAINC51__ALL_AREAS_1948_2025.csv",
    }


def test_county_transfer_receipts_parse_by_program_with_their_release() -> None:
    """Covers: ETL-115 — Dane County's 2022 programs, as BEA published them."""
    parsed = _parsed("CAINC35")
    assert parsed.quarantined == ()
    assert parsed.release_date == date(2023, 11, 16)
    dane = _values("CAINC35", DANE, 2022)
    assert dane["1000"] == Decimal("4849762")
    assert dane["2110"] == Decimal("1974610")  # Social Security
    assert dane["2220"] == Decimal("718090")  # Medicaid and other public care
    assert dane["2330"] == Decimal("135220")  # SNAP
    assert set(dane) == set(get_table("CAINC35").lines)
    assert {item.unit for item in parsed.observations} == {"Thousands of dollars"}


def test_state_tables_load_taxes_and_after_tax_income_not_repeats() -> None:
    """Covers: ETL-115 — SAINC50's repeats of SAINC51 and CAINC1 are counted, not loaded."""
    taxes = _parsed("SAINC50")
    assert taxes.release_date == date(2026, 9, 30)
    nation = _values("SAINC50", "us:1", 2025)
    assert nation["60"] == Decimal("3341556000")
    assert nation["10"] == Decimal("26326993000")
    # Line 15 repeats line 60; 16, 20, 30 and 50 repeat SAINC51 and CAINC1.
    assert not {"15", "16", "20", "30", "50"} & set(nation)
    # The fixture carries three of the repeats (15, 16, 20) for the nation.
    assert taxes.out_of_scope_row_count == 3
    income = _values("SAINC51", "us:1", 2025)
    assert income == {"51": Decimal("22985437.0"), "53": Decimal("67251")}
    transfers = _values("SAINC35", "us:1", 2025)
    assert transfers["2221"] == Decimal("1023953000")  # Medicaid itself


@pytest.mark.parametrize(
    ("code", "geo_id", "year"),
    [
        ("CAINC35", DANE, 2020),
        ("CAINC35", DANE, 2022),
        ("CAINC35", "us:1", 2021),
        ("SAINC35", "us:1", 2025),
        ("SAINC50", "us:1", 2024),
    ],
)
def test_the_component_sums_the_quality_rule_checks_hold_on_bea_rows(
    code: str, geo_id: str, year: int
) -> None:
    """Covers: ETL-115 — DQ-BEA-006's sums are BEA's, proved on its published rows."""
    values = _values(code, geo_id, year)
    checked = 0
    for table, total_line, parts in BEA_COMPONENT_SUMS:
        if table != code:
            continue
        total = values[total_line]
        assert total is not None
        assert abs(sum(values[line] for line in parts) - total) <= max(
            Decimal("0.005") * abs(total), Decimal(3)
        )
        checked += 1
    assert checked >= 1


def test_refundable_credits_overlap_the_programs_and_are_in_no_sum() -> None:
    """Covers: ETL-115 — the EITC is a program and a credit; adding 5000 double-counts it."""
    for _table, total_line, parts in BEA_COMPONENT_SUMS:
        assert "5000" not in parts and total_line != "5000"
    nation = _values("SAINC35", "us:1", 2025)
    assert nation["2000"] + nation["3000"] + nation["4000"] == nation["1000"]
    assert (
        nation["2000"] + nation["3000"] + nation["4000"] + nation["5000"]
        != (nation["1000"])
    )


def test_each_rate_divides_by_personal_income_of_the_same_release() -> None:
    """Covers: ETL-115 — state rates use SAINC50's own income; county rates CAINC1's."""
    pairings = {
        pairing.ratio_id: pairing
        for pairing in load_registry().values()
        if pairing.numerator.split(":")[1] in {"CAINC35", "SAINC35", "SAINC50"}
    }
    assert len(pairings) == 22
    for pairing in pairings.values():
        assert pairing.kind == "share" and pairing.scale == 100
        assert pairing.status == "approved"
        table = pairing.numerator.split(":")[1]
        expected = "BEA:CAINC1:1" if table == "CAINC35" else "BEA:SAINC50:10"
        assert pairing.denominator == expected, pairing.ratio_id
        line = pairing.numerator.split(":")[2]
        assert line in get_table(table).lines
    # The state income denominator is loaded from the same table and file.
    assert "10" in get_table("SAINC50").lines
    taxes = load_registry()["personal_taxes_share_of_income"]
    assert taxes.numerator == "BEA:SAINC50:60"


def test_the_weekly_run_captures_every_registered_table_with_no_conf() -> None:
    """Covers: ETL-115 — the loader rule: a newly registered table needs no history run.

    Each BEA zip holds every year of its table, and the scheduled run plans
    every registered table, so the first Wednesday run after this merges
    loads CAINC35 from 1969 and the state tables from 1929 or 1948.
    """
    planned = [table.code for table in TABLES]
    for code in ("CAINC35", "SAINC35", "SAINC50", "SAINC51"):
        assert code in planned
    header_years = [
        item.year
        for item in _parsed("CAINC35").observations
        if item.line_code == "1000"
    ]
    assert sorted(set(header_years)) == [2020, 2021, 2022]
