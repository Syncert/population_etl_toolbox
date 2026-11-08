"""NDCP county childcare prices: the registry, the transport check and the offline parser.

Covers: ETL-123
"""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.dol_childcare.client import (
    ChildcarePayloadError,
    check_workbook,
)
from data_ingestion_toolbox.dol_childcare.registry import (
    MEASURES,
    RELEASES,
    UNIT,
    get_release,
    measure_for,
)
from data_ingestion_toolbox.dol_childcare.silver_dol_childcare import load
from data_ingestion_toolbox.dol_childcare.silver_dol_childcare.parse import (
    _combined,
    parse_workbook,
)
from tests.support.dol_childcare import build_workbook as _workbook
from tests.support.dol_childcare import header as _header
from tests.support.dol_childcare import row as _row

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests" / "fixtures" / "dol_childcare" / "NDCP2022.xlsx"
GOLD = (
    ROOT
    / "src/data_ingestion_toolbox/dol_childcare/gold_dol_childcare/DDL/gold_dol_childcare.sql"
)
RELEASE = RELEASES[0]


def test_one_release_is_registered_and_it_restates_the_earlier_one() -> None:
    """Covers: ETL-123 — NDCP2022 holds 2008-2022, so the 2008-2018 file is not loaded twice."""
    assert [release.key for release in RELEASES] == ["ndcp2022"]
    assert RELEASE.path == "/NDCP2022.xlsx"
    assert list(RELEASE.years) == list(range(2008, 2023))
    assert get_release("ndcp2022") is RELEASE
    with pytest.raises(ValueError, match="no NDCP release"):
        get_release("ndcp2018")


def test_eight_prices_cover_two_care_types_and_four_age_groups() -> None:
    """Covers: ETL-123 — the median weekly price of each care type and age group, with its bands."""
    assert {m.metric_key for m in MEASURES} == {
        f"{care}_{age}"
        for care in ("center", "home")
        for age in ("infant", "toddler", "preschool", "school_age")
    }
    infant = measure_for("center_infant")
    assert infant.column == "MCINFANT"
    assert infant.imputation_columns == (
        "iMCBto5",
        "iMC6to11",
        "iMC12to17",
        "iMC18to23",
    )
    assert measure_for("home_school_age").imputation_columns == ("iMFCCSA",)
    assert measure_for("home_preschool").column == "MFCCPRESCHOOL"


def test_the_gold_measure_definitions_repeat_the_registry() -> None:
    """Covers: ETL-123 — the served labels and columns are the registry's, word for word."""
    sql = GOLD.read_text()
    rows = re.findall(
        r"\('([a-z_]+)', '(center|home)', '([a-z_]+)', '([A-Z]+)', '([^']+)', '([^']+)'\)",
        sql,
    )
    assert rows == [
        (m.metric_key, m.care_type, m.age_group, m.column, m.label, UNIT)
        for m in MEASURES
    ]


def test_a_body_that_is_not_a_workbook_is_refused_before_it_is_kept() -> None:
    """Covers: ETL-123 — an HTML page answered for the workbook is never captured."""
    check_workbook(b"PK\x03\x04rest", "/NDCP2022.xlsx")
    with pytest.raises(ChildcarePayloadError, match="not_a_workbook"):
        check_workbook(b"<!doctype html><html>", "/NDCP2022.xlsx")


def test_the_fixture_workbook_reads_dane_county_as_published() -> None:
    """Covers: ETL-123 — Dane County's 2022 and 2021 prices, the 2021 ones interpolated."""
    parsed = parse_workbook(FIXTURE.read_bytes(), release=RELEASE)
    # Five county-years, the release's wholly blank row not counted, one bad row.
    assert parsed.row_count == 6
    assert len(parsed.prices) == 5 * len(MEASURES)
    dane = {
        (p.study_year, p.metric_key): p for p in parsed.prices if p.fips_code == "55025"
    }
    assert dane[(2022, "center_infant")].value == Decimal(293)
    assert dane[(2022, "center_toddler")].value == Decimal("258.75")
    assert dane[(2022, "home_infant")].value == Decimal(220)
    assert dane[(2022, "center_infant")].geo_id == "state:55|county:025"
    assert dane[(2022, "center_infant")].county_name == "Dane County"
    assert not dane[(2022, "center_infant")].imputed
    assert dane[(2022, "center_infant")].imputation_code == "00000"
    interpolated = dane[(2021, "center_infant")]
    assert interpolated.value == Decimal("299.75")
    assert interpolated.imputed and interpolated.imputation_code == "00001"


def test_a_blank_price_is_missing_never_zero() -> None:
    """Covers: ETL-123 — Rock County's blank 2022 school-age home price is provider_missing."""
    parsed = parse_workbook(FIXTURE.read_bytes(), release=RELEASE)
    (blank,) = [p for p in parsed.prices if p.value_status != "valid"]
    assert (blank.fips_code, blank.study_year, blank.metric_key) == (
        "55105",
        2022,
        "home_school_age",
    )
    assert blank.value is None and blank.missing_reason == "provider_missing"
    assert blank.value_source == ""


def test_a_row_that_cannot_be_read_is_set_aside_with_its_reason() -> None:
    """Covers: ETL-123 — the fixture's unreadable county code quarantines its row only."""
    parsed = parse_workbook(FIXTURE.read_bytes(), release=RELEASE)
    assert [(q.error_code, q.source_row_index) for q in parsed.quarantined] == [
        ("unreadable_row", 8)
    ]
    assert not parsed.rejected


@pytest.mark.parametrize(
    ("values", "code"),
    [
        ({"STUDYYEAR": 2023}, "unregistered_year"),
        ({"COUNTY_FIPS_CODE": 55000}, "unreadable_row"),
        ({"MCINFANT": "n/a"}, "unreadable_value"),
        ({"MCINFANT": 0}, "implausible_value"),
        ({"iMCBto5": "0001"}, "unreadable_row"),
    ],
)
def test_each_unreadable_value_names_its_reason(
    values: dict[str, object], code: str
) -> None:
    """Covers: ETL-123 — a year outside the release, a state total, text or zero price, a short code."""
    header = _header()
    parsed = parse_workbook(
        _workbook([header, _row(header, **values)]), release=RELEASE
    )
    assert parsed.prices == ()
    assert [q.error_code for q in parsed.quarantined] == [code]


def test_a_county_and_year_given_twice_keeps_the_first() -> None:
    """Covers: ETL-123 — a repeated county and study year is set aside, not stored twice."""
    header = _header()
    parsed = parse_workbook(
        _workbook([header, _row(header), _row(header, MCINFANT=300)]), release=RELEASE
    )
    assert len(parsed.prices) == len(MEASURES)
    assert [q.error_code for q in parsed.quarantined] == ["duplicate_row"]


def test_an_aggregate_is_imputed_when_any_band_is() -> None:
    """Covers: ETL-123 — each imputation position is set when any band of the age group set it."""
    assert _combined(["00000", "00010", "00001"]) == "00011"
    assert _combined([]) is None
    header = _header()
    parsed = parse_workbook(
        _workbook([header, _row(header, iMC12to17="10000", iMCSA=11)]), release=RELEASE
    )
    by_key = {p.metric_key: p for p in parsed.prices}
    assert by_key["center_infant"].imputed
    assert by_key["center_infant"].imputation_code == "10000"
    # The 2008-2018 release stores the code as a number: 11 is 00011.
    assert by_key["center_school_age"].imputation_code == "00011"
    assert not by_key["center_toddler"].imputed


def test_a_sheet_without_the_registered_columns_is_refused_whole() -> None:
    """Covers: ETL-123 — a changed layout is quarantined as row 0, never guessed."""
    header = [c for c in _header() if c != "MCSA"]
    parsed = parse_workbook(_workbook([header, _row(header)]), release=RELEASE)
    assert parsed.rejected and parsed.prices == ()
    assert "MCSA" in parsed.quarantined[0].error_summary
    other = parse_workbook(_workbook([_header()], sheet="Sheet1"), release=RELEASE)
    assert [q.error_code for q in other.quarantined] == ["sheet_missing"]


def test_a_repeated_price_is_compared_by_value_status_and_imputation() -> None:
    """Covers: ETL-123 — CHURN RULE: only a changed number, status or code is stored again."""
    sql = " ".join(load._STORE_CHANGED_PRICES_SQL.split())
    assert "newest.value IS NOT DISTINCT FROM incoming.value" in sql
    assert "newest.value_status = incoming.value_status" in sql
    assert "newest.imputation_code IS NOT DISTINCT FROM incoming.imputation_code" in sql
    assert "stored.study_year = incoming.study_year" in sql
