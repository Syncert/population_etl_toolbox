"""ETL unit tests: BLS period normalization.

Covers ETL-013 (parse_bls_period_to_date returns exact period date and
duration boundaries for M01-M12, Q01-Q04, S01-S02, A01, and leap-year
February).
"""

import pytest
from datetime import date

from data_ingestion_toolbox.bls.silver_bls.time_utils import (
    parse_bls_period_to_date,
)


@pytest.mark.unit
class TestBlsMonthlyPeriods:
    """ETL-013: monthly periods produce correct first/last-day boundaries."""

    def test_january(self) -> None:
        period_date, start, end = parse_bls_period_to_date(2023, "M01")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 1, 31)
        assert period_date == end

    def test_december(self) -> None:
        period_date, start, end = parse_bls_period_to_date(2023, "M12")
        assert start == date(2023, 12, 1)
        assert end == date(2023, 12, 31)

    def test_february_non_leap_year(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "M02")
        assert start == date(2023, 2, 1)
        assert end == date(2023, 2, 28)

    def test_february_leap_year(self) -> None:
        """ETL-013: leap-year February ends on the 29th."""
        _, start, end = parse_bls_period_to_date(2024, "M02")
        assert start == date(2024, 2, 1)
        assert end == date(2024, 2, 29)

    def test_april_has_30_days(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "M04")
        assert end == date(2023, 4, 30)

    def test_lowercase_period_code(self) -> None:
        period_date, start, end = parse_bls_period_to_date(2023, "m06")
        assert start == date(2023, 6, 1)
        assert end == date(2023, 6, 30)


@pytest.mark.unit
class TestBlsQuarterlyPeriods:
    """ETL-013: quarterly periods span their three-month window."""

    def test_q1_jan_through_mar(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "Q01")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 3, 31)

    def test_q2_apr_through_jun(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "Q02")
        assert start == date(2023, 4, 1)
        assert end == date(2023, 6, 30)

    def test_q3_jul_through_sep(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "Q03")
        assert start == date(2023, 7, 1)
        assert end == date(2023, 9, 30)

    def test_q4_oct_through_dec(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "Q04")
        assert start == date(2023, 10, 1)
        assert end == date(2023, 12, 31)


@pytest.mark.unit
class TestBlsSemiannualPeriods:
    """ETL-013: semiannual periods S01 and S02."""

    def test_s01_jan_through_jun(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "S01")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 6, 30)

    def test_s02_jul_through_dec(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "S02")
        assert start == date(2023, 7, 1)
        assert end == date(2023, 12, 31)


@pytest.mark.unit
class TestBlsAnnualPeriod:
    """ETL-013: annual period A01 spans the full calendar year."""

    def test_a01_full_year(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "A01")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 12, 31)


@pytest.mark.unit
class TestBlsUnknownPeriod:
    """ETL-013: unknown period codes default to annual without raising."""

    def test_empty_period_defaults_to_annual(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 12, 31)

    def test_none_period_defaults_to_annual(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, None)  # type: ignore[arg-type]
        assert start == date(2023, 1, 1)
        assert end == date(2023, 12, 31)

    def test_unrecognized_code_defaults_to_annual(self) -> None:
        _, start, end = parse_bls_period_to_date(2023, "X99")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 12, 31)
