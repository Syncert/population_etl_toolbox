"""ETL unit tests: FRED duration normalization.

Covers ETL-018 (compute_fred_duration returns exact inclusive date ranges
for daily, weekly, biweekly, monthly, quarterly, semiannual, and annual
frequency inputs).
"""

import pytest
from datetime import date, timedelta

from data_ingestion_toolbox.fred.silver_fred.time_utils import (
    compute_fred_duration,
)


@pytest.mark.unit
class TestFredDailyDuration:
    """ETL-018: daily observations span a single day."""

    def test_daily_code(self) -> None:
        obs = date(2023, 6, 15)
        start, end = compute_fred_duration(obs, "D")
        assert start == end == obs

    def test_daily_long_name(self) -> None:
        obs = date(2023, 6, 15)
        start, end = compute_fred_duration(obs, "Daily")
        assert start == end == obs


@pytest.mark.unit
class TestFredWeeklyDuration:
    """ETL-018: weekly observations span seven days ending on the obs date."""

    def test_weekly_code(self) -> None:
        obs = date(2023, 6, 15)
        start, end = compute_fred_duration(obs, "W")
        assert end == obs
        assert start == obs - timedelta(days=6)

    def test_weekly_long_name(self) -> None:
        obs = date(2023, 6, 15)
        start, end = compute_fred_duration(obs, "Weekly")
        assert end == obs
        assert start == obs - timedelta(days=6)


@pytest.mark.unit
class TestFredBiweeklyDuration:
    """ETL-018: biweekly observations span fourteen days."""

    def test_biweekly_code(self) -> None:
        obs = date(2023, 6, 15)
        start, end = compute_fred_duration(obs, "BW")
        assert end == obs
        assert start == obs - timedelta(days=13)

    def test_biweekly_long_name(self) -> None:
        obs = date(2023, 6, 15)
        start, end = compute_fred_duration(obs, "Biweekly")
        assert end == obs
        assert start == obs - timedelta(days=13)


@pytest.mark.unit
class TestFredMonthlyDuration:
    """ETL-018: monthly observations span the calendar month of the obs date."""

    def test_january(self) -> None:
        obs = date(2023, 1, 1)
        start, end = compute_fred_duration(obs, "M")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 1, 31)

    def test_february_non_leap_year(self) -> None:
        obs = date(2023, 2, 1)
        start, end = compute_fred_duration(obs, "M")
        assert start == date(2023, 2, 1)
        assert end == date(2023, 2, 28)

    def test_february_leap_year(self) -> None:
        obs = date(2024, 2, 1)
        start, end = compute_fred_duration(obs, "M")
        assert end == date(2024, 2, 29)

    def test_december(self) -> None:
        obs = date(2023, 12, 1)
        start, end = compute_fred_duration(obs, "M")
        assert start == date(2023, 12, 1)
        assert end == date(2023, 12, 31)

    def test_monthly_long_name(self) -> None:
        obs = date(2023, 6, 1)
        start, end = compute_fred_duration(obs, "Monthly")
        assert start == date(2023, 6, 1)
        assert end == date(2023, 6, 30)


@pytest.mark.unit
class TestFredQuarterlyDuration:
    """ETL-018: quarterly observations span the calendar quarter."""

    def test_q1(self) -> None:
        obs = date(2023, 1, 1)
        start, end = compute_fred_duration(obs, "Q")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 3, 31)

    def test_q2(self) -> None:
        obs = date(2023, 4, 1)
        start, end = compute_fred_duration(obs, "Q")
        assert start == date(2023, 4, 1)
        assert end == date(2023, 6, 30)

    def test_q3(self) -> None:
        obs = date(2023, 7, 1)
        start, end = compute_fred_duration(obs, "Q")
        assert start == date(2023, 7, 1)
        assert end == date(2023, 9, 30)

    def test_q4(self) -> None:
        obs = date(2023, 10, 1)
        start, end = compute_fred_duration(obs, "Q")
        assert start == date(2023, 10, 1)
        assert end == date(2023, 12, 31)

    def test_quarterly_long_name(self) -> None:
        obs = date(2023, 4, 1)
        start, end = compute_fred_duration(obs, "Quarterly")
        assert start == date(2023, 4, 1)
        assert end == date(2023, 6, 30)


@pytest.mark.unit
class TestFredSemiannualDuration:
    """ETL-018: semiannual observations span H1 or H2."""

    def test_h1_jan_through_jun(self) -> None:
        obs = date(2023, 1, 1)
        start, end = compute_fred_duration(obs, "SA")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 6, 30)

    def test_h2_jul_through_dec(self) -> None:
        obs = date(2023, 7, 1)
        start, end = compute_fred_duration(obs, "SA")
        assert start == date(2023, 7, 1)
        assert end == date(2023, 12, 31)

    def test_semiannual_long_name(self) -> None:
        obs = date(2023, 1, 1)
        start, end = compute_fred_duration(obs, "Semiannual")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 6, 30)


@pytest.mark.unit
class TestFredAnnualDuration:
    """ETL-018: annual observations span the full calendar year."""

    def test_annual_code(self) -> None:
        obs = date(2023, 1, 1)
        start, end = compute_fred_duration(obs, "A")
        assert start == date(2023, 1, 1)
        assert end == date(2023, 12, 31)

    def test_annual_long_name(self) -> None:
        obs = date(2022, 1, 1)
        start, end = compute_fred_duration(obs, "Annual")
        assert start == date(2022, 1, 1)
        assert end == date(2022, 12, 31)


@pytest.mark.unit
class TestFredUnknownFrequency:
    """ETL-018: unknown frequencies fall back to daily (logged as warning)."""

    def test_none_frequency_falls_back_to_daily(self) -> None:
        obs = date(2023, 5, 1)
        start, end = compute_fred_duration(obs, None)
        assert start == end == obs

    def test_empty_string_falls_back_to_daily(self) -> None:
        obs = date(2023, 5, 1)
        start, end = compute_fred_duration(obs, "")
        assert start == end == obs
