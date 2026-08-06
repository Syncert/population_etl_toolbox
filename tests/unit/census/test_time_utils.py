"""ETL unit tests: Census ACS time utilities.

Covers ETL-007 (ACS duration — ACS1 spans one calendar year; ACS5 spans
estimate year minus four through estimate year).
"""

import pytest
from datetime import date

from data_ingestion_toolbox.census_acs.silver_census.time_utils import (
    compute_acs_duration,
)


@pytest.mark.unit
class TestAcsDuration:
    """ETL-007: ACS duration windows are correct for acs1 and acs5."""

    def test_acs1_spans_single_calendar_year(self) -> None:
        start, end = compute_acs_duration("acs1", 2022)
        assert start == date(2022, 1, 1)
        assert end == date(2022, 12, 31)

    def test_acs5_start_is_estimate_year_minus_4(self) -> None:
        start, end = compute_acs_duration("acs5", 2022)
        assert start == date(2018, 1, 1)
        assert end == date(2022, 12, 31)

    def test_acs5_spans_five_years(self) -> None:
        start, end = compute_acs_duration("acs5", 2024)
        assert start == date(2020, 1, 1)
        assert end == date(2024, 12, 31)

    def test_acs1_case_insensitive(self) -> None:
        start, end = compute_acs_duration("ACS1", 2021)
        assert start == date(2021, 1, 1)
        assert end == date(2021, 12, 31)

    def test_acs5_case_insensitive(self) -> None:
        start, end = compute_acs_duration("ACS5", 2020)
        assert start == date(2016, 1, 1)
        assert end == date(2020, 12, 31)

    def test_unknown_dataset_defaults_to_one_year_window(self) -> None:
        """Unknown datasets fall back to a single-year window (logged warning)."""
        start, end = compute_acs_duration("acs3", 2015)
        assert start == date(2015, 1, 1)
        assert end == date(2015, 12, 31)

    def test_empty_dataset_defaults_to_one_year_window(self) -> None:
        start, end = compute_acs_duration("", 2019)
        assert start == date(2019, 1, 1)
        assert end == date(2019, 12, 31)
