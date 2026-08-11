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
    """Covers: ETL-007 — ACS duration windows match each dataset."""

    def test_acs1_spans_single_calendar_year(self) -> None:
        """Covers: ETL-007 — ACS1 spans its estimate calendar year."""
        start, end = compute_acs_duration("acs1", 2022)
        assert start == date(2022, 1, 1)
        assert end == date(2022, 12, 31)

    def test_acs5_start_is_estimate_year_minus_4(self) -> None:
        """Covers: ETL-007 — ACS5 starts four years before its estimate."""
        start, end = compute_acs_duration("acs5", 2022)
        assert start == date(2018, 1, 1)
        assert end == date(2022, 12, 31)

    def test_acs5_spans_five_years(self) -> None:
        """Covers: ETL-007 — ACS5 ends in its estimate year."""
        start, end = compute_acs_duration("acs5", 2024)
        assert start == date(2020, 1, 1)
        assert end == date(2024, 12, 31)

    def test_acs1_case_insensitive(self) -> None:
        """Covers: ETL-007, ETL-031 — ACS1 matching is case-insensitive."""
        start, end = compute_acs_duration("ACS1", 2021)
        assert start == date(2021, 1, 1)
        assert end == date(2021, 12, 31)

    def test_acs5_case_insensitive(self) -> None:
        """Covers: ETL-007, ETL-031 — ACS5 matching is case-insensitive."""
        start, end = compute_acs_duration("ACS5", 2020)
        assert start == date(2016, 1, 1)
        assert end == date(2020, 12, 31)

    def test_unknown_dataset_defaults_to_one_year_window(self) -> None:
        """Covers: ETL-031 — unknown ACS datasets use a one-year fallback."""
        start, end = compute_acs_duration("acs3", 2015)
        assert start == date(2015, 1, 1)
        assert end == date(2015, 12, 31)

    def test_empty_dataset_defaults_to_one_year_window(self) -> None:
        """Covers: ETL-031 — an empty ACS dataset uses a one-year fallback."""
        start, end = compute_acs_duration("", 2019)
        assert start == date(2019, 1, 1)
        assert end == date(2019, 12, 31)
