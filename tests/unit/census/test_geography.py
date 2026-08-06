"""ETL unit tests: Census geography mapping.

Covers ETL-001 (geography API parameters), ETL-002 (canonical geo IDs),
ETL-003 (invalid geography returns None).
"""

import pytest

from data_ingestion_toolbox.census_acs.silver_census.geography_mapper import (
    map_census_geography,
)


@pytest.mark.unit
class TestCensusGeoId:
    """ETL-002: canonical geo_id format for each supported level."""

    def test_us_maps_to_us_colon_1(self) -> None:
        assert map_census_geography("us", None, None) == "us:1"

    def test_us_level_case_insensitive(self) -> None:
        assert map_census_geography("US", None, None) == "us:1"

    def test_state_maps_to_state_colon_fips(self) -> None:
        assert map_census_geography("state", "06", None) == "state:06"

    def test_state_preserves_zero_padding(self) -> None:
        assert map_census_geography("state", "01", None) == "state:01"

    def test_county_maps_to_state_pipe_county(self) -> None:
        assert map_census_geography("county", "06", "001") == "state:06|county:001"

    def test_county_preserves_three_digit_fips(self) -> None:
        assert map_census_geography("county", "55", "025") == "state:55|county:025"


@pytest.mark.unit
class TestCensusInvalidGeography:
    """ETL-003: invalid inputs return None rather than producing a bogus key."""

    def test_state_missing_fips_returns_none(self) -> None:
        assert map_census_geography("state", None, None) is None

    def test_state_empty_fips_returns_none(self) -> None:
        assert map_census_geography("state", "", None) is None

    def test_state_non_numeric_fips_returns_none(self) -> None:
        assert map_census_geography("state", "XX", None) is None

    def test_state_single_digit_fips_returns_none(self) -> None:
        # FIPS must be exactly two digits
        assert map_census_geography("state", "6", None) is None

    def test_county_missing_state_fips_returns_none(self) -> None:
        assert map_census_geography("county", None, "001") is None

    def test_county_missing_county_fips_returns_none(self) -> None:
        assert map_census_geography("county", "06", None) is None

    def test_county_short_county_fips_returns_none(self) -> None:
        # County FIPS must be exactly three digits
        assert map_census_geography("county", "06", "01") is None

    def test_county_non_numeric_county_fips_returns_none(self) -> None:
        assert map_census_geography("county", "06", "XYZ") is None

    def test_unsupported_level_returns_none(self) -> None:
        assert map_census_geography("tract", "06", None) is None

    def test_unknown_level_returns_none(self) -> None:
        assert map_census_geography("metro", None, None) is None
