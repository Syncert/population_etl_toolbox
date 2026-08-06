"""ETL unit tests: BLS LAUS geography parser and builder.

Covers ETL-009 (LAUS series round-trip with exact program, measure, FIPS,
and geo_id) and ETL-010 (unsupported geography rejected).
"""

import pytest

from data_ingestion_toolbox.bls.geography import (
    build_laus_series_id,
    parse_laus_series_id,
)


@pytest.mark.unit
class TestParseLausSeries:
    """ETL-009: parse_laus_series_id extracts correct components."""

    def test_state_series_parses_correctly(self) -> None:
        sid = "LAUST060000000000003"
        result = parse_laus_series_id(sid)
        assert result["program"] == "LA"
        assert result["seasonal"] == "U"
        assert result["geo_level"] == "state"
        assert result["state_fips"] == "06"
        assert result["county_fips"] is None
        assert result["geo_id"] == "state:06"
        assert result["measure_code"] == "03"

    def test_county_series_parses_correctly(self) -> None:
        sid = "LAUCN550250000000003"
        result = parse_laus_series_id(sid)
        assert result["program"] == "LA"
        assert result["seasonal"] == "U"
        assert result["geo_level"] == "county"
        assert result["state_fips"] == "55"
        assert result["county_fips"] == "025"
        assert result["geo_id"] == "state:55|county:025"
        assert result["measure_code"] == "03"

    def test_seasonally_adjusted_state_series(self) -> None:
        sid = "LAUST060000000000004"
        result = parse_laus_series_id(sid)
        assert result["seasonal"] == "U"
        sid_s = "LASS" + sid[4:]
        # Build a seasonally-adjusted series and parse it
        s_series = "LAS" + "S" + "T060000000000004"
        result_s = parse_laus_series_id(s_series)
        assert result_s["seasonal"] == "S"

    def test_all_measure_codes_parse(self) -> None:
        for code in ("03", "04", "05", "06"):
            sid = f"LAUST0600000000000{code}"
            result = parse_laus_series_id(sid)
            assert result["measure_code"] == code, f"measure_code mismatch for {code}"

    def test_series_id_too_short_returns_none_fields(self) -> None:
        result = parse_laus_series_id("SHORT")
        assert result["geo_level"] is None
        assert result["geo_id"] is None
        assert result["state_fips"] is None

    def test_non_string_returns_empty(self) -> None:
        result = parse_laus_series_id(None)  # type: ignore[arg-type]
        assert result["geo_level"] is None

    def test_wrong_program_prefix_returns_empty(self) -> None:
        result = parse_laus_series_id("XXUST060000000000003")
        assert result["geo_level"] is None

    def test_national_laus_area_rejected(self) -> None:
        # National-level LAUS area code doesn't match state or county pattern
        sid = "LAUUS000000000000003"
        result = parse_laus_series_id(sid)
        assert result["geo_level"] is None

    def test_metro_area_rejected(self) -> None:
        # MT prefix not a supported geographic pattern
        sid = "LAUMT123456789012303"
        result = parse_laus_series_id(sid)
        assert result["geo_level"] is None


@pytest.mark.unit
class TestBuildLausSeries:
    """ETL-009: build_laus_series_id constructs the exact 20-character ID."""

    def test_state_series_round_trip(self) -> None:
        area_code = "ST060000000000000"[:15]  # ST + 2 FIPS + 11 zeros
        area_code = "ST06" + "0" * 11
        sid = build_laus_series_id(area_code, "03", seasonal="U")
        assert len(sid) == 20
        assert sid.startswith("LAU")
        assert sid[2] == "U"
        parsed = parse_laus_series_id(sid)
        assert parsed["geo_level"] == "state"
        assert parsed["state_fips"] == "06"
        assert parsed["measure_code"] == "03"

    def test_county_series_round_trip(self) -> None:
        area_code = "CN55025" + "0" * 8
        sid = build_laus_series_id(area_code, "06", seasonal="U")
        assert len(sid) == 20
        parsed = parse_laus_series_id(sid)
        assert parsed["geo_level"] == "county"
        assert parsed["state_fips"] == "55"
        assert parsed["county_fips"] == "025"

    def test_seasonal_flag_preserved(self) -> None:
        area_code = "ST06" + "0" * 11
        sid_s = build_laus_series_id(area_code, "03", seasonal="S")
        assert sid_s[3] == "S"

    def test_invalid_area_code_length_raises(self) -> None:
        with pytest.raises(ValueError, match="area_code must be 15 characters"):
            build_laus_series_id("TOOSHORT", "03")

    def test_unsupported_area_code_pattern_raises(self) -> None:
        # MT-prefix (metro) is not supported
        area_code = "MT123456789012345"[:15]
        with pytest.raises(ValueError, match="unsupported LAUS area_code pattern"):
            build_laus_series_id(area_code, "03")

    def test_invalid_measure_code_raises(self) -> None:
        area_code = "ST06" + "0" * 11
        with pytest.raises(ValueError, match="measure_code must be 2 digits"):
            build_laus_series_id(area_code, "3")

    def test_invalid_seasonal_raises(self) -> None:
        area_code = "ST06" + "0" * 11
        with pytest.raises(ValueError, match="seasonal must be"):
            build_laus_series_id(area_code, "03", seasonal="X")
