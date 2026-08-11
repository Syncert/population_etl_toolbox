"""ETL unit tests: BLS LAUS geography parser and builder.

Covers ETL-009 (LAUS series round-trip with exact program, measure, FIPS,
and geo_id) and ETL-010 (unsupported geography rejected).
"""

import pytest

from data_ingestion_toolbox.bls.geography import (
    build_laus_series_id,
    get_laus_area_codes,
    get_laus_series_ids,
    parse_laus_series_id,
)


@pytest.mark.unit
class TestParseLausSeries:
    """ETL-009: parse_laus_series_id extracts correct components."""

    def test_state_series_parses_correctly(self) -> None:
        """Covers: ETL-009 — a published state LAUS series parses exactly."""
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
        """Covers: ETL-009 — a published county LAUS series parses exactly."""
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
        """Covers: ETL-009 — parsing preserves seasonal adjustment."""
        sid = "LAUST060000000000004"
        result = parse_laus_series_id(sid)
        assert result["seasonal"] == "U"
        # Build a seasonally-adjusted series and parse it
        s_series = "LAS" + "S" + "T060000000000004"
        result_s = parse_laus_series_id(s_series)
        assert result_s["seasonal"] == "S"

    def test_all_measure_codes_parse(self) -> None:
        """Covers: ETL-009 — all supported LAUS measures parse."""
        for code in ("03", "04", "05", "06"):
            sid = f"LAUST0600000000000{code}"
            result = parse_laus_series_id(sid)
            assert result["measure_code"] == code, f"measure_code mismatch for {code}"

    def test_series_id_too_short_returns_none_fields(self) -> None:
        """Covers: ETL-010 — a short LAUS series ID is rejected."""
        result = parse_laus_series_id("SHORT")
        assert result["geo_level"] is None
        assert result["geo_id"] is None
        assert result["state_fips"] is None

    def test_non_string_returns_empty(self) -> None:
        """Covers: ETL-010 — a non-string LAUS ID is rejected."""
        result = parse_laus_series_id(None)  # type: ignore[arg-type]
        assert result["geo_level"] is None

    def test_wrong_program_prefix_returns_empty(self) -> None:
        """Covers: ETL-010 — a non-LAUS program prefix is rejected."""
        result = parse_laus_series_id("XXUST060000000000003")
        assert result["geo_level"] is None

    def test_national_laus_area_rejected(self) -> None:
        """Covers: ETL-010, ETL-033 — national LAUS is rejected."""
        # National-level LAUS area code doesn't match state or county pattern
        sid = "LAUUS000000000000003"
        result = parse_laus_series_id(sid)
        assert result["geo_level"] is None

    def test_metro_area_rejected(self) -> None:
        """Covers: ETL-010 — unsupported metro LAUS geography is rejected."""
        # MT prefix not a supported geographic pattern
        sid = "LAUMT123456789012303"
        result = parse_laus_series_id(sid)
        assert result["geo_level"] is None


@pytest.mark.unit
class TestBuildLausSeries:
    """ETL-009: build_laus_series_id constructs the exact 20-character ID."""

    def test_state_series_round_trip(self) -> None:
        """Covers: ETL-009 — a state series round-trips through its builder."""
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
        """Covers: ETL-009 — a county series round-trips through its builder."""
        area_code = "CN55025" + "0" * 8
        sid = build_laus_series_id(area_code, "06", seasonal="U")
        assert len(sid) == 20
        parsed = parse_laus_series_id(sid)
        assert parsed["geo_level"] == "county"
        assert parsed["state_fips"] == "55"
        assert parsed["county_fips"] == "025"

    def test_seasonal_flag_preserved(self) -> None:
        """Covers: ETL-009 — the builder preserves seasonal adjustment."""
        area_code = "ST06" + "0" * 11
        sid_s = build_laus_series_id(area_code, "03", seasonal="S")
        assert sid_s[3] == "S"

    def test_invalid_area_code_length_raises(self) -> None:
        """Covers: ETL-010 — invalid LAUS area-code length is rejected."""
        with pytest.raises(ValueError, match="area_code must be 15 characters"):
            build_laus_series_id("TOOSHORT", "03")

    def test_unsupported_area_code_pattern_raises(self) -> None:
        """Covers: ETL-010 — unsupported LAUS area patterns are rejected."""
        # MT-prefix (metro) is not supported
        area_code = "MT123456789012345"[:15]
        with pytest.raises(ValueError, match="unsupported LAUS area_code pattern"):
            build_laus_series_id(area_code, "03")

    def test_invalid_measure_code_raises(self) -> None:
        """Covers: ETL-010 — unsupported LAUS measures are rejected."""
        area_code = "ST06" + "0" * 11
        with pytest.raises(ValueError, match="measure_code must be 2 digits"):
            build_laus_series_id(area_code, "3")

    def test_invalid_seasonal_raises(self) -> None:
        """Covers: ETL-010 — invalid seasonal flags are rejected."""
        area_code = "ST06" + "0" * 11
        with pytest.raises(ValueError, match="seasonal must be"):
            build_laus_series_id(area_code, "03", seasonal="X")


class _Cursor:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.rows = rows
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def execute(self, sql, params) -> None:
        self.params = params

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.cursor_value = _Cursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self) -> None:
        self.closed = True


@pytest.mark.unit
class TestLausMetadataQueries:
    @pytest.mark.parametrize(
        ("level", "state_fips", "prefix"),
        [
            ("state", None, "ST%"),
            ("county", "6", "CN06%"),
            ("metro", None, "MT%"),
            ("city", None, "CI%"),
        ],
    )
    def test_area_code_queries_use_exact_prefix(
        self, monkeypatch, level: str, state_fips: str | None, prefix: str
    ) -> None:
        """Covers: ETL-032 — metadata queries use exact geography prefixes."""
        connection = _Connection([("ST0600000000000",)])
        monkeypatch.setattr(
            "data_ingestion_toolbox.bls.geography._get_pg_connection",
            lambda: connection,
        )
        assert get_laus_area_codes(level, state_fips) == ["ST0600000000000"]
        assert connection.cursor_value.params == (prefix,)
        assert connection.closed

    @pytest.mark.parametrize(
        ("level", "state_fips", "message"),
        [
            ("us", None, "CPS/LN"),
            ("county", None, "state_fips"),
            ("tract", None, "Unsupported"),
        ],
    )
    def test_area_code_queries_reject_unsupported_scope(
        self, level: str, state_fips: str | None, message: str
    ) -> None:
        """Covers: ETL-032 — unsupported metadata scopes are rejected."""
        with pytest.raises(ValueError, match=message):
            get_laus_area_codes(level, state_fips)

    def test_state_series_query_filters_invalid_metadata(self, monkeypatch) -> None:
        """Covers: ETL-032 — state queries discard invalid metadata rows."""
        connection = _Connection([("LAUST060000000000003",), ("LAUMT123450000000003",)])
        monkeypatch.setattr(
            "data_ingestion_toolbox.bls.geography._get_pg_connection",
            lambda: connection,
        )
        assert get_laus_series_ids(["03"], "state") == ["LAUST060000000000003"]
        assert connection.cursor_value.params == ("ST%", "U", ["03"])

    def test_county_series_query_keeps_requested_state(self, monkeypatch) -> None:
        """Covers: ETL-032 — county queries retain only the requested state."""
        connection = _Connection([("LAUCN060010000000003",), ("LAUCN550010000000003",)])
        monkeypatch.setattr(
            "data_ingestion_toolbox.bls.geography._get_pg_connection",
            lambda: connection,
        )
        assert get_laus_series_ids(["03"], "county", "06") == ["LAUCN060010000000003"]

    @pytest.mark.parametrize(
        ("measures", "level", "state_fips", "seasonal", "message"),
        [
            (["03"], "us", None, "U", "CPS/LN"),
            (["03"], "county", None, "U", "state_fips"),
            (["03"], "county", "XX", "U", "state_fips"),
            (["03"], "metro", None, "U", "Unsupported"),
            (["03"], "state", None, "X", "seasonal"),
            (["3"], "state", None, "U", "measure codes"),
        ],
    )
    def test_series_queries_validate_scope_before_database(
        self,
        measures: list[str],
        level: str,
        state_fips: str | None,
        seasonal: str,
        message: str,
    ) -> None:
        """Covers: ETL-032 — invalid scope fails before database access."""
        with pytest.raises(ValueError, match=message):
            get_laus_series_ids(measures, level, state_fips, seasonal)

    def test_empty_measure_list_does_not_query_database(self) -> None:
        """Covers: ETL-032 — empty measure scope avoids database access."""
        assert get_laus_series_ids([], "state") == []
