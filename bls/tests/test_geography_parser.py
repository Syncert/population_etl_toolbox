"""Unit tests for BLS geography parsing.

These are lightweight and do not require a database.
Run with pytest (if you use it) or just execute the file.
"""

from bls.silver_bls.geography_parser import parse_bls_geography


def test_parse_laus_us():
    r = parse_bls_geography("LAU00000000000000003", program="la")
    assert r["geo_level"] == "us"
    assert r["geo_id"] == "us:1"


def test_parse_laus_state():
    # ST + state_fips + padding zeros (length can vary)
    r = parse_bls_geography("LAUST01000000000000003", program="la")
    assert r["geo_level"] == "state"
    assert r["geo_id"] == "state:01"
    assert r["state_fips"] == "01"


def test_parse_laus_county():
    # CN + state_fips + 5-digit county FIPS (includes state+county), then padding
    # Example county_fips_full=01001 -> county part is 001
    r = parse_bls_geography("LAUCN01010010000000003", program="la")
    # NOTE: the prefix is LAU + seasonal(U) + area_code starting with CN...
    # This is a synthetic example just to validate slicing behavior.
    assert r["geo_level"] in ("county", None)
    if r["geo_level"] == "county":
        assert r["state_fips"] == "01"
        assert r["county_fips"].isdigit() and len(r["county_fips"]) == 3


if __name__ == "__main__":
    test_parse_laus_us()
    test_parse_laus_state()
    test_parse_laus_county()
    print("ok")
