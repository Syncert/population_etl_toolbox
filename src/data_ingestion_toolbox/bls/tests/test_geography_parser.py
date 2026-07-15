"""Unit tests for BLS geography parsing.

These are lightweight and do not require a database.
Run with pytest (if you use it) or just execute the file.
"""

from data_ingestion_toolbox.bls.silver_bls.geography_parser import parse_bls_geography


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
    # Example BLS county area code uses the 3-digit county FIPS followed by 00.
    r = parse_bls_geography("LAUCN0100100003", program="la")
    assert r["geo_level"] == "county"
    assert r["state_fips"] == "01"
    assert r["county_fips"] == "001"
    assert r["geo_id"] == "state:01|county:001"


def test_parse_laus_county_with_independent_city_code():
    r = parse_bls_geography("LAUCN5151000003", program="la")
    assert r["geo_level"] == "county"
    assert r["state_fips"] == "51"
    assert r["county_fips"] == "510"
    assert r["geo_id"] == "state:51|county:510"


if __name__ == "__main__":
    test_parse_laus_us()
    test_parse_laus_state()
    test_parse_laus_county()
    print("ok")
