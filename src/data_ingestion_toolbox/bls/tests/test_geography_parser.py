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
    r = parse_bls_geography("LAUST010000000000003", program="la")
    assert r["geo_level"] == "state"
    assert r["geo_id"] == "state:01"
    assert r["state_fips"] == "01"


def test_parse_laus_county():
    r = parse_bls_geography("LAUCN010010000000003", program="la")
    assert r["geo_level"] == "county"
    assert r["state_fips"] == "01"
    assert r["county_fips"] == "001"
    assert r["geo_id"] == "state:01|county:001"


def test_parse_laus_county_with_independent_city_code():
    r = parse_bls_geography("LAUCN515100000000003", program="la")
    assert r["geo_level"] == "county"
    assert r["state_fips"] == "51"
    assert r["county_fips"] == "510"
    assert r["geo_id"] == "state:51|county:510"


def test_parse_laus_metro():
    r = parse_bls_geography("LAUMT171698000000003", program="la")
    assert r == {
        "geo_level": "metro",
        "geo_id": "metro:16980",
        "state_fips": "17",
        "county_fips": None,
    }


def test_parse_laus_city():
    r = parse_bls_geography("LAUCT480500000000003", program="la")
    assert r == {
        "geo_level": "city",
        "geo_id": "state:48|city:05000",
        "state_fips": "48",
        "county_fips": None,
    }


def test_rejects_legacy_short_laus_id():
    r = parse_bls_geography("LAUCN0100100003", program="la")
    assert all(value is None for value in r.values())


if __name__ == "__main__":
    test_parse_laus_us()
    test_parse_laus_state()
    test_parse_laus_county()
    print("ok")
