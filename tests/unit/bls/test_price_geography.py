"""CPI and average-price areas map to the shared reference by code.

Covers: ETL-077
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.bls.silver_bls.geography_parser import parse_bls_geography

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("series_id", "program", "geo_level", "geo_id"),
    [
        ("CUUR0000SA0", "cu", "us", "us:1"),
        ("CUUR0200SAF11", "cu", "census_region", "region:2"),
        ("CUUR0230SETB01", "cu", "census_division", "division:3"),
        ("CUUR0490SETB01", "cu", "census_division", "division:9"),
        ("CUURS12ASAF11", "cu", "provider_area", "area:bls_cpi:S12A"),
        ("APU0300709112", "ap", "census_region", "region:3"),
        ("APUS35A74714", "ap", "provider_area", "area:bls_cpi:S35A"),
    ],
)
def test_a_price_area_resolves_by_its_code(
    series_id: str, program: str, geo_level: str, geo_id: str
) -> None:
    """Covers: ETL-077 — nation, region, division and metro from the series id alone."""
    parsed = parse_bls_geography(series_id, program)
    assert (parsed["geo_level"], parsed["geo_id"]) == (geo_level, geo_id)
    assert parsed["state_fips"] is None and parsed["county_fips"] is None


@pytest.mark.parametrize(
    "series_id",
    ["CUURA104SA0", "CUURN100SA0", "CUURS000SA0", "CUURD200SA0"],
)
def test_a_discontinued_area_or_size_class_is_refused(series_id: str) -> None:
    """Covers: ETL-077 — Pittsburgh's discontinued series and size classes have no geography."""
    assert parse_bls_geography(series_id, "cu")["geo_id"] is None
