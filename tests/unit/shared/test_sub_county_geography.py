"""Offline contracts for tract and ZCTA geography (sub-county-geography).

Covers: ETL-060
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.silver_ref.geography_contract import (
    canonical_geo_id,
    resolve_provider_geography,
)
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    SUB_COUNTY_SCOPE,
    ZCTA_BOUNDARY_VINTAGE,
    parse_boundary_capture,
    parse_gazetteer_capture,
    sub_county_urls,
    tract_name,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "geography"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_tract_and_zcta_identities_come_from_exact_codes() -> None:
    """Covers: ETL-060 — a tract names its county; a ZCTA names nothing but itself."""
    assert canonical_geo_id(
        "tract", state_fips="10", county_fips="1", tract_code="40100"
    ) == ("state:10|county:001|tract:040100")
    assert canonical_geo_id("zcta", zcta_code="1901") == "zcta:01901"
    with pytest.raises(ValueError):
        canonical_geo_id("tract", state_fips="10", tract_code="040100")
    with pytest.raises(ValueError):
        canonical_geo_id("zcta", state_fips="10", zcta_code="19901")
    with pytest.raises(ValueError):
        canonical_geo_id(
            "tract", state_fips="10", county_fips="001", tract_code="1234567"
        )
    resolved = resolve_provider_geography(
        "CENSUS_ACS", "tract", state_fips="10", county_fips="001", tract_code="040100"
    )
    assert (resolved.status, resolved.geo_id) == (
        "resolved",
        "state:10|county:001|tract:040100",
    )
    assert (
        resolve_provider_geography("BLS", "tract", state_fips="10").status
        == "unsupported"
    )


def test_tract_names_follow_the_bureaus_convention() -> None:
    """Covers: ETL-060 — 040100 is Census Tract 401; 040203 is Census Tract 402.03."""
    assert tract_name("040100") == "Census Tract 401"
    assert tract_name("040203") == "Census Tract 402.03"
    assert tract_name("990000") == "Census Tract 9900"


def test_the_tract_gazetteer_replays_every_delaware_tract() -> None:
    """Covers: ETL-060 — the tract Gazetteer carries no name; the name comes from the code."""
    records = parse_gazetteer_capture(
        _fixture("2024_Gaz_tracts_DE.zip"), geo_type="tract", geography_vintage=2024
    )
    assert len(records) == 262
    first = next(record for record in records if record.census_geoid == "10001040100")
    assert first.geo_id == "state:10|county:001|tract:040100"
    assert (first.state_fips, first.county_fips, first.tract_code) == (
        "10",
        "001",
        "040100",
    )
    assert first.name == "Census Tract 401" and first.usps == "DE"
    assert first.land_area_m2 == 124745857
    assert {record.county_fips for record in records} == {"001", "003", "005"}


def test_the_zcta_gazetteer_and_boundaries_carry_no_state() -> None:
    """Covers: ETL-060 — a ZCTA crosses state lines, so it has no state code."""
    attributes = parse_gazetteer_capture(
        _fixture("2024_Gaz_zcta_DE.zip"), geo_type="zcta", geography_vintage=2024
    )
    dover = next(record for record in attributes if record.zcta_code == "19901")
    assert (dover.geo_id, dover.state_fips, dover.name) == (
        "zcta:19901",
        None,
        "ZCTA5 19901",
    )
    boundaries = parse_boundary_capture(
        _fixture("cb_2020_zcta520_DE_500k.zip"),
        geo_type="zcta",
        boundary_vintage=ZCTA_BOUNDARY_VINTAGE,
    )
    assert {record.geo_id for record in boundaries} == {
        record.geo_id for record in attributes
    }
    assert all(record.boundary_vintage == 2020 for record in boundaries)


def test_tract_boundaries_name_their_county() -> None:
    """Covers: ETL-060 — the boundary file's tract code and county make the identity."""
    boundaries = parse_boundary_capture(
        _fixture("cb_2024_10_tract_500k.zip"), geo_type="tract", boundary_vintage=2024
    )
    assert len(boundaries) == 259
    sample = next(
        record
        for record in boundaries
        if record.geo_id == "state:10|county:003|tract:010400"
    )
    assert sample.geography is not None and sample.geography.name == "Census Tract 104"
    assert sample.geography.tract_code == "010400"


def test_existing_records_keep_their_checksums() -> None:
    """Covers: ETL-060 — adding the sub-county codes changes no county's attribute checksum."""
    county = GeographyRecord(
        "county", "state:10|county:001", "10001", "10", "001", None, "Kent County", 2024
    )
    tract = GeographyRecord(
        "tract",
        "state:10|county:001|tract:040100",
        "10001040100",
        "10",
        "001",
        None,
        "Census Tract 401",
        2024,
        tract_code="040100",
    )
    import hashlib
    import json
    from dataclasses import asdict

    legacy = {
        key: value
        for key, value in asdict(county).items()
        if key not in {"tract_code", "zcta_code"}
    }
    expected = hashlib.sha256(
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert county.attribute_checksum == expected
    assert tract.attribute_checksum != expected


def test_the_sub_county_assets_use_the_decennial_zcta_boundary() -> None:
    """Covers: ETL-060 — every vintage reads the 2020 ZCTA boundary the Bureau publishes."""
    assets = {
        (product, geo_type): url for product, geo_type, url in sub_county_urls(2024)
    }
    assert assets[("attributes", "tract")].endswith(
        "2024_Gazetteer/2024_Gaz_tracts_national.zip"
    )
    assert assets[("geometry", "tract")].endswith(
        "GENZ2024/shp/cb_2024_us_tract_500k.zip"
    )
    assert assets[("geometry", "zcta")].endswith(
        "GENZ2020/shp/cb_2020_us_zcta520_500k.zip"
    )
    assert SUB_COUNTY_SCOPE == "sub_county"
