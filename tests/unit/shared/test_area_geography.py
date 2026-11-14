"""Regions, divisions and CBSAs are read from Census codes, never names.

Covers: ETL-075
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.silver_ref.area_geography import (
    parse_cbsa_delineation,
    parse_regions_and_divisions,
)
from data_ingestion_toolbox.silver_ref.geography_contract import (
    canonical_geo_id,
    resolve_provider_geography,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "silver_ref" / "area"
STATES = (FIXTURES / "NST-EST2024-ALLDATA.excerpt.csv").read_bytes()
CBSA = (FIXTURES / "cbsa-est2024-alldata.excerpt.csv").read_bytes()


def test_area_identities_come_from_codes_alone() -> None:
    """Covers: ETL-075 — each area kind has one code shape, and nothing else identifies it."""
    assert canonical_geo_id("census_region", area_code="3") == "region:3"
    assert canonical_geo_id("census_division", area_code=5) == "division:5"
    assert canonical_geo_id("metro", area_code="31540") == "cbsa:31540"
    assert (
        canonical_geo_id("provider_area", area_code="S12A", provider="bls_cpi")
        == "area:bls_cpi:S12A"
    )
    for kind, kwargs in (
        ("census_region", {"area_code": "5"}),
        ("census_division", {"area_code": "0"}),
        ("metro", {"area_code": "3154"}),
        ("metro", {"area_code": "31540", "state_fips": "55"}),
        ("provider_area", {"area_code": "S12A"}),
        ("provider_area", {"area_code": "New York, NY", "provider": "bls_cpi"}),
    ):
        with pytest.raises(ValueError):
            canonical_geo_id(kind, **kwargs)


def test_a_name_alone_never_resolves_an_area() -> None:
    """Covers: ETL-075 — a metro named rather than coded is ledgered unmapped, not guessed."""
    resolved = resolve_provider_geography("BLS", "census_division", area_code="5")
    assert (resolved.geo_id, resolved.status) == ("division:5", "resolved")
    named = resolve_provider_geography("BLS", "metro", area_code="Madison, WI")
    assert (named.geo_id, named.status, named.reason_code) == (
        None,
        "unmapped",
        "invalid_exact_code",
    )
    # A source that does not publish areas is refused the type outright.
    refused = resolve_provider_geography("CDC", "metro", area_code="31540")
    assert refused.status == "unsupported"


def test_regions_and_divisions_contain_their_states_by_code() -> None:
    """Covers: ETL-075 — four regions, nine divisions, and each state's two parents."""
    snapshot = parse_regions_and_divisions(STATES, vintage=2024)
    kinds = sorted((r.geo_type, r.geo_id, r.name) for r in snapshot.records)
    assert ("census_region", "region:2", "Midwest Region") in kinds
    assert ("census_division", "division:3", "East North Central") in kinds
    assert len(kinds) == 13
    assert ("region:2", "state:55") in snapshot.memberships
    assert ("division:3", "state:55") in snapshot.memberships
    assert ("region:3", "division:5") in snapshot.memberships
    assert ("division:5", "state:11") in snapshot.memberships
    # Puerto Rico is in no region: no membership is invented for it.
    assert not any(member == "state:72" for _, member in snapshot.memberships)


def test_cbsas_contain_their_counties_by_fips() -> None:
    """Covers: ETL-075 — metro and micro areas by OMB code, members by county FIPS."""
    snapshot = parse_cbsa_delineation(CBSA, delineation_vintage=2023)
    metros = {r.geo_id: r for r in snapshot.records}
    assert set(metros) == {"cbsa:10180", "cbsa:25540", "cbsa:31540"}
    madison = metros["cbsa:31540"]
    assert (madison.name, madison.lsad, madison.geography_vintage) == (
        "Madison, WI",
        "Metropolitan Statistical Area",
        2023,
    )
    assert ("cbsa:31540", "state:55|county:025") in snapshot.memberships
    # Connecticut's planning regions are the delineation's county equivalents.
    assert ("cbsa:25540", "state:09|county:110") in snapshot.memberships
    assert snapshot.member_codes["state:09|county:110"] == ("county", "09110")


@pytest.mark.parametrize(
    ("payload", "parser"),
    [
        (b"SUMLEV,NAME\n040,Wisconsin\n", "regions"),
        (STATES.replace(b"\n030,1,2,", b"\n031,1,2,", 1), "regions"),
        (b"CBSA,NAME\n31540,Madison\n", "cbsa"),
        (CBSA.replace(b"31540,,55025,", b"31540,,5525,", 1), "cbsa"),
    ],
)
def test_a_malformed_file_is_refused(payload: bytes, parser: str) -> None:
    """Covers: ETL-075 — missing columns, a missing division or a short county code stop the parse."""
    with pytest.raises(ValueError):
        if parser == "regions":
            parse_regions_and_divisions(payload, vintage=2024)
        else:
            parse_cbsa_delineation(payload, delineation_vintage=2023)


def _cbsa_boundary(code: str, *, field: str = "CBSAFP") -> bytes:
    import io
    import zipfile

    import shapefile

    shp, shx, dbf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    writer = shapefile.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapefile.POLYGON)
    writer.field(field, "C", size=5)
    writer.field("NAMELSAD", "C", size=100)
    writer.poly(
        [[[-89.5, 43.0], [-89.5, 43.2], [-89.3, 43.2], [-89.3, 43.0], [-89.5, 43.0]]]
    )
    writer.record(code, "Madison, WI Metro Area")
    writer.close()
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        for suffix, buffer in (("shp", shp), ("shx", shx), ("dbf", dbf)):
            archive.writestr(f"cb_2023_us_cbsa_500k.{suffix}", buffer.getvalue())
    return payload.getvalue()


@pytest.mark.parametrize("field", ["CBSAFP", "GEOID"])
def test_a_cbsa_boundary_is_keyed_by_its_omb_code_and_names_nothing(field: str) -> None:
    """Covers: ETL-105 — the CBSA polygon carries the delineation vintage and no attributes.

    The metro's name and kind come from the delineation file that lists its
    counties. The boundary file's "Madison, WI Metro Area" would be a second
    attribute version renaming every metro, so a metro boundary carries none.
    """
    from data_ingestion_toolbox.silver_ref.geography_pipeline import (
        parse_boundary_capture,
    )

    (record,) = parse_boundary_capture(
        _cbsa_boundary("31540", field=field), geo_type="metro", boundary_vintage=2023
    )
    assert record.geo_id == "cbsa:31540"
    assert record.boundary_vintage == 2023
    assert record.geography is None
    assert '"type":"Polygon"' in record.geojson


def test_a_cbsa_boundary_without_a_five_digit_code_is_refused() -> None:
    """Covers: ETL-105 — no polygon is attached to a guessed identity."""
    from data_ingestion_toolbox.silver_ref.geography_pipeline import (
        parse_boundary_capture,
    )

    with pytest.raises(ValueError, match="five-digit code"):
        parse_boundary_capture(
            _cbsa_boundary("3154"), geo_type="metro", boundary_vintage=2023
        )


def test_the_area_release_captures_the_boundary_of_its_own_delineation() -> None:
    """Covers: ETL-105 — the polygons are the delineation the membership follows."""
    from data_ingestion_toolbox.silver_ref.area_geography import AREA_RELEASES

    for release in AREA_RELEASES.values():
        assert release.cbsa_boundary_url.endswith(
            f"/GENZ{release.delineation_vintage}/shp/"
            f"cb_{release.delineation_vintage}_us_cbsa_500k.zip"
        )
