"""Provider-defined areas come from the provider's own list, by its codes.

Covers: ETL-076
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.silver_ref.provider_areas import (
    PROVIDER_AREA_LISTS,
    parse_bls_cpi_areas,
    provider_area_records,
)

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "silver_ref"
    / "provider_areas"
    / "bls_cu.area"
)


def test_only_current_cpi_metros_are_bls_provider_areas() -> None:
    """Covers: ETL-076 — 23 metros; discontinued areas and size classes are not places."""
    areas = parse_bls_cpi_areas(FIXTURE.read_bytes())
    codes = [area.code for area in areas]
    assert len(codes) == 23
    assert "S12A" in codes and "S49G" in codes
    assert not any(code.startswith(("A", "N", "D")) for code in codes)
    assert "S000" not in codes and "S100" not in codes
    assert dict((a.code, a.name) for a in areas)["S12A"] == (
        "New York-Newark-Jersey City, NY-NJ-PA"
    )


def test_a_provider_area_is_identified_by_provider_and_code() -> None:
    """Covers: ETL-076 — the identity is the provider's code, never a CBSA matched by name."""
    (record,) = provider_area_records(
        "bls_cpi", parse_bls_cpi_areas(FIXTURE.read_bytes())[:1], vintage=2026
    )
    assert (record.geo_type, record.geo_id, record.area_code, record.census_geoid) == (
        "provider_area",
        "area:bls_cpi:S11A",
        "bls_cpi:S11A",
        "S11A",
    )
    assert record.state_fips is None and record.county_fips is None


@pytest.mark.parametrize(
    "payload",
    [b"code\tname\nS12A\tNew York\n", b"area_code\tarea_name\n0000\tU.S. city average\n"],
)
def test_a_list_without_its_columns_or_its_metros_is_refused(payload: bytes) -> None:
    """Covers: ETL-076 — a malformed or empty list stops the load."""
    with pytest.raises(ValueError):
        parse_bls_cpi_areas(payload)


def test_every_list_names_its_provider_and_source() -> None:
    """Covers: ETL-076 — each registered list is captured under its own source."""
    bls = PROVIDER_AREA_LISTS["bls_cpi"]
    assert bls.source_code == "BLS"
    assert bls.url.endswith("/cu/cu.area")
    assert "User-Agent" in bls.headers
