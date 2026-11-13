"""The Bureau's tract relationship files, parsed offline (acs-place-and-tract-history).

Covers: ETL-102
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.silver_ref.tract_crosswalk import (
    RELATIONSHIP_FILES,
    parse_2000_2010,
    parse_2010_2020,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/silver_ref/tract_crosswalk"
KENT_404 = "state:10|county:001|tract:040500"


def _parts_2000():
    return parse_2000_2010((FIXTURES / "us2010trf.excerpt.txt").read_bytes())


def test_a_2000_tract_split_in_2010_carries_its_population_weights() -> None:
    """Covers: ETL-102 -- each part's land and 2010 population shares, as published."""
    parts = {
        part.later_geo_id: part
        for part in _parts_2000()
        if part.earlier_geo_id == KENT_404
    }
    assert set(parts) == {
        "state:10|county:001|tract:040501",
        "state:10|county:001|tract:040502",
        "state:10|county:001|tract:043202",
    }
    first = parts["state:10|county:001|tract:040501"]
    assert first.earlier_vintage == 2000
    assert first.land_area_part_m2 == 10466151
    assert first.earlier_land_share == Decimal("0.263")
    assert first.later_land_share == Decimal("1")
    assert (first.later_population_part, first.earlier_population_share) == (
        4923,
        Decimal("0.7131"),
    )
    shares = sum(part.earlier_population_share for part in parts.values())
    assert shares == Decimal("1")


def test_an_unchanged_tract_succeeds_itself() -> None:
    """Covers: ETL-102 -- the same code in both decades, all of it, is its own successor."""
    unchanged = [
        part
        for part in _parts_2000()
        if part.earlier_geo_id == "state:10|county:001|tract:040100"
    ]
    assert len(unchanged) == 1
    assert unchanged[0].later_geo_id == unchanged[0].earlier_geo_id
    assert unchanged[0].earlier_land_share == unchanged[0].later_land_share == 1


def test_the_2010_to_2020_file_publishes_land_shares_and_no_population() -> None:
    """Covers: ETL-102 -- shares are part land over each tract's land; population is absent."""
    parts = parse_2010_2020(
        (FIXTURES / "tab20_tract20_tract10.excerpt.txt").read_bytes()
    )
    assert {part.earlier_vintage for part in parts} == {2010}
    sliver = next(
        part
        for part in parts
        if part.earlier_geo_id == "state:10|county:003|tract:016901"
        and part.later_geo_id == "state:10|county:001|tract:040201"
    )
    assert sliver.land_area_part_m2 == 6921
    assert sliver.earlier_land_share == Decimal(6921) / Decimal(53757620)
    assert sliver.later_land_share == Decimal(6921) / Decimal(9732880)
    assert all(part.later_population_part is None for part in parts)
    assert all(part.earlier_population_share is None for part in parts)


def test_a_malformed_file_is_refused_whole() -> None:
    """Covers: ETL-102 -- a short row, a bad GEOID or missing columns refuse the file."""
    with pytest.raises(ValueError, match="columns, not 30"):
        parse_2000_2010(b"10,001,040100\n")
    row = (FIXTURES / "us2010trf.excerpt.txt").read_text().splitlines()[0]
    with pytest.raises(ValueError, match="tract GEOID"):
        parse_2000_2010(row.replace("10001040100", "1000104010X", 1).encode())
    with pytest.raises(ValueError, match="lacks its columns"):
        parse_2010_2020(b"GEOID_TRACT_20|GEOID_TRACT_10\n1|2\n")
    with pytest.raises(ValueError, match="lists no part"):
        parse_2000_2010(b"")
    assert set(RELATIONSHIP_FILES) == {2000, 2010}
