"""The API's page addresses follow the web's address rule (api-place-search)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from apps.api.services.place_paths import PagePaths, SegmentRow, place_slug

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "shared" / "place_segments.json"
)


def _segment_name(row: dict) -> str:
    # The field the web's `stateName` / `countyName` / `cityName` reads, then
    # `geo_name`; the API slugs the published `geo_name`, which resolves to
    # the same name at these grains.
    own = {"STATE": "state_name", "COUNTY": "county_name", "PLACE": "place_name"}
    field = own.get(row["geo_level"])
    return (field and row.get(field)) or row.get("geo_name") or row["geo_id"]


def test_page_paths_agree_with_the_web_address_rule() -> None:
    """Covers: API-171 — the shared fixture answers the same paths in both suites."""
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rows = [
        *fixture["states"],
        *fixture["counties"],
        *fixture["places"],
        *fixture["metros"],
    ]
    paths = PagePaths(
        SegmentRow(
            geo_id=row["geo_id"],
            geo_level=row["geo_level"],
            state_fips=row.get("state_fips"),
            county_fips=row.get("county_fips"),
            place_fips=row.get("place_fips"),
            segment_name=_segment_name(row),
        )
        for row in rows
    )
    # A tract's path is its county's plus its own segment: no tract row is
    # needed to build it (api-place-search, tract pages).
    rows = [*rows, *fixture["tracts"]]
    expected = fixture["expected_paths"]
    assert sorted(row["geo_id"] for row in rows) == sorted(expected)
    assert {row["geo_id"]: paths.path(row["geo_id"]) for row in rows} == expected


def test_a_grain_without_a_page_has_no_path() -> None:
    """Covers: API-171 — a ZCTA or an unknown id has no path, not a guess; a
    metro's path is its own name's slug (API-187)."""
    paths = PagePaths(
        [
            {
                "geo_id": "state:48",
                "geo_level": "STATE",
                "state_fips": "48",
                "segment_name": "Texas",
            },
            {
                "geo_id": "cbsa:12420",
                "geo_level": "METRO",
                "state_fips": "48",
                "segment_name": None,
            },
        ]
    )
    assert paths.path("state:48") == "/us/texas"
    # No name: the slug is empty, so the CBSA code answers.
    assert paths.path("cbsa:12420") == "/metro/12420"
    assert paths.path("zcta:78701") is None
    # A tract whose county is not among the rows has no path, not a guess.
    assert paths.path("state:48|county:453|tract:001805") is None
    assert paths.path(None) is None


def test_slug_matches_the_web_placeslug() -> None:
    """Covers: API-171 — accents, ampersands and punctuation slug as the web does."""
    assert place_slug("Española city") == "espanola-city"
    assert place_slug("Town & Country city") == "town-and-country-city"
    assert place_slug("  St. Mary's  County ") == "st-mary-s-county"
    assert place_slug(None) == ""
