"""TileJSON, URL, and geography-join contracts shared with Martin smoke checks."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.silver_census.geography_mapper import (
    map_census_geography,
)
from data_ingestion_toolbox.martin_contract import (
    MartinContractError,
    choose_join_key,
    extract_vector_layer,
    field_names,
    normalize_base_url,
    normalize_tile_template,
    reconcile_geo_ids,
    sample_tile_url,
    tile_layer_url,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "fields",
    [
        {"geo_id": "String", "county_fips": "String"},
        ["county_fips", "geo_id"],
    ],
)
def test_tilejson_selects_exact_counties_layer_and_canonical_field(fields) -> None:
    """Covers: MARTIN-003 — exact layer selection accepts both field formats."""
    tilejson = {
        "vector_layers": [
            {"id": "states", "fields": {"geo_id": "String"}},
            {"id": "counties", "fields": fields},
        ]
    }
    layer = extract_vector_layer(tilejson, "counties")
    names = field_names(layer)
    assert layer["id"] == "counties"
    assert choose_join_key(names) == "geo_id"


@pytest.mark.parametrize(
    ("tilejson", "message"),
    [
        (None, "TileJSON must be an object"),
        ({}, "vector_layers must be a non-empty list"),
        ({"vector_layers": "counties"}, "vector_layers must be a non-empty list"),
        ({"vector_layers": []}, "vector_layers must be a non-empty list"),
        (
            {"vector_layers": [{"id": "states", "fields": ["geo_id"]}]},
            "does not contain vector layer 'counties'",
        ),
    ],
)
def test_tilejson_rejects_missing_or_malformed_layer_metadata(
    tilejson: object, message: str
) -> None:
    """Covers: MARTIN-003 — malformed or missing vector layers fail clearly."""
    with pytest.raises(MartinContractError, match=message):
        extract_vector_layer(tilejson, "counties")


@pytest.mark.parametrize("fields", [None, "geo_id", [], {}])
def test_tilejson_rejects_missing_or_malformed_fields(fields: object) -> None:
    """Covers: MARTIN-003 — field metadata must be non-empty object or list."""
    with pytest.raises(MartinContractError, match="fields"):
        field_names({"id": "counties", "fields": fields})


@pytest.mark.parametrize(
    ("fields", "expected"),
    [({"geo_id", "county_fips"}, "geo_id"), ({"GEO_ID"}, "GEO_ID")],
)
def test_join_key_is_canonical_and_case_insensitive(
    fields: set[str], expected: str
) -> None:
    """Covers: MARTIN-003, MARTIN-005 — only canonical geo_id is joinable."""
    assert choose_join_key(fields) == expected


@pytest.mark.parametrize(
    "fields",
    [
        {"geoid", "county_fips", "state_fips"},
        {"county_fips"},
        set(),
        {"geo_id", "GEO_ID"},
    ],
)
def test_join_key_rejects_fallback_only_or_ambiguous_fields(fields: set[str]) -> None:
    """Covers: MARTIN-003, MARTIN-005 — fallbacks cannot satisfy tile joins."""
    with pytest.raises(MartinContractError, match="exactly one geo_id"):
        choose_join_key(fields)


def test_tile_layer_url_uses_the_public_tiles_base() -> None:
    """Covers: MARTIN-004 — TileJSON requests retain the public /tiles base."""
    assert normalize_base_url("https://studio.example/tiles") == (
        "https://studio.example/tiles/"
    )
    assert tile_layer_url("https://studio.example/tiles/", "counties") == (
        "https://studio.example/tiles/counties"
    )


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        (
            "counties/{z}/{x}/{y}",
            "https://studio.example/tiles/counties/{z}/{x}/{y}",
        ),
        (
            "/counties/{z}/{x}/{y}.pbf",
            "https://studio.example/tiles/counties/{z}/{x}/{y}.pbf",
        ),
        (
            "/tiles/counties/{z}/{x}/{y}",
            "https://studio.example/tiles/counties/{z}/{x}/{y}",
        ),
        (
            "http://martin:3000/tiles/counties/{z}/{x}/{y}?token=test",
            "https://studio.example/tiles/counties/{z}/{x}/{y}?token=test",
        ),
    ],
)
def test_tile_template_normalizes_to_same_origin(template: str, expected: str) -> None:
    """Covers: MARTIN-004 — relative/internal templates become same-origin URLs."""
    assert (
        normalize_tile_template(template, "https://studio.example/tiles/") == expected
    )
    assert "martin:3000" not in expected


def test_sample_tile_url_substitutes_coordinates_and_bbox() -> None:
    """Covers: MARTIN-004 — tile placeholders resolve to one concrete request."""
    tilejson = {
        "tiles": ["http://martin:3000/tiles/counties/{z}/{x}/{y}?bbox={bbox-epsg-3857}"]
    }
    url = sample_tile_url(
        tilejson,
        "https://studio.example/tiles/",
        "counties",
        z=4,
        x=3,
        y=6,
    )
    assert url.startswith("https://studio.example/tiles/counties/4/3/6?bbox=")
    assert "{" not in url and "}" not in url
    assert "martin:3000" not in url


def test_sample_tile_url_uses_layer_fallback_when_tilejson_has_no_template() -> None:
    """Covers: MARTIN-004 — absent tile templates use the declared layer path."""
    assert sample_tile_url({}, "http://localhost:3001/tiles/", "counties") == (
        "http://localhost:3001/tiles/counties/0/0/0"
    )


def test_county_geo_id_preserves_padding_and_joins_exactly() -> None:
    """Covers: MARTIN-005 — API and tile county IDs join with exact padding."""
    geo_id = map_census_geography("county", "01", "001")
    assert geo_id == "state:01|county:001"
    assert reconcile_geo_ids(
        [{"geo_id": geo_id, "value": "100"}],
        [{"geo_id": "state:01|county:001", "county_fips": "001"}],
    ) == {"state:01|county:001"}


@pytest.mark.parametrize(
    ("observations", "features", "message"),
    [
        (
            [{"geo_id": "state:01|county:001"}],
            [{"geo_id": "state:1|county:1"}],
            "do not match",
        ),
        (
            [{"geo_id": "same"}, {"geo_id": "same"}],
            [{"geo_id": "same"}],
            "API observations contain duplicate",
        ),
        (
            [{"geo_id": "same"}],
            [{"geo_id": "same"}, {"geo_id": "same"}],
            "Tile features contain duplicate",
        ),
        ([{"value": 1}], [{"geo_id": "same"}], "Every API observation"),
        ([{"geo_id": "same"}], [{"county_fips": "001"}], "Every tile feature"),
    ],
)
def test_geo_id_reconciliation_rejects_mismatch_duplicates_and_fallbacks(
    observations, features, message: str
) -> None:
    """Covers: MARTIN-005 — invalid one-to-one geography joins fail clearly."""
    with pytest.raises(MartinContractError, match=message):
        reconcile_geo_ids(observations, features)
