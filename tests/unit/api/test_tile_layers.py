"""The tile-layer catalog: every layer a map can draw, and its vintages.

Covers: API-177 (tiles-places-and-metros)
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import PUBLIC_CACHE_TARGETS, app
from apps.api.ratelimit import RateLimitMiddleware
from apps.api.services.tile_layers import TILE_LAYERS, UNCOLOURED_RULE
from data_ingestion_toolbox.sql.catalog_queries import TILE_LAYER_VINTAGES_QUERY

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PATH = "/api/v1/catalog/tile-layers"


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute(self, query, params=None):
        self.queries.append(query)
        assert query is TILE_LAYER_VINTAGES_QUERY
        return _Result(self.rows)


def _get(session):
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        return TestClient(app).get(PATH)
    finally:
        app.dependency_overrides.clear()


HELD = [
    {
        "geo_level": "METRO",
        "boundary_vintage": 2023,
        "feature_count": 925,
        "is_default": True,
    },
    {
        "geo_level": "METRO",
        "boundary_vintage": 2020,
        "feature_count": 939,
        "is_default": False,
    },
    {
        "geo_level": "PLACE",
        "boundary_vintage": 2024,
        "feature_count": 32350,
        "is_default": True,
    },
    {
        "geo_level": "PLACE",
        "boundary_vintage": 2010,
        "feature_count": 29514,
        "is_default": False,
    },
]


def test_every_layer_is_listed_with_the_vintages_it_holds() -> None:
    """Covers: API-177 — versioned layers list each vintage and its own template."""
    response = _get(_Session(HELD))
    assert response.status_code == 200
    body = response.json()
    layers = {layer["layer_id"]: layer for layer in body["layers"]}
    assert list(layers) == ["counties", "tracts", "places", "metros"]
    assert body["uncoloured_rule"] == UNCOLOURED_RULE
    assert "never drawn as zero" in body["uncoloured_rule"]

    counties = layers["counties"]
    assert counties["grains"] == ["STATE", "COUNTY"]
    assert counties["versioned"] is False
    assert counties["vintages"] == []
    assert counties["available"] is True
    assert counties["tile_template"] == "/tiles/counties/{z}/{x}/{y}"

    places = layers["places"]
    assert places["grains"] == ["PLACE"]
    assert places["versioned"] is True
    assert places["default_vintage"] == 2024
    assert places["tile_template"] == "/tiles/places/{z}/{x}/{y}?vintage=2024"
    assert [v["boundary_vintage"] for v in places["vintages"]] == [2024, 2010]
    assert places["vintages"][1] == {
        "boundary_vintage": 2010,
        "feature_count": 29514,
        "is_default": False,
        "tile_template": "/tiles/places/{z}/{x}/{y}?vintage=2010",
    }
    metros = layers["metros"]
    assert metros["default_vintage"] == 2023
    assert metros["name_field"] == "metro_name"
    assert all(layer["join_key"] == "geo_id" for layer in layers.values())


def test_a_versioned_layer_with_no_boundary_says_it_is_unavailable() -> None:
    """Covers: API-177 — an empty layer is reported, not offered as a blank map."""
    body = _get(_Session([])).json()
    layers = {layer["layer_id"]: layer for layer in body["layers"]}
    for layer_id in ("places", "metros"):
        assert layers[layer_id]["available"] is False
        assert layers[layer_id]["vintages"] == []
        assert layers[layer_id]["default_vintage"] is None
        assert (
            layers[layer_id]["tile_template"] == f"/tiles/{layer_id}/{{z}}/{{x}}/{{y}}"
        )
    assert layers["tracts"]["available"] is True


def test_the_declared_layers_are_the_ones_martin_serves() -> None:
    """Covers: API-177 — the API's declaration and martin.yml agree on id, zoom and kind."""
    postgres = yaml.safe_load(
        (REPOSITORY_ROOT / "infra/martin/martin.yml").read_text(encoding="utf-8")
    )["postgres"]
    served = {
        **{key: (False, value) for key, value in postgres["tables"].items()},
        **{key: (True, value) for key, value in postgres["functions"].items()},
    }
    declared = {layer.layer_id: layer for layer in TILE_LAYERS}
    assert set(declared) == set(served)
    for layer_id, (versioned, source) in served.items():
        layer = declared[layer_id]
        assert layer.versioned is versioned
        assert (layer.minzoom, layer.maxzoom) == (source["minzoom"], source["maxzoom"])
        if not versioned:
            assert layer.name_field in source["properties"]


def test_the_route_is_cached_and_spends_the_catalog_budget() -> None:
    """Covers: API-177 — a public cache target in the catalog bucket."""
    assert PUBLIC_CACHE_TARGETS.covers(PATH)
    limiter = RateLimitMiddleware(
        app=None, catalog_per_minute=60, analysis_per_minute=10
    )
    assert limiter._classify(PATH) == ("catalog", 60)
    assert _get(_Session(HELD)).headers["cache-control"].startswith("public, max-age=")
