"""Pure helpers for validating the Martin vector-tile serving contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urljoin, urlparse

CANONICAL_JOIN_KEY = "geo_id"
WORLD_BOUNDS_EPSG_3857 = (
    "-20037508.342789244,-20037508.342789244,20037508.342789244,20037508.342789244"
)


class MartinContractError(ValueError):
    """Raised when Martin metadata cannot satisfy the application contract."""


def normalize_base_url(url: str) -> str:
    """Return a directory-style HTTP(S) base URL."""
    value = str(url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise MartinContractError("Martin base URL must be an absolute HTTP(S) URL")
    return value.rstrip("/") + "/"


def tile_layer_url(tiles_base_url: str, layer_id: str) -> str:
    """Build the same-origin TileJSON URL for a configured layer."""
    layer = str(layer_id or "").strip().strip("/")
    if not layer or "/" in layer:
        raise MartinContractError("Martin layer ID must be one non-empty path segment")
    return urljoin(normalize_base_url(tiles_base_url), layer)


def extract_vector_layer(tilejson: object, layer_id: str) -> dict[str, Any]:
    """Return the exact requested vector-layer definition or fail clearly."""
    if not isinstance(tilejson, Mapping):
        raise MartinContractError("TileJSON must be an object")
    vector_layers = tilejson.get("vector_layers")
    if not isinstance(vector_layers, Sequence) or isinstance(
        vector_layers, (str, bytes)
    ):
        raise MartinContractError("TileJSON vector_layers must be a non-empty list")
    if not vector_layers:
        raise MartinContractError("TileJSON vector_layers must be a non-empty list")

    for layer in vector_layers:
        if isinstance(layer, Mapping) and layer.get("id") == layer_id:
            return dict(layer)
    raise MartinContractError(f"TileJSON does not contain vector layer {layer_id!r}")


def field_names(vector_layer: object) -> set[str]:
    """Return field names from either Martin-supported TileJSON representation."""
    if not isinstance(vector_layer, Mapping):
        raise MartinContractError("Vector layer must be an object")
    fields = vector_layer.get("fields")
    if isinstance(fields, Mapping):
        names = {str(key) for key in fields}
    elif isinstance(fields, Sequence) and not isinstance(fields, (str, bytes)):
        names = {str(item) for item in fields}
    else:
        raise MartinContractError("Vector layer fields must be an object or list")
    if not names:
        raise MartinContractError("Vector layer fields must not be empty")
    return names


def choose_join_key(fields: set[str]) -> str:
    """Return the actual spelling of canonical ``geo_id`` and reject fallbacks."""
    matches = [field for field in fields if field.lower() == CANONICAL_JOIN_KEY]
    if len(matches) != 1:
        raise MartinContractError("Vector layer must expose exactly one geo_id field")
    return matches[0]


def normalize_tile_template(template: str, tiles_base_url: str) -> str:
    """Normalize a TileJSON template onto the public same-origin tile base URL."""
    base = normalize_base_url(tiles_base_url)
    value = str(template or "").strip()
    if not value:
        raise MartinContractError("Tile template must not be empty")

    parsed_template = urlparse(value)
    if parsed_template.scheme and parsed_template.netloc:
        value = parsed_template.path
        if parsed_template.query:
            value += f"?{parsed_template.query}"

    base_path = urlparse(base).path.rstrip("/")
    if (
        value.startswith("/")
        and base_path
        and (value == base_path or value.startswith(base_path + "/"))
    ):
        value = value[len(base_path) :].lstrip("/")
    else:
        value = value.lstrip("/")
    return urljoin(base, value)


def sample_tile_url(
    tilejson: object,
    tiles_base_url: str,
    layer_id: str,
    *,
    z: int = 0,
    x: int = 0,
    y: int = 0,
) -> str:
    """Resolve one concrete same-origin tile URL from TileJSON or its fallback."""
    template = ""
    if isinstance(tilejson, Mapping):
        tiles = tilejson.get("tiles")
        if (
            isinstance(tiles, Sequence)
            and not isinstance(tiles, (str, bytes))
            and tiles
        ):
            template = str(tiles[0])
    if not template:
        template = f"{layer_id}/{{z}}/{{x}}/{{y}}"

    normalized = normalize_tile_template(template, tiles_base_url)
    return (
        normalized.replace("{z}", str(z))
        .replace("{x}", str(x))
        .replace("{y}", str(y))
        .replace("{bbox-epsg-3857}", WORLD_BOUNDS_EPSG_3857)
    )


def reconcile_geo_ids(
    observations: Sequence[Mapping[str, Any]],
    tile_features: Sequence[Mapping[str, Any]],
) -> set[str]:
    """Require an exact one-to-one ``geo_id`` join between API rows and tiles."""
    observation_ids = [str(item.get(CANONICAL_JOIN_KEY) or "") for item in observations]
    tile_ids = [str(item.get(CANONICAL_JOIN_KEY) or "") for item in tile_features]
    if not observation_ids or any(not value for value in observation_ids):
        raise MartinContractError("Every API observation must contain geo_id")
    if not tile_ids or any(not value for value in tile_ids):
        raise MartinContractError("Every tile feature must contain geo_id")
    if len(observation_ids) != len(set(observation_ids)):
        raise MartinContractError("API observations contain duplicate geo_id values")
    if len(tile_ids) != len(set(tile_ids)):
        raise MartinContractError("Tile features contain duplicate geo_id values")
    if set(observation_ids) != set(tile_ids):
        raise MartinContractError("API and tile geo_id values do not match exactly")
    return set(observation_ids)
