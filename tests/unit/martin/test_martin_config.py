"""Deterministic Martin configuration and cross-surface contracts."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MARTIN_CONFIG_PATH = REPOSITORY_ROOT / "infra/martin/martin.yml"
EXPECTED_PROPERTIES = {
    "geo_id": "text",
    "geo_level": "text",
    "state_fips": "text",
    "county_fips": "text",
    "state_name": "text",
    "county_name": "text",
    "latitude": "float8",
    "longitude": "float8",
}


def _martin_config() -> dict:
    return yaml.safe_load(MARTIN_CONFIG_PATH.read_text(encoding="utf-8"))


def test_martin_counties_layer_maps_the_authoritative_geography_contract() -> None:
    """Covers: MARTIN-001 — Martin publishes the exact county layer contract."""
    config = _martin_config()
    postgres = config["postgres"]
    tables = postgres["tables"]

    assert config["listen_addresses"] == "0.0.0.0:3000"
    assert config["base_path"] == "/tiles"
    assert postgres["connection_string"] == "${MARTIN_DATABASE_URL}"
    assert postgres["auto_publish"] is False
    assert set(tables) == {"counties"}

    counties = tables["counties"]
    assert counties == {
        "layer_id": "counties",
        "schema": "gold",
        "table": "dim_geo_latest",
        "geometry_column": "geo_geom",
        "id_column": None,
        "srid": 4326,
        "minzoom": 0,
        "maxzoom": 12,
        "bounds": [-180.0, -90.0, 180.0, 90.0],
        "properties": EXPECTED_PROPERTIES,
    }


def test_martin_paths_and_relation_are_consistent_across_runtime_surfaces() -> None:
    """Covers: MARTIN-002 — runtime, proxy, and documentation surfaces agree."""
    config = _martin_config()
    counties = config["postgres"]["tables"]["counties"]
    relation = f"{counties['schema']}.{counties['table']}"
    assert relation == "gold.dim_geo_latest"

    internal_compose = (REPOSITORY_ROOT / "infra/docker/docker-compose.yml").read_text(
        encoding="utf-8"
    )
    external_compose = (
        REPOSITORY_ROOT / "infra/docker/docker-compose.external.yml"
    ).read_text(encoding="utf-8")
    next_config = (REPOSITORY_ROOT / "apps/web/next.config.mjs").read_text(
        encoding="utf-8"
    )
    nginx_config = (REPOSITORY_ROOT / "infra/web/nginx.conf").read_text(
        encoding="utf-8"
    )
    martin_readme = (REPOSITORY_ROOT / "infra/martin/README.md").read_text(
        encoding="utf-8"
    )
    docker_readme = (REPOSITORY_ROOT / "infra/docker/README.md").read_text(
        encoding="utf-8"
    )

    for compose in (internal_compose, external_compose):
        assert "command: --config /etc/martin/martin.yml" in compose
        assert "../martin/martin.yml:/etc/martin/martin.yml:ro" in compose
        assert "127.0.0.1:${MARTIN_HOST_PORT:-3000}:3000" in compose
        assert "TILES_ORIGIN: http://martin:3000" in compose
        assert "MARTIN_DATABASE_URL:" in compose

    assert 'source: "/tiles/:path*"' in next_config
    assert "destination: `${tilesOrigin}/:path*`" in next_config
    assert "location /tiles/" in nginx_config
    assert "proxy_pass http://martin:3000/" in nginx_config
    assert "gold.dim_geo_latest" in martin_readme
    assert "gold.dim_geo_latest.geo_geom" in docker_readme
    assert "`counties`" in docker_readme
    assert "gold_glossary.dim_geo_latest" not in martin_readme


def test_martin_configuration_has_no_implicit_table_publication() -> None:
    """Covers: MARTIN-001 — no undeclared database relation is auto-published."""
    postgres = _martin_config()["postgres"]
    assert postgres["auto_publish"] is False
    assert list(postgres["tables"]) == ["counties"]
