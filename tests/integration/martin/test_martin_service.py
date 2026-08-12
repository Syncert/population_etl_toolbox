"""Live Martin TileJSON, decoded MVT, proxy, and security contracts."""

from __future__ import annotations

from urllib.parse import urlparse

import httpx
import psycopg2
import pytest

from data_ingestion_toolbox.martin_contract import (
    choose_join_key,
    extract_vector_layer,
    field_names,
    normalize_tile_template,
)
from tests.support.martin import (
    MARTIN_IMAGE,
    MARTIN_TEST_PASSWORD,
    MARTIN_TEST_ROLE,
    MARTIN_VERSION,
    REPOSITORY_ROOT,
    SEEDED_GEO_ID,
    SEEDED_LATITUDE,
    SEEDED_LONGITUDE,
    MartinTestConfig,
    decode_mvt,
    docker,
    request_json,
    tile_for_coordinate,
)
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.martin]


def _tile_response(config: MartinTestConfig) -> httpx.Response:
    zoom, x, y = tile_for_coordinate(SEEDED_LONGITUDE, SEEDED_LATITUDE)
    response = httpx.get(f"{config.direct_url}/counties/{zoom}/{x}/{y}", timeout=5.0)
    response.raise_for_status()
    return response


def _decoded_county(config: MartinTestConfig) -> dict:
    decoded = decode_mvt(_tile_response(config).content)
    assert set(decoded) == {"counties"}
    features = decoded["counties"]["features"]
    assert len(features) == 1
    return features[0]


def test_live_tilejson_reports_seeded_counties_contract_within_budget(
    martin_test_config: MartinTestConfig,
) -> None:
    """Covers: MARTIN-006 — live pinned Martin exposes exact seeded metadata."""
    payload = request_json(f"{martin_test_config.direct_url}/counties", timeout=5.0)
    layer = extract_vector_layer(payload, "counties")
    fields = field_names(layer)

    assert layer["id"] == "counties"
    assert choose_join_key(fields) == "geo_id"
    assert set(fields) == {
        "geo_id",
        "geo_level",
        "state_fips",
        "county_fips",
        "state_name",
        "county_name",
        "latitude",
        "longitude",
    }
    assert payload["minzoom"] == 0
    assert payload["maxzoom"] == 12
    assert payload["bounds"] == [-180.0, -90.0, 180.0, 90.0]


def test_vector_tile_decodes_to_exact_polygon_and_properties(
    martin_test_config: MartinTestConfig,
) -> None:
    """Covers: MARTIN-007 — decoded MVT contains the one exact county feature."""
    feature = _decoded_county(martin_test_config)

    assert feature["geometry"]["type"] in {"Polygon", "MultiPolygon"}
    assert feature["geometry"]["coordinates"]
    assert feature["properties"] == {
        "geo_id": SEEDED_GEO_ID,
        "geo_level": "COUNTY",
        "state_fips": "55",
        "county_fips": "025",
        "state_name": "Wisconsin",
        "county_name": "Dane County",
        "latitude": pytest.approx(43.0667),
        "longitude": pytest.approx(-89.4),
    }


def test_same_origin_proxy_serves_health_tilejson_and_returned_tile(
    martin_test_config: MartinTestConfig,
) -> None:
    """Covers: MARTIN-009 — actual nginx proxy preserves every public tile route."""
    health = httpx.get(f"{martin_test_config.proxy_url}/health", timeout=5.0)
    assert health.status_code == 200

    tilejson_response = httpx.get(
        f"{martin_test_config.proxy_url}/counties", timeout=5.0
    )
    tilejson_response.raise_for_status()
    tilejson = tilejson_response.json()
    template = normalize_tile_template(
        tilejson["tiles"][0], martin_test_config.proxy_url
    )
    assert urlparse(template).path.startswith("/tiles/")
    assert "martin" not in tilejson_response.text.lower()
    assert "postgres" not in tilejson_response.text.lower()

    zoom, x, y = tile_for_coordinate(SEEDED_LONGITUDE, SEEDED_LATITUDE)
    public_url = (
        template.replace("{z}", str(zoom)).replace("{x}", str(x)).replace("{y}", str(y))
    )
    tile = httpx.get(public_url, timeout=5.0)
    tile.raise_for_status()
    assert decode_mvt(tile.content)["counties"]["features"]


def test_runtime_image_version_and_read_only_role_are_enforced(
    martin_test_config: MartinTestConfig,
    postgres_test_config: PostgresTestConfig,
) -> None:
    """Covers: MARTIN-010 — pinned runtime matches and serving role cannot mutate."""
    inspected = docker(
        "inspect",
        "--format",
        "{{.Config.Image}}",
        martin_test_config.container_name,
    )
    assert inspected.returncode == 0, inspected.stderr
    assert inspected.stdout.strip() == MARTIN_IMAGE

    version = docker("exec", martin_test_config.container_name, "martin", "--version")
    assert version.returncode == 0, version.stderr
    assert MARTIN_VERSION in version.stdout

    reader = psycopg2.connect(
        host=postgres_test_config.host,
        port=postgres_test_config.port,
        user=MARTIN_TEST_ROLE,
        password=MARTIN_TEST_PASSWORD,
        dbname=postgres_test_config.database,
        connect_timeout=5,
    )
    try:
        with reader.cursor() as cursor:
            cursor.execute("SELECT geo_id FROM gold.dim_geo_latest")
            assert cursor.fetchall() == [(SEEDED_GEO_ID,)]
            with pytest.raises(psycopg2.Error):
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_geo_latest WHERE geo_id = %s",
                    (SEEDED_GEO_ID,),
                )
        reader.rollback()
    finally:
        reader.close()


def test_missing_relation_failure_is_clear_and_does_not_disclose_dsn(
    martin_test_config: MartinTestConfig,
) -> None:
    """Covers: MARTIN-010 — invalid relation startup fails clearly and sanitizes secrets."""
    config_path = REPOSITORY_ROOT / "tests/fixtures/martin/missing_relation.yml"
    secret = MARTIN_TEST_PASSWORD
    result = docker(
        "run",
        "--rm",
        "--network",
        martin_test_config.docker_network,
        "--env",
        f"MARTIN_DATABASE_URL=postgres://martin_test:{secret}@postgres:5432/population_etl_test",
        "--volume",
        f"{config_path.resolve()}:/etc/martin/missing.yml:ro",
        MARTIN_IMAGE,
        "--config",
        "/etc/martin/missing.yml",
        timeout=12.0,
    )
    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode != 0
    assert "relation_that_does_not_exist" in output or "missing_counties" in output
    assert secret not in output
    assert "postgres://" not in output
