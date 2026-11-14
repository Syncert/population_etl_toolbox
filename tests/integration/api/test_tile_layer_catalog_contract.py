"""The tile-layer catalog read against the real boundaries-by-vintage relation.

Covers the database-tier half of API-177 (tiles-places-and-metros). The rows
are fictional and dated in the 2090s so no other suite's boundaries can be
the default vintage while this runs.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

ROWS = [
    ("state:96|place:98761", "PLACE", 2091),
    ("state:96|place:98761", "PLACE", 2092),
    ("state:96|place:98762", "PLACE", 2092),
    ("cbsa:99961", "METRO", 2093),
]
SQUARE = "ST_Multi(ST_Transform(ST_MakeEnvelope(-150, 10, -149, 11, 4326), 3857))"


def _delete(cursor) -> None:
    cursor.execute(
        "DELETE FROM gold_glossary.tile_boundary_version WHERE geo_id = ANY(%s)",
        (sorted({row[0] for row in ROWS}),),
    )


@pytest.fixture
def client(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[TestClient]:
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _delete(cursor)
            for geo_id, level, vintage in ROWS:
                cursor.execute(
                    f"""
                    INSERT INTO gold_glossary.tile_boundary_version (
                        geo_id, geo_level, boundary_vintage, geo_name,
                        geometry_checksum, tile_geom
                    ) VALUES (%s, %s, %s, %s, %s, {SQUARE})
                    """,
                    (geo_id, level, vintage, f"Fixture {geo_id}", "0" * 64),
                )
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                _delete(cursor)
            cleanup.commit()
        finally:
            cleanup.close()


def test_the_catalog_lists_each_held_vintage_and_the_newest_as_default(
    client: TestClient,
) -> None:
    """Covers: API-177 — vintages and counts come from the warehouse, not a declaration."""
    response = client.get("/api/v1/catalog/tile-layers")
    assert response.status_code == 200
    layers = {layer["layer_id"]: layer for layer in response.json()["layers"]}

    places = layers["places"]
    assert places["available"] is True
    assert places["default_vintage"] == 2092
    assert places["tile_template"] == "/tiles/places/{z}/{x}/{y}?vintage=2092"
    held = {v["boundary_vintage"]: v for v in places["vintages"]}
    assert held[2092]["feature_count"] == 2
    assert held[2092]["is_default"] is True
    assert held[2091]["feature_count"] == 1
    assert held[2091]["is_default"] is False
    assert [v["boundary_vintage"] for v in places["vintages"]][:2] == [2092, 2091]

    metros = layers["metros"]
    assert metros["default_vintage"] == 2093
    assert metros["vintages"][0]["tile_template"] == (
        "/tiles/metros/{z}/{x}/{y}?vintage=2093"
    )
