"""Real API observation to decoded Martin county feature contract."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from data_ingestion_toolbox.martin_contract import reconcile_geo_ids
from tests.support.martin import (
    SEEDED_GEO_ID,
    SEEDED_LATITUDE,
    SEEDED_LONGITUDE,
    MartinTestConfig,
    decode_mvt,
    tile_for_coordinate,
)
from tests.support.postgres import PostgresTestConfig

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.database,
    pytest.mark.martin,
    pytest.mark.slow,
]

METRIC_CODE = "ACS:acs5:B01003_001_MARTIN_TEST"


def test_real_api_county_joins_one_to_one_to_decoded_tile_and_rejects_mismatch(
    postgres_test_config: PostgresTestConfig,
) -> None:
    """Covers: MARTIN-008 — real API and decoded MVT reconcile only by exact geo_id."""
    martin_config = MartinTestConfig.from_environment()
    assert martin_config is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": postgres_test_config.host,
            "port": postgres_test_config.port,
            "user": postgres_test_config.user,
            "password": postgres_test_config.password,
            "dbname": postgres_test_config.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        response = TestClient(app).get(
            "/api/v1/census/observations/latest",
            params={"metric_code": METRIC_CODE, "geo_level": "COUNTY"},
        )
    finally:
        app.dependency_overrides.clear()
        engine.dispose()

    assert response.status_code == 200
    assert response.json()["total"] == 1
    api_rows = response.json()["items"]
    assert [item["geo_id"] for item in api_rows] == [SEEDED_GEO_ID]

    zoom, x, y = tile_for_coordinate(SEEDED_LONGITUDE, SEEDED_LATITUDE)
    tile_response = httpx.get(
        f"{martin_config.direct_url}/counties/{zoom}/{x}/{y}", timeout=5.0
    )
    tile_response.raise_for_status()
    decoded = decode_mvt(tile_response.content)
    tile_rows = [feature["properties"] for feature in decoded["counties"]["features"]]
    assert reconcile_geo_ids(api_rows, tile_rows) == {SEEDED_GEO_ID}
    with pytest.raises(ValueError, match="do not match exactly"):
        reconcile_geo_ids(api_rows, [{"geo_id": "state:55|county:999"}])
