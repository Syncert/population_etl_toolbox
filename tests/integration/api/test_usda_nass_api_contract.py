"""USDA NASS API routes exercised against the actual gold warehouse schema.

The unit tier proves filter binding and response shape against a session
double. This module proves the SQL itself: that every column the service reads
exists in the published views and that the contract a consumer sees matches the
rows the pipeline actually produced.
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
from tests.integration.database.test_usda_nass_pipeline import _fixture, _run_to_gold
from tests.support import usda_nass as nass_support
from tests.support.postgres import PostgresTestConfig
from data_ingestion_toolbox.usda_nass.registry import get_product

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


@pytest.fixture
def published_nass_api(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Iterator[TestClient]:
    """Publish the reviewed corn releases and serve them through the API."""
    connection_factory = nass_support.reviewed_warehouse(
        postgres_connection_factory, request
    )
    product = get_product("corn_survey_annual")
    _run_to_gold(connection_factory, product, _fixture(product.product_id))
    _run_to_gold(connection_factory, product, _fixture("corn_survey_annual_revised"))

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


def test_observations_read_the_published_views_with_exact_semantics(
    published_nass_api: TestClient,
) -> None:
    """Covers: API-013 — the observation SQL matches the published views."""
    response = published_nass_api.get(
        "/api/v1/usda-nass/observations",
        params={"commodity_desc": "CORN", "limit": 500},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] > 0
    assert body["release_scope"] == "as_released"

    items = body["items"]
    assert items
    for item in items:
        assert item["unit_desc"]
        assert item["source_desc"] in {"SURVEY", "CENSUS"}
        assert item["value_status"]
        if item["value_status"] == "valid":
            assert item["value"] is not None
        else:
            assert item["value"] is None
            assert item["value_source"]

    # Both releases are visible as released, and units never mix within a
    # single data item.
    watermarks = {item["release_watermark"] for item in items}
    assert len(watermarks) == 2
    units_by_item: dict[str, set[str]] = {}
    for item in items:
        units_by_item.setdefault(item["short_desc"], set()).add(item["unit_desc"])
    assert all(len(units) == 1 for units in units_by_item.values())


def test_latest_release_hides_superseded_values_without_deleting_them(
    published_nass_api: TestClient,
) -> None:
    """Covers: API-013 — latest and as-released differ on a revised release."""
    latest = published_nass_api.get(
        "/api/v1/usda-nass/observations",
        params={"commodity_desc": "CORN", "latest": "true", "limit": 500},
    ).json()
    as_released = published_nass_api.get(
        "/api/v1/usda-nass/observations",
        params={"commodity_desc": "CORN", "limit": 500},
    ).json()

    assert latest["release_scope"] == "latest"
    assert latest["total"] < as_released["total"]
    assert len({item["release_watermark"] for item in latest["items"]}) == 1


def test_geography_and_period_filters_narrow_the_published_result(
    published_nass_api: TestClient,
) -> None:
    """Covers: API-013 — multidimensional filters reach the real views."""
    county = published_nass_api.get(
        "/api/v1/usda-nass/observations",
        params={
            "agg_level_desc": "COUNTY",
            "geo_id": "state:01|county:001",
            "statisticcat_desc": "YIELD",
            "year_start": 2024,
            "year_end": 2024,
            "limit": 500,
        },
    ).json()

    assert county["total"] > 0
    for item in county["items"]:
        assert item["agg_level_desc"] == "COUNTY"
        assert item["geo_id"] == "state:01|county:001"
        assert item["statisticcat_desc"] == "YIELD"
        assert item["year"] == 2024
        assert item["additive_behavior"] == "non_additive"


def test_series_and_measures_read_the_published_views(
    published_nass_api: TestClient,
) -> None:
    """Covers: API-013 — series and measure SQL match the published views."""
    series = published_nass_api.get(
        "/api/v1/usda-nass/series", params={"commodity_desc": "CORN", "limit": 500}
    ).json()
    assert series["total"] > 0
    for item in series["items"]:
        assert item["series_id"]
        assert item["unit_desc"]
        assert (
            item["numeric_observation_count"] + item["non_numeric_observation_count"]
            == item["observation_count"]
        )

    measures = published_nass_api.get("/api/v1/usda-nass/measures").json()
    assert measures["total"] > 0
    assert all(item["unit"] for item in measures["items"])
