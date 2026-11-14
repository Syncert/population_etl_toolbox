"""Deterministic CDC FluView ILINet flow from captured downloads to the API.

Every provider byte is CDC's own: FluView's init document and the national,
HHS-region and state ILINet downloads read on 2026-11-12 (trimmed to a few
weeks), played through the adapter's own capture path by a scripted client;
nothing here reaches the network. The node proves what a consumer must be
able to read off a state's weekly figure: CDC's value, the MMWR week it
belongs to, and CDC's own text where it does not publish a figure.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import cdc_ilinet as ilinet
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CDC_ILINET"
PUBLISHER_SCHEMA = "gold_cdc_ilinet"
UNWEIGHTED = f"{SOURCE_CODE}:ilinet:unweighted_ili_pct"
WEIGHTED = f"{SOURCE_CODE}:ilinet:weighted_ili_pct"


@pytest.fixture
def ilinet_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ilinet.reviewed_warehouse(postgres_connection_factory, request)


def test_wisconsin_weekly_ili_reaches_the_neutral_api(
    ilinet_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-136 — a glossary-discovered ILINet figure answers through
    `/api/v1/observations` for Wisconsin with CDC's percentage for each MMWR
    week, and CDC's `X` where it does not publish the weighted figure.

    Covers: E2E-004 — reading the frozen downloads again adds no row and the
        API answers identical JSON.
    """
    factory = ilinet_warehouse
    ilinet.load_release(factory)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 11

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {UNWEIGHTED, WEIGHTED, f"{SOURCE_CODE}:ilinet:ili_visits_total"} <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        params = {
            "metric_code": UNWEIGHTED,
            "geo_id": ilinet.WISCONSIN,
            "year_from": 2024,
            "year_to": 2025,
            "limit": 100,
        }
        weekly = client.get("/api/v1/observations", params=params)
        assert weekly.status_code == 200, weekly.text
        items = weekly.json()["items"]
        assert [item["period_start"][:10] for item in items] == [
            "2024-12-29",
            "2025-01-05",
        ]
        first = items[0]
        assert float(first["value"]) == pytest.approx(3.04037)
        assert first["period_end"][:10] == "2025-01-04"
        assert first["geo_level"] == "STATE"
        assert first["dimensions"]["mmwr_year"] == "2025"
        assert first["dimensions"]["mmwr_week"] == "1"
        assert first["dimensions"]["value_source"] == "3.04037"

        weighted = client.get(
            "/api/v1/observations", params={**params, "metric_code": WEIGHTED}
        ).json()["items"]
        assert weighted and all(item["value"] is None for item in weighted)
        assert {item["dimensions"]["value_source"] for item in weighted} == {"X"}

        assert ilinet.load_release(factory)[0] is None
        repeat = client.get("/api/v1/observations", params=params).json()
        assert repeat["items"] == items
