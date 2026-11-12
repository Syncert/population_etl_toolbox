"""Deterministic USAspending award obligations flow from captured answers to the API.

Every provider byte is USAspending's own fiscal 2024 answer, played through
the adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what a place-of-performance amount must keep on the
way to a consumer: the obligation as published, the October-to-September
fiscal year, and the state or county USAspending recorded it in.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import usaspending as usa
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "USASPENDING"


@pytest.fixture
def usaspending_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return usa.reviewed_warehouse(postgres_connection_factory, request)


def test_place_obligations_reach_the_neutral_api_as_published(
    usaspending_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered USAspending metric answers through
    `/api/v1/observations` for a state and a county with the fixture's values.

    Covers: ETL-132 — the value is the obligation as published, over the
    October-to-September fiscal year, at the place of performance.
    """
    factory = usaspending_warehouse
    usa.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_usaspending")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"]
        state = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:federal_award_obligations",
                "geo_id": usa.WISCONSIN,
            },
        )
        assert state.status_code == 200, state.text
        (row,) = state.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"]) == (
            usa.WI_FY2024_ALL_AWARDS,
            "US dollars",
            "STATE",
        )
        assert (row["period_start"], row["period_end"]) == ("2023-10-01", "2024-09-30")

        county = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:federal_award_obligations_grants",
                "geo_id": usa.DANE,
            },
        )
        assert county.status_code == 200, county.text
        assert [
            (item["value"], item["geo_level"]) for item in county.json()["items"]
        ] == [(usa.DANE_FY2024_GRANTS, "COUNTY")]
