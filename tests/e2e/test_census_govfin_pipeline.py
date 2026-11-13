"""Deterministic Census government finance flow from captured files to the API.

Every provider byte is a checked-in Census file or Data API answer, played
through the adapter's own capture path by a scripted client; nothing here
reaches the network. The node proves what a tax figure must not lose on the
way to a consumer: a county area's figure says it is the warehouse's sum of
Census's government records, which records it sums, and how many
governments; a state figure is Census's own; and a fiscal year is dated by
the calendar year it ends in.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import census_govfin
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CENSUS_GOVFIN"
PUBLISHER_SCHEMA = "gold_census_govfin"
DANE = "state:55|county:025"
WISCONSIN = "state:55"
#: Wisconsin's state and local taxes for fiscal 2022 in ``asslgf_2022_state.json``
#: (AGG_DESC LF0008, GOVTYPE 001), in thousands of dollars.
WISCONSIN_TAXES_2022 = Decimal(35_077_588)


@pytest.fixture
def govfin_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return census_govfin.reviewed_warehouse(postgres_connection_factory, request)


def test_county_area_and_state_taxes_reach_the_neutral_api(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-118 — a glossary-discovered county-area roll-up and a state
    figure answer through `/api/v1/observations` with their derivation, and a
    replay of the same captures leaves the answer unchanged.
    """
    factory = govfin_warehouse
    client = census_govfin.FixtureClient(
        {**census_govfin.unit_archives(), **census_govfin.api_responses()}
    )
    census_govfin.run_to_gold(factory, "cog_units", census_govfin.UNIT_YEAR, client)
    census_govfin.run_to_gold(factory, "asslgf", 2022, client)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) > 0

    county_code = f"{SOURCE_CODE}:cog_units:taxes_total:local"
    state_code = f"{SOURCE_CODE}:asslgf:taxes_total:state_and_local"
    with real_api_client() as api:
        catalog = api.get(
            "/api/v1/catalog/metrics",
            # By code: the spending lines put the source's catalog past one
            # page, and the two metrics this node reads are what it checks.
            params={
                "source_code": SOURCE_CODE,
                "metric_codes": f"{county_code},{state_code}",
            },
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {county_code, state_code} <= codes

        capabilities = api.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        county = api.get(
            "/api/v1/observations", params={"metric_code": county_code, "geo_id": DANE}
        )
        assert county.status_code == 200, county.text
        (item,) = county.json()["items"]
        assert Decimal(item["value"]) == Decimal(census_govfin.DANE_TAXES_TOTAL)
        assert item["geo_level"] == "COUNTY"
        assert item["period_start"] == "2022-01-01"
        assert item["dimensions"]["derivation"] == "county_area_rollup"
        assert item["dimensions"]["record_coverage"] == "reported_and_imputed"
        assert item["dimensions"]["government_count"] == str(
            census_govfin.DANE_GOVERNMENT_COUNT
        )
        assert item["dimensions"]["period_label"] == "fiscal year ending in 2022"

        state = api.get(
            "/api/v1/observations",
            params={"metric_code": state_code, "geo_id": WISCONSIN},
        ).json()
        (state_item,) = state["items"]
        assert Decimal(state_item["value"]) == WISCONSIN_TAXES_2022
        assert state_item["geo_level"] == "STATE"
        assert state_item["dimensions"]["derivation"] == "census_published"

        census_govfin.run_to_gold(factory, "cog_units", census_govfin.UNIT_YEAR, client)
        again = api.get(
            "/api/v1/observations", params={"metric_code": county_code, "geo_id": DANE}
        ).json()
        assert [row["value"] for row in again["items"]] == [item["value"]]
