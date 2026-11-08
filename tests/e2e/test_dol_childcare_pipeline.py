"""Deterministic NDCP county childcare prices from a captured workbook to the API.

The fixture workbook holds the Women's Bureau's own rows with every column of
``NDCP2022.xlsx`` and is played through the adapter's own capture path by a
scripted client; nothing here reaches the network. The node proves what a
childcare price must not lose on the way to a consumer: its county, its
study year, its type of care and age group, and whether it was imputed.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import dol_childcare as ndcp
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "DOL_CHILDCARE"


@pytest.fixture
def ndcp_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ndcp.reviewed_warehouse(postgres_connection_factory, request)


def test_county_childcare_prices_reach_the_neutral_api_with_imputation_flags(
    ndcp_warehouse,
) -> None:
    """Covers: DB-073 — Dane County's center-based infant price for 2021 (imputed) and
    2022 reach `/api/v1/observations` with the workbook's values, and a blank cell
    arrives as a missing value, never zero.
    """
    factory = ndcp_warehouse
    ndcp.run_to_gold(factory)
    assert harvest_publisher(factory, Publisher("gold_dol_childcare")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]

        infant = client.get(
            "/api/v1/observations",
            params={"metric_code": f"{SOURCE_CODE}:center_infant", "geo_id": ndcp.DANE},
        )
        assert infant.status_code == 200, infant.text
        by_year = {
            int(item["period_start"][:4]): item for item in infant.json()["items"]
        }
        assert {year: by_year[year]["value"] for year in (2021, 2022)} == {
            2021: "299.75",
            2022: "293",
        }
        assert by_year[2022]["geo_level"] == "COUNTY"
        assert by_year[2021]["dimensions"]["imputed"] == "true"
        assert by_year[2021]["dimensions"]["imputation_code"] == "00001"
        assert by_year[2022]["dimensions"]["imputed"] == "false"
        assert by_year[2022]["dimensions"]["care_type"] == "center"

        blank = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:home_school_age",
                "geo_id": ndcp.ROCK,
                "year_from": 2022,
            },
        )
        assert blank.status_code == 200, blank.text
        (row,) = blank.json()["items"]
        assert row["value"] is None
        assert row["value_status"] == "missing"
