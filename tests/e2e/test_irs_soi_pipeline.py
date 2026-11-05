"""Deterministic IRS SOI county and state income from captured files to the API.

Every provider byte is a row copied verbatim from SOI's published files,
played through the adapter's own capture path by a scripted client; nothing
here reaches the network. The node proves what an income value must not
lose on the way to a consumer: its tax year, its AGI band, the county file
preferred where it and the state file both speak, and the nation answered
by the state file alone.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import irs_soi as soi
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "IRS_SOI"


@pytest.fixture
def soi_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return soi.reviewed_warehouse(postgres_connection_factory, request)


def _rows(factory: Callable[[], connection], sql: str, parameters: tuple) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_county_and_state_income_reach_the_neutral_api_by_agi_band(
    soi_warehouse,
) -> None:
    """Covers: ETL-114 — county returns from 1989 and 2023, a 2023 AGI band and the
    nation reach `/api/v1/observations` with the fixture's values.

    Covers: E2E-014 — a glossary-discovered SOI metric answers through the
    neutral observation route.
    """
    factory = soi_warehouse
    for key in (
        "county_total:1989",
        "county_total:2010",
        "county_agi:2010",
        "county_total:2023",
        "county_agi:2023",
        "state:2023",
    ):
        soi.run_to_gold(factory, key)
    assert harvest_publisher(factory, Publisher("gold_irs_soi")) > 0
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_irs_soi.observation_revision
        WHERE geo_id = %s AND metric_key = 'returns' AND tax_year = 2023
        """,
        (soi.DELAWARE,),
    ) == [(2,)]
    assert _rows(
        factory,
        """
        SELECT product FROM gold_irs_soi.observation_latest
        WHERE geo_id = %s AND metric_key = 'returns' AND tax_year = 2023
        """,
        (soi.DELAWARE,),
    ) == [("county",)]

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]

        returns = client.get(
            "/api/v1/observations",
            params={"metric_code": f"{SOURCE_CODE}:returns", "geo_id": soi.KENT},
        )
        assert returns.status_code == 200, returns.text
        by_year = {
            int(item["period_start"][:4]): item for item in returns.json()["items"]
        }
        assert {year: by_year[year]["value"] for year in (1989, 2023)} == {
            1989: "48600",
            2023: "89390.0000",
        }
        assert by_year[2023]["geo_level"] == "COUNTY"
        assert by_year[2023]["dimensions"]["agi_band"] == "all"

        band = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:returns:agi_200k_plus",
                "geo_id": soi.KENT,
            },
        )
        assert band.status_code == 200, band.text
        items = {int(item["period_start"][:4]): item for item in band.json()["items"]}
        assert items[2023]["value"] == "3330.0000"
        assert items[2023]["dimensions"]["band_label"]

        nation = client.get(
            "/api/v1/observations",
            params={"metric_code": f"{SOURCE_CODE}:returns", "geo_id": soi.NATION},
        )
        assert nation.status_code == 200, nation.text
        (national,) = nation.json()["items"]
        assert national["value"] == "159949000"
        assert national["geo_level"] == "NATIONAL"
        assert national["dimensions"]["product"] == "state"

        # The plan's two derived rates, each side from the same SOI file.
        for ratio_id, expected in (
            ("income_tax_share_of_agi", Decimal(580723) / Decimal(6057153) * 100),
            ("eitc_share_of_returns", Decimal(15310) / Decimal(89390) * 100),
        ):
            derived = client.get(
                "/api/v1/observations/derived",
                params={"ratio_id": ratio_id, "geo_id": soi.KENT, "year_from": 2023},
            )
            assert derived.status_code == 200, derived.text
            (row,) = derived.json()["items"]
            assert row["derived"] and row["unavailable_reason"] is None
            assert abs(Decimal(row["value"]) - expected) < Decimal("0.01"), row
