"""Deterministic BLS QCEW flow from captured slices to the API.

Every provider byte is a row copied verbatim from a published open-data
slice, played through the adapter's own capture path by a scripted client;
nothing here reaches the network. The node proves what QCEW must not lose
on the way to a consumer: the industry and ownership a value describes, the
establishment basis that keeps it apart from LAUS's residents, the month a
monthly employment figure belongs to, and a withheld cell that is never a
zero.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls_qcew.registry import TOTAL, get_industry
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import bls_qcew as qcew
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "BLS_QCEW"
PUBLISHER_SCHEMA = "gold_bls_qcew"
KENT = "state:10|county:001"


@pytest.fixture
def qcew_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return qcew.reviewed_warehouse(postgres_connection_factory, request)


def test_qcew_reaches_the_neutral_api_with_industry_ownership_and_basis(
    qcew_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered QCEW metric answers through
    `/api/v1/observations` with the fixture's values, industry and ownership.

    Covers: E2E-006 — a withheld cell carries no number and keeps its code.
    Covers: ETL-056 — monthly employment carries its month; the basis is on the row.
    """
    factory = qcew_warehouse
    qcew.run_to_gold(factory, 2024, "1", (TOTAL, get_industry("62")))
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 12

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert f"{SOURCE_CODE}:employment:10:0" in codes
        assert f"{SOURCE_CODE}:avg_weekly_wage:62:5" in codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        employment = client.get(
            "/api/v1/observations",
            params={"metric_code": f"{SOURCE_CODE}:employment:10:0", "geo_id": KENT},
        )
        assert employment.status_code == 200, employment.text
        rows = employment.json()["items"]
        assert [(row["period_start"], Decimal(row["value"])) for row in rows] == [
            ("2024-01-01", Decimal("69258")),
            ("2024-02-01", Decimal("69754")),
            ("2024-03-01", Decimal("70393")),
        ]
        first = rows[0]
        assert first["geo_level"] == "COUNTY"
        assert first["dimensions"]["industry_code"] == "10"
        assert first["dimensions"]["own_code"] == "0"
        assert first["dimensions"]["ownership_title"] == "Total covered"
        assert first["dimensions"]["observation_basis"].startswith(
            "establishment-based"
        )

        wage = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:avg_weekly_wage:62:5",
                "geo_level": "county",
            },
        ).json()
        assert wage["total"] == 4
        assert {row["dimensions"]["industry_title"] for row in wage["items"]} == {
            "Health care and social assistance"
        }

        everywhere = client.get(
            "/api/v1/observations",
            params={"metric_code": f"{SOURCE_CODE}:establishments:10:5", "limit": 100},
        ).json()["items"]
        withheld = [row for row in everywhere if row["value_status"] == "withheld"]
        for row in withheld:
            assert row["value"] is None
            assert row["dimensions"]["disclosure_code"] == "N"
        assert not [
            row
            for row in everywhere
            if row["value_status"] != "valid" and row["value"] is not None
        ]
