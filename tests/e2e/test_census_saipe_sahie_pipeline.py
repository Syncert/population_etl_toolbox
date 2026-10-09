"""Deterministic Census SAIPE/SAHIE flow from captured responses to the API.

Every provider byte is a checked-in Census Data API response, played through
the adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what these model-based estimates must not lose on the
way to a consumer: the 90 percent interval travels with the value, the row
says it is a model-based annual estimate rather than a survey estimate, and
the metric identity is the dataset's own, never an ACS table's.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_saipe_sahie.registry import SAHIE, SAIPE
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import census_sae
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CENSUS_SAIPE_SAHIE"
PUBLISHER_SCHEMA = "gold_census_sae"
#: Kent County, Delaware.
COUNTY = "state:10|county:001"


@pytest.fixture
def sae_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return census_sae.reviewed_warehouse(postgres_connection_factory, request)


def _fixture_row(dataset_id: str, county: str) -> dict[str, object]:
    rows = census_sae.fixture_rows(dataset_id, "county")
    header = rows[0]
    return next(
        dict(zip(header, row))
        for row in rows[1:]
        if row[header.index("county")] == county
    )


def test_saipe_and_sahie_reach_the_neutral_api_with_their_intervals(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: E2E-014 — a glossary-discovered SAIPE and SAHIE metric answers
    through `/api/v1/observations` with the fixture's value and interval.

    Covers: E2E-004 — replaying the same captures adds no estimate and the
        API answers identical JSON.
    Covers: ETL-054 — the row carries the model-based basis and the bounds.
    """
    factory = sae_warehouse
    for dataset in (SAIPE, SAHIE):
        census_sae.run_to_gold(factory, dataset)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 7

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {f"{SOURCE_CODE}:saipe:SAEMHI", f"{SOURCE_CODE}:sahie:PCTUI"} <= codes
        assert not [code for code in codes if code.startswith("CENSUS_ACS:")]

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True
        assert "/api/v1/observations" in {
            route["path"] for route in entry["observation_routes"]
        }

        answers = {}
        for dataset_id, measure_id in (("saipe", "SAEMHI"), ("sahie", "PCTUI")):
            response = client.get(
                "/api/v1/observations",
                params={
                    "metric_code": f"{SOURCE_CODE}:{dataset_id}:{measure_id}",
                    "geo_id": COUNTY,
                },
            )
            assert response.status_code == 200, response.text
            payload = response.json()
            answers[dataset_id] = payload
            assert payload["source_code"] == SOURCE_CODE
            (item,) = payload["items"]
            expected = _fixture_row(dataset_id, "001")
            assert Decimal(item["value"]) == Decimal(str(expected[f"{measure_id}_PT"]))
            assert Decimal(item["uncertainty"]["confidence_lower"]) == Decimal(
                str(expected[f"{measure_id}_LB90"])
            )
            assert Decimal(item["uncertainty"]["confidence_upper"]) == Decimal(
                str(expected[f"{measure_id}_UB90"])
            )
            assert Decimal(item["uncertainty"]["margin_of_error"]) == Decimal(
                str(expected[f"{measure_id}_MOE"])
            )
            assert item["value_status"] == "valid"
            assert item["geo_level"] == "COUNTY"
            assert item["dimensions"]["estimate_method"].startswith(
                "model-based annual estimate"
            )
            assert item["period_start"] == "2023-01-01"

        # The county filter by grain answers all three Delaware counties.
        by_grain = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:saipe:SAEPOVRTALL",
                "geo_level": "county",
            },
        ).json()
        assert by_grain["total"] == 3

        # Replay: the same bytes add no estimate, and the answer is unchanged.
        census_sae.run_to_gold(factory, SAIPE)
        again = client.get(
            "/api/v1/observations",
            params={"metric_code": f"{SOURCE_CODE}:saipe:SAEMHI", "geo_id": COUNTY},
        ).json()
        assert [item["value"] for item in again["items"]] == [
            item["value"] for item in answers["saipe"]["items"]
        ]
        assert [item["uncertainty"] for item in again["items"]] == [
            item["uncertainty"] for item in answers["saipe"]["items"]
        ]
