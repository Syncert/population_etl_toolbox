"""Deterministic USDA NASS crop flow from reviewed Quick Stats slices to the API.

Every provider byte is a checked-in reviewed slice; nothing here reaches the
network. The node proves what NASS specifically must not lose on the way to a
consumer: a withheld value is not zero and keeps its exact provider symbol, a
survey estimate is not a census count even under an identical label, a
coefficient of variation travels with the value it qualifies, and acres,
bushels, and bushels-per-acre never share one series identity.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.usda_nass.registry import enabled_products, get_product
from tests.support import usda_nass as nass_support
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "USDA_NASS"
PUBLISHER_SCHEMA = "gold_nass"
REVISED_PRODUCT = "corn_survey_annual"
#: Loving County, Texas: withheld in the first release, published in the revision.
REVISED_GEO_ID = "state:48|county:301"


@pytest.fixture
def nass_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the geographies the reviewed fixtures resolve against."""
    return nass_support.reviewed_warehouse(postgres_connection_factory, request)


def _query(
    connection_factory: Callable[[], connection],
    sql: str,
    parameters: tuple[object, ...] = (),
) -> list[tuple]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchall()
    finally:
        database_connection.close()


def _observation(items: list[dict], **match: object) -> dict:
    matched = [
        item
        for item in items
        if all(item[name] == value for name, value in match.items())
    ]
    assert len(matched) == 1, f"expected exactly one item for {match}, got {matched}"
    return matched[0]


def test_nass_fixtures_reach_the_api_without_losing_source_classification(
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: E2E-011 — NASS replays raw-to-API with exact source semantics.

    Covers: E2E-004 — replaying a captured release publishes the same rows and
        returns identical API JSON.
    Covers: E2E-005 — a revised release advances the latest projection while
        the release it supersedes stays queryable as released.
    Covers: E2E-006 — a withheld value keeps its provider symbol and a null
        number rather than becoming a zero.
    Covers: E2E-014 — a glossary-discovered NASS metric answers through the
        registry-dispatched neutral resource with the same releases, values,
        and withheld semantics its own route serves.
    """
    factory = nass_warehouse

    expected_rows = 0
    for product in enabled_products():
        document = nass_support.load_product_fixture(product.product_id)
        _release, transformed, published = nass_support.run_to_gold(
            factory, product, document
        )
        rows = sum(
            len(envelope["data"]["data"]) for envelope in document["slices"].values()
        )
        assert transformed == published == rows
        expected_rows += rows
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)

    assert _query(factory, "SELECT COUNT(*) FROM gold_nass.crop_observation") == [
        (expected_rows,)
    ]

    with real_api_client() as client:
        observations = client.get("/api/usda-nass/observations", params={"limit": 1000})
        assert observations.status_code == 200
        payload = observations.json()
        assert payload["total"] == expected_rows

        # A withheld value keeps the provider's exact symbol as its source text,
        # carries no number, and never appears as zero. Its CV is withheld with
        # it, so a consumer cannot read an unqualified estimate.
        withheld = [
            item for item in payload["items"] if item["value_status"] == "withheld"
        ]
        assert withheld
        for item in withheld:
            assert item["value"] is None
            assert item["value_source"] == item["suppression_code"]
            assert item["value_source"].startswith("(")
            assert item["cv_value"] is None
        assert not [
            item
            for item in payload["items"]
            if item["value_status"] != "valid" and item["value"] is not None
        ]

        # Exact source text and parsed value stay distinguishable: the provider
        # writes thousands separators the warehouse must not invent or drop.
        planted = _observation(
            payload["items"],
            product_id=REVISED_PRODUCT,
            short_desc="CORN - ACRES PLANTED",
            geo_id="state:01|county:001",
            year=2024,
        )
        assert planted["value_source"] == "18,500"
        assert float(planted["value"]) == 18500
        assert planted["unit_desc"] == "ACRES"
        assert planted["cv_value"] is not None
        assert planted["source_desc"] == "SURVEY"
        assert planted["domain_desc"] == "TOTAL"
        assert planted["domaincat_desc"] == "NOT SPECIFIED"
        assert planted["agg_level_desc"] == "COUNTY"
        assert planted["freq_desc"] == "ANNUAL"
        assert planted["reference_period_desc"] == "YEAR"
        assert planted["load_time"] is not None
        assert planted["release_watermark"]

        # A survey estimate and a census count sharing one label remain
        # separate products with their own source program.
        harvested_label = "CORN, GRAIN - ACRES HARVESTED"
        by_program = {
            item["source_desc"]
            for item in payload["items"]
            if item["short_desc"] == harvested_label
        }
        assert by_program == {"SURVEY", "CENSUS"}
        national_by_program = {
            item["source_desc"]: item
            for item in payload["items"]
            if item["short_desc"] == harvested_label
            and item["agg_level_desc"] == "NATIONAL"
        }
        assert set(national_by_program) == {"SURVEY", "CENSUS"}
        assert (
            national_by_program["SURVEY"]["product_id"]
            != national_by_program["CENSUS"]["product_id"]
        )

        # Incompatible units never collapse into one series identity, and a
        # non-additive rate is declared as such rather than left to the caller.
        series = client.get("/api/usda-nass/series", params={"limit": 1000}).json()
        assert series["total"] > 0
        unit_by_series: dict[tuple, set[str]] = {}
        for item in series["items"]:
            key = (
                item["product_id"],
                item["commodity_desc"],
                item["statisticcat_desc"],
                item["geo_id"],
            )
            unit_by_series.setdefault(key, set()).add(item["unit_desc"])
        assert all(len(units) == 1 for units in unit_by_series.values())

        measures = client.get("/api/usda-nass/measures").json()
        behavior = {
            item["statisticcat_desc"]: (
                item["additive_behavior"],
                item["additive_behavior_known"],
            )
            for item in measures["items"]
        }
        assert behavior["YIELD"] == ("non_additive", True)
        assert behavior["PRODUCTION"] == ("not_established", False)

        # The source notes a consumer needs to avoid an unsafe comparison are
        # served beside the data rather than left in the repository.
        notes = client.get("/api/usda-nass/source-notes")
        assert notes.status_code == 200
        notes_payload = notes.json()
        assert notes_payload["total"] == len(notes_payload["items"]) > 0
        assert all(
            note["topic"] and note["summary"] and note["detail"]
            for note in notes_payload["items"]
        )

        # A filter that would mix programs or units must be expressible, and
        # the response must stay inside the filter it was given.
        census_only = client.get(
            "/api/usda-nass/observations",
            params={"source_desc": "CENSUS", "limit": 1000},
        ).json()
        assert census_only["total"] > 0
        assert {item["source_desc"] for item in census_only["items"]} == {"CENSUS"}
        acres_only = client.get(
            "/api/usda-nass/observations",
            params={"unit_desc": "ACRES", "limit": 1000},
        ).json()
        assert {item["unit_desc"] for item in acres_only["items"]} == {"ACRES"}

        # Covers: E2E-004 — replaying the captured releases changes nothing a
        # consumer can observe.
        for product in enabled_products():
            document = nass_support.load_product_fixture(product.product_id)
            nass_support.run_to_gold(factory, product, document)
        replayed = client.get(
            "/api/usda-nass/observations", params={"limit": 1000}
        ).json()
        assert replayed == payload

        # Covers: E2E-005 — the revision publishes a formerly withheld county
        # without erasing the release that withheld it.
        product = get_product(REVISED_PRODUCT)
        revised_document = nass_support.load_product_fixture(
            "corn_survey_annual_revised"
        )
        revised, _transformed, _published = nass_support.run_to_gold(
            factory, product, revised_document
        )
        watermark = revised.contract.extraction_watermark

        history = client.get(
            "/api/usda-nass/observations",
            params={
                "product_id": REVISED_PRODUCT,
                "geo_id": REVISED_GEO_ID,
                "statisticcat_desc": "PRODUCTION",
                "limit": 100,
            },
        ).json()
        assert history["total"] == 2
        as_released = sorted(
            history["items"], key=lambda item: item["release_watermark"]
        )
        assert as_released[0]["value_status"] == "withheld"
        assert as_released[0]["value"] is None
        assert as_released[0]["value_source"] == "(D)"
        assert as_released[1]["value_status"] == "valid"
        assert as_released[1]["value"] is not None

        latest = client.get(
            "/api/usda-nass/observations",
            params={
                "product_id": REVISED_PRODUCT,
                "geo_id": REVISED_GEO_ID,
                "statisticcat_desc": "PRODUCTION",
                "latest": True,
                "limit": 100,
            },
        ).json()
        assert latest["total"] == 1
        assert latest["items"][0]["release_watermark"] == watermark
        assert latest["items"][0]["value_status"] == "valid"

        # Provider-neutral discovery: NASS measures reach the shared catalog
        # with their own units, so no consumer can add bushels to acres.
        harvested = harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA))
        assert harvested > 0
        catalog = client.get(
            "/api/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 500}
        )
        assert catalog.status_code == 200
        catalog_payload = catalog.json()
        assert catalog_payload["total"] == harvested
        assert {item["source_code"] for item in catalog_payload["items"]} == {
            SOURCE_CODE
        }
        assert len({item["metric_code"] for item in catalog_payload["items"]}) == (
            harvested
        )
        assert {"ACRES", "BU"} <= {item["units"] for item in catalog_payload["items"]}

        # Covers: E2E-014 — the neutral resource answers the same publication
        # the NASS route serves, resolved through the published lineage.
        revised_row = as_released[1]
        production_metrics = [
            item
            for item in catalog_payload["items"]
            if item["physical_lineage"]["product_id"] == REVISED_PRODUCT
            and item["physical_lineage"]["statisticcat_desc"] == "PRODUCTION"
            and item["metric_display_name"] == revised_row["short_desc"]
        ]
        assert len(production_metrics) == 1
        production_metric = production_metrics[0]["metric_code"]

        neutral_latest = client.get(
            "/api/v1/observations",
            params={
                "metric_code": production_metric,
                "geo_id": REVISED_GEO_ID,
                "limit": 100,
            },
        )
        assert neutral_latest.status_code == 200
        neutral_payload = neutral_latest.json()
        assert neutral_payload["source_code"] == SOURCE_CODE
        assert neutral_payload["total"] == 1
        (neutral_row,) = neutral_payload["items"]
        assert neutral_row["release"] == watermark
        assert neutral_row["value_status"] == "valid"
        assert Decimal(neutral_row["value"]) == Decimal(str(revised_row["value"]))
        assert neutral_row["unit"] == revised_row["unit_desc"]
        assert neutral_row["dimensions"]["short_desc"] == revised_row["short_desc"]
        assert neutral_row["dimensions"]["domain_desc"] == "TOTAL"

        neutral_history = client.get(
            "/api/v1/observations",
            params={
                "metric_code": production_metric,
                "scope": "as_released",
                "geo_id": REVISED_GEO_ID,
                "limit": 100,
            },
        ).json()
        assert neutral_history["total"] == 2
        withheld_neutral = next(
            row for row in neutral_history["items"] if row["value_status"] == "withheld"
        )
        assert withheld_neutral["value"] is None
        assert withheld_neutral["dimensions"]["suppression_code"] == "(D)"

        release_listing = client.get(
            "/api/v1/observations/releases",
            params={"metric_code": production_metric},
        ).json()
        listed = [item["release"] for item in release_listing["items"]]
        assert listed[0] == watermark
        assert as_released[0]["release_watermark"] in listed
