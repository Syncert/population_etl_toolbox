"""Deterministic Census PEP flow from reviewed bulk CSV through the API.

Every provider byte is a checked-in reviewed release excerpt; nothing in this
module reaches the network. The node proves what PEP specifically must not lose
on the way to a consumer: a release vintage is not an observation year, a
newest-complete-vintage projection does not erase the release it superseded, a
population estimate is not an ACS survey estimate, and an incorporated place
keeps its exact Census identity and functional status.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.census_pep.silver_pep.replay import replay_pep_capture
from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support.api import real_api_client
from tests.support.postgres import PostgresHookStub
from tests.support.warehouse_scope import WarehouseScope, warehouse_scope

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures/census_pep"
SOURCE_CODE = CONFIG.source_code
PUBLISHER_SCHEMA = "gold_pep"

#: The reviewed national rows the two vintages disagree about. Vintage 2025
#: revised the 2024 national estimate downward; both must remain readable.
NATIONAL_2024_AS_RELEASED = 340110988
NATIONAL_2024_REVISED = 340003797
#: Abbeville city, Alabama, from the reviewed sub-county release.
PLACE_GEO_ID = "state:01|place:00124"
PLACE_2025_ESTIMATE = 2378

POPULATION_METRIC = "CENSUS_PEP:pep_nst_alldata:POPESTIMATE"
PLACE_METRIC = "CENSUS_PEP:pep_subcounty:POPESTIMATE"

SEEDED_GEOGRAPHIES: tuple[dict[str, object], ...] = (
    {"geo_type": "nation", "vintage": 2025, "name": "United States"},
    {"geo_type": "state", "state_fips": "01", "vintage": 2025, "name": "Alabama"},
    {
        "geo_type": "place",
        "state_fips": "01",
        "place_fips": "00124",
        "vintage": 2025,
        "name": "Abbeville city",
    },
)

_SILVER_STATEMENTS = (
    "DELETE FROM silver_pep.fact_population_estimate",
    "DELETE FROM silver_pep.release_load",
    "DELETE FROM silver_pep.observation_revision",
    "DELETE FROM silver_pep.dim_measure",
)


@pytest.fixture
def pep_scope(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> WarehouseScope:
    """Own every PEP row this node commits, teardown registered up front."""
    scope = warehouse_scope(
        postgres_connection_factory,
        request,
        source_code=SOURCE_CODE,
        silver_statements=_SILVER_STATEMENTS,
    )
    scope.seed_geographies(list(SEEDED_GEOGRAPHIES))
    return scope


def _capture_and_replay(
    scope: WarehouseScope,
    *,
    dataset_code: str,
    vintage_year: int,
    fixture_name: str,
) -> int:
    """Capture one reviewed release exactly as production does, then replay it."""
    release = next(
        item
        for item in CONFIG.releases
        if item.dataset_code == dataset_code and item.vintage_year == vintage_year
    )
    control = CaptureControl(scope.connection_factory, source_code=SOURCE_CODE)
    run_id = scope.track_run(
        control.start_run(watermark={"product_code": release.product_code})
    )
    parameters = {
        "dataset_code": dataset_code,
        "vintage_year": vintage_year,
        "product_code": release.product_code,
    }
    request = control.start_request(
        run_id=run_id, endpoint=release.data_url, parameters=parameters
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=SOURCE_CODE,
        endpoint=release.data_url,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "text/csv"},
        media_type=release.media_type,
        payload=(FIXTURE_DIR / fixture_name).read_bytes(),
        payload_schema_version=release.schema_version,
        source_revision=release.product_code,
    )
    persist_response_capture(scope.connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    replayed = replay_pep_capture(
        scope.connection_factory, capture_id=capture.capture_id, release=release
    )
    control.finish_run(run_id, status="success")
    return replayed


def _publish(scope: WarehouseScope) -> int:
    """Conform silver and announce publication, as the production DAG does."""
    inserted = transform_pep_to_silver(PostgresHookStub(scope.connection_factory))
    emit_latest_publisher_ready(
        scope.connection_factory, publisher_schema=PUBLISHER_SCHEMA
    )
    return inserted


def _observation(items: list[dict], **match: object) -> dict:
    matched = [
        item
        for item in items
        if all(item[name] == value for name, value in match.items())
    ]
    assert len(matched) == 1, f"expected exactly one item for {match}, got {matched}"
    return matched[0]


def test_pep_fixtures_reach_the_api_with_vintage_and_place_identity_intact(
    pep_scope: WarehouseScope,
) -> None:
    """Covers: E2E-009 — PEP replays raw-to-API with exact source semantics.

    Covers: E2E-004 — replaying a captured release adds no fact and returns
        byte-identical API JSON.
    Covers: E2E-005 — a newer vintage advances the latest projection while the
        superseded release stays readable as released.
    Covers: E2E-006 — a population estimate carries no fabricated margin of
        error and cannot be confused with an ACS survey estimate.
    Covers: E2E-014 — the glossary-discovered PEP measure answers through the
        registry-dispatched neutral resource with exact vintages and values,
        while the legacy union routes stay honestly empty for PEP.
    """
    scope = pep_scope

    replayed_2024 = _capture_and_replay(
        scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2024,
        fixture_name="nst_2024.csv",
    )
    replayed_2025 = _capture_and_replay(
        scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2025,
        fixture_name="nst_2025.csv",
    )
    replayed_place = _capture_and_replay(
        scope,
        dataset_code="pep_subcounty",
        vintage_year=2025,
        fixture_name="subcounty_2025.csv",
    )
    assert replayed_2024 > 0 and replayed_2025 > 0 and replayed_place > 0

    inserted = _publish(scope)
    assert inserted > 0

    # A release vintage is a release identity, not the year being estimated.
    as_released = scope.query(
        """
        SELECT pep_vintage, observation_year, estimate_date, value
        FROM gold_pep.population_estimate_revision
        WHERE dataset_code = 'pep_nst_alldata'
          AND metric_code = 'POPESTIMATE'
          AND geo_id = 'us:1'
          AND observation_year = 2024
        ORDER BY pep_vintage
        """
    )
    assert as_released == [
        (2024, 2024, datetime(2024, 7, 1).date(), NATIONAL_2024_AS_RELEASED),
        (2025, 2024, datetime(2024, 7, 1).date(), NATIONAL_2024_REVISED),
    ]

    # Newest-complete-vintage selection projects one row and keeps the vintage.
    assert scope.query(
        """
        SELECT pep_vintage, value
        FROM gold_pep.population_estimate_latest
        WHERE dataset_code = 'pep_nst_alldata'
          AND metric_code = 'POPESTIMATE'
          AND geo_id = 'us:1'
          AND observation_year = 2024
        """
    ) == [(2025, NATIONAL_2024_REVISED)]

    # Exact Census place identity, functional status, and geography basis.
    assert scope.query(
        """
        SELECT geo_id, geo_type, summary_level, functional_status_source,
               geography_basis_date, value
        FROM gold_pep.population_estimate_latest
        WHERE dataset_code = 'pep_subcounty'
          AND metric_code = 'POPESTIMATE'
          AND observation_year = 2025
        """
    ) == [
        (
            PLACE_GEO_ID,
            "place",
            "162",
            "A",
            datetime(2025, 1, 1).date(),
            PLACE_2025_ESTIMATE,
        )
    ]
    assert scope.query(
        """
        SELECT status, resolution_method
        FROM silver_ref.geography_resolution
        WHERE provider_source = %s
          AND provider_dataset = 'pep_subcounty'
          AND source_geo_type = 'place'
          AND source_code = '0100124'
        """,
        (SOURCE_CODE,),
    ) == [("resolved", "exact_code")]

    with real_api_client() as client:
        latest = client.get(
            "/api/pep/observations/latest",
            params={"metric_code": POPULATION_METRIC, "limit": 100},
        )
        assert latest.status_code == 200
        latest_payload = latest.json()
        # "Latest" is the newest *vintage*, not the newest observation year:
        # the 2025 release publishes one row per estimated year, and every one
        # of them must carry that release's vintage.
        assert {item["vintage_year"] for item in latest_payload["items"]} == {2025}
        national = _observation(
            latest_payload["items"], geo_id="us:1", observation_date="2024-07-01"
        )
        assert national["source_code"] == SOURCE_CODE
        assert float(national["value"]) == NATIONAL_2024_REVISED
        # No fabricated uncertainty: PEP publishes estimates, not survey
        # samples, so the survey margin-of-error fields stay null rather than
        # defaulting to zero.
        assert national["margin_of_error"] is None
        assert national["margin_of_error_pct"] is None
        assert national["units"] == "persons"

        # As-released history remains addressable through the source route.
        timeseries = client.get(
            "/api/pep/observations/timeseries",
            params={"metric_code": POPULATION_METRIC, "geo_id": "us:1", "limit": 500},
        )
        assert timeseries.status_code == 200
        released_2024 = [
            item
            for item in timeseries.json()["items"]
            if item["observation_date"] == "2024-07-01"
        ]
        assert {item["vintage_year"] for item in released_2024} == {2024, 2025}
        assert {float(item["value"]) for item in released_2024} == {
            float(NATIONAL_2024_AS_RELEASED),
            float(NATIONAL_2024_REVISED),
        }

        place = client.get(
            "/api/pep/observations/latest",
            params={"metric_code": PLACE_METRIC, "geo_level": "place", "limit": 100},
        ).json()
        place_item = _observation(
            place["items"], geo_id=PLACE_GEO_ID, observation_date="2025-07-01"
        )
        # The canonical identifier is what survives to a consumer: the
        # observation contract carries no place_fips column, so a place is
        # addressable only as its exact canonical geo_id.
        assert place_item["geo_level"] == "place"
        assert place_item["dataset_code"] == "pep_subcounty"
        assert float(place_item["value"]) == PLACE_2025_ESTIMATE

        # The legacy cross-source views union ACS, BLS, and FRED only, and the
        # legacy latest route still reads them; PEP's neutral reach is the
        # registry-dispatched ``/api/v1/observations`` resource (asserted
        # below), while this legacy route stays an empty, well-formed answer
        # until API-008 retires it. This assertion fails the moment the union
        # widens without registering the change.
        neutral = client.get(
            "/api/observations/latest",
            params={"metric_code": POPULATION_METRIC, "limit": 10},
        )
        assert neutral.status_code == 200
        assert neutral.json()["total"] == 0

        # A PEP estimate must never be reachable under an ACS metric identity.
        assert POPULATION_METRIC.startswith(f"{SOURCE_CODE}:")
        acs_shaped = client.get(
            "/api/observations/latest",
            params={"metric_code": "CENSUS_ACS:pep_nst_alldata:POPESTIMATE"},
        )
        assert acs_shaped.status_code == 200
        assert acs_shaped.json()["total"] == 0

        # Provider-neutral discovery: the glossary harvest is the contract that
        # makes PEP measures visible beside every other source's.
        harvested = harvest_publisher(
            scope.connection_factory, Publisher(PUBLISHER_SCHEMA)
        )
        assert harvested > 0
        catalog = client.get(
            "/api/catalog/metrics",
            params={"source_code": SOURCE_CODE, "limit": 200},
        )
        assert catalog.status_code == 200
        catalog_payload = catalog.json()
        assert catalog_payload["total"] == harvested
        assert {item["source_code"] for item in catalog_payload["items"]} == {
            SOURCE_CODE
        }
        assert "POPESTIMATE" in {
            item["metric_code"].rsplit(":", 1)[-1] for item in catalog_payload["items"]
        }

        # Covers: E2E-014 — the discovered measure answers through the
        # registry-dispatched neutral resource. One glossary measure spans the
        # datasets that publish it; each row keeps its dataset identity.
        glossary_metric = next(
            item["metric_code"]
            for item in catalog_payload["items"]
            if item["metric_code"].endswith(":POPESTIMATE")
        )
        neutral_latest = client.get(
            "/api/v1/observations",
            params={"metric_code": glossary_metric, "geo_id": "us:1", "limit": 100},
        )
        assert neutral_latest.status_code == 200
        neutral_payload = neutral_latest.json()
        assert neutral_payload["source_code"] == SOURCE_CODE
        neutral_national = next(
            row
            for row in neutral_payload["items"]
            if row["period_start"] == "2024-07-01"
        )
        assert float(neutral_national["value"]) == NATIONAL_2024_REVISED
        assert neutral_national["release"] == "2025"
        assert neutral_national["uncertainty"] is None, (
            "PEP publishes no survey uncertainty; the envelope must not invent one"
        )
        assert neutral_national["dimensions"]["dataset_code"] == "pep_nst_alldata"

        # As-released history serves both vintages of the same estimate date.
        neutral_history = client.get(
            "/api/v1/observations",
            params={
                "metric_code": glossary_metric,
                "scope": "as_released",
                "geo_id": "us:1",
                "limit": 500,
            },
        ).json()
        released_neutral_2024 = [
            row
            for row in neutral_history["items"]
            if row["period_start"] == "2024-07-01"
        ]
        assert {row["release"] for row in released_neutral_2024} == {"2024", "2025"}
        assert {float(row["value"]) for row in released_neutral_2024} == {
            float(NATIONAL_2024_AS_RELEASED),
            float(NATIONAL_2024_REVISED),
        }

        release_listing = client.get(
            "/api/v1/observations/releases",
            params={"metric_code": glossary_metric},
        ).json()
        listed_releases = [item["release"] for item in release_listing["items"]]
        assert listed_releases[0] == "2025"
        assert "2024" in listed_releases

        # The place estimate keeps its canonical identity and dataset.
        neutral_place = client.get(
            "/api/v1/observations",
            params={
                "metric_code": glossary_metric,
                "geo_id": PLACE_GEO_ID,
                "limit": 100,
            },
        ).json()
        place_row = next(
            row for row in neutral_place["items"] if row["period_start"] == "2025-07-01"
        )
        assert place_row["geo_level"] == "place"
        assert float(place_row["value"]) == PLACE_2025_ESTIMATE
        assert place_row["dimensions"]["dataset_code"] == "pep_subcounty"

        # Covers: E2E-004 — replaying the same captures changes nothing a
        # consumer can observe.
        assert transform_pep_to_silver(PostgresHookStub(scope.connection_factory)) == 0
        replayed = client.get(
            "/api/pep/observations/latest",
            params={"metric_code": POPULATION_METRIC, "limit": 100},
        )
        assert replayed.json() == latest_payload


def test_pep_teardown_removes_every_row_after_a_deliberate_failure(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> None:
    """Covers: E2E-013 — cleanup runs when a product node fails.

    A teardown that only runs on success leaves the shared warehouse dirty
    exactly when a run is already failing, which is when the next test's
    diagnosis matters most.
    """
    inner_request = _StandaloneRequest()
    scope = warehouse_scope(
        postgres_connection_factory,
        inner_request,  # type: ignore[arg-type]
        source_code=SOURCE_CODE,
        silver_statements=_SILVER_STATEMENTS,
    )
    scope.seed_geographies(list(SEEDED_GEOGRAPHIES))
    _capture_and_replay(
        scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2025,
        fixture_name="nst_2025.csv",
    )
    _publish(scope)
    assert int(scope.scalar("SELECT COUNT(*) FROM silver_pep.observation_revision")) > 0

    try:
        raise AssertionError("deliberate in-test failure")
    except AssertionError:
        pass
    finally:
        inner_request.finalize()

    assert scope.scalar("SELECT COUNT(*) FROM silver_pep.observation_revision") == 0
    assert scope.scalar("SELECT COUNT(*) FROM silver_pep.fact_population_estimate") == 0
    assert (
        scope.scalar(
            "SELECT COUNT(*) FROM raw_capture.response_capture WHERE source_code = %s",
            (SOURCE_CODE,),
        )
        == 0
    )
    assert (
        scope.scalar(
            "SELECT COUNT(*) FROM silver_ref.geography_resolution "
            "WHERE provider_source = %s",
            (SOURCE_CODE,),
        )
        == 0
    )
    scope.assert_absent([PLACE_GEO_ID])


class _StandaloneRequest:
    """A minimal finalizer registry, so teardown is provable inside one test."""

    def __init__(self) -> None:
        self._finalizers: list[Callable[[], None]] = []

    def addfinalizer(self, finalizer: Callable[[], None]) -> None:
        self._finalizers.append(finalizer)

    def finalize(self) -> None:
        for finalizer in reversed(self._finalizers):
            finalizer()
        self._finalizers.clear()
