"""Deterministic FBI UCR flow from reviewed CDE fixtures to the published boundary.

FBI Crime publishes no source-specific HTTP route: the accepted FBI plan
delivered the agency-grain contract as the ``gold_fbi`` views plus the glossary
publisher, and a crime router belongs to the API platform plan. This node
therefore drives the reviewed Wisconsin release from raw capture to the two
surfaces a consumer can actually read today — the published gold views and the
provider-neutral catalog the glossary harvest feeds — and proves the semantics
FBI data is easiest to misreport: a month nobody reported is not zero crime, a
county filter is not a county total, and a rate is not an absolute count.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import fbi_release
from tests.support.api import real_api_client
from tests.support.fbi_release import (
    OBSERVATIONS_PER_SUBJECT,
    PERIODS,
    PRODUCT,
    SOURCE_CODE,
)

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

PUBLISHER_SCHEMA = "gold_fbi"
#: The reviewed agency that reports January and is absent in March.
INTERMITTENT_AGENCY = "WI0400100"
#: The reviewed agency associated with two counties, for the dedup contract.
TWO_COUNTY_AGENCY = "WI0540300"


@pytest.fixture
def fbi_warehouse(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    """Seed the reviewed geographies and remove all FBI state afterwards."""
    yield from fbi_release.reviewed_warehouse(postgres_connection_factory)


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


def test_fbi_fixtures_reach_the_published_boundary_without_inventing_totals(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: E2E-010 — FBI replays raw-to-published with exact semantics.

    Covers: E2E-004 — replaying a captured release publishes no additional
        observation and leaves the published projection unchanged.
    Covers: E2E-005 — a revised refresh date is retained beside the release it
        supersedes while the latest projection advances.
    Covers: E2E-006 — an unreported month stays ``not_reported`` with a null
        value, and a reported zero stays a zero.
    """
    factory = fbi_warehouse

    captured = fbi_release.persist_fixture_release(factory)
    transformed, published = fbi_release.run_pipeline(factory, captured)
    assert transformed > 0
    assert published == transformed
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)

    # Exact raw evidence is committed before anything is parsed.
    assert _query(
        factory,
        "SELECT COUNT(*) FROM raw_capture.response_capture WHERE run_id = %s",
        (str(captured.run_id),),
    ) == [(len(fbi_release.slice_fixtures()),)]

    # Provider-published totals and agency reports stay separable, and no
    # agency row is ever labelled as a published geography total.
    basis_by_subject = dict(
        _query(
            factory,
            """
            SELECT subject_type, MIN(geography_basis)
            FROM gold_fbi.crime_observation
            GROUP BY subject_type
            """,
        )
    )
    assert basis_by_subject == {
        "national": "provider-published national total",
        "state": "provider-published state total",
        "agency": "agency-reported for one law-enforcement agency",
    }

    # Program, offense, measure form, counted-entity basis, unit, and reported
    # status all survive publication, and a rate never shares a unit with an
    # absolute count.
    measure_identity = _query(
        factory,
        """
        SELECT DISTINCT ucr_program, offense_code, measure_form,
               counted_entity_basis, unit, reported_status
        FROM gold_fbi.crime_observation
        WHERE subject_type = 'national'
        ORDER BY measure_form, counted_entity_basis
        """,
    )
    # The provider publishes this product under one combined program label;
    # it must reach the consumer verbatim rather than being narrowed to "SRS",
    # which would misstate which collections the counts came from.
    program = "SRS_AND_SUMMARIZED_NIBRS"
    assert measure_identity == [
        (program, "V", "absolute_total", "clearance", "count", "reported"),
        (program, "V", "absolute_total", "offense", "count", "reported"),
        (program, "V", "rate", "clearance", "per_100000_population", "reported"),
        (program, "V", "rate", "offense", "per_100000_population", "reported"),
    ]
    assert _query(
        factory,
        """
        SELECT COUNT(*) FROM gold_fbi.crime_observation
        WHERE (measure_form = 'rate' AND unit = 'count')
           OR (measure_form = 'absolute_total' AND unit <> 'count')
        """,
    ) == [(0,)]

    # A month with no report is not zero crime; a reported zero is still zero.
    assert _query(
        factory,
        """
        SELECT period, value_status, value, participation_status
        FROM gold_fbi.crime_observation
        WHERE subject_code = %s AND measure_form = 'absolute_total'
          AND counted_entity_basis = 'offense' AND period IN ('01-2023', '03-2023')
        ORDER BY period
        """,
        (INTERMITTENT_AGENCY,),
    ) == [
        ("01-2023", "reported", 3, "full_participation"),
        ("03-2023", "not_reported", None, "no_participation"),
    ]

    # Every published observation carries its coverage interpretation.
    assert _query(
        factory,
        """
        SELECT COUNT(*) FROM gold_fbi.crime_observation
        WHERE participation_status IS NULL OR coverage_basis IS NULL
        """,
    ) == [(0,)]
    assert _query(
        factory,
        "SELECT COUNT(*) FROM gold_fbi.reporting_coverage WHERE coverage_basis IS NULL",
    ) == [(0,)]

    # A county or place association is an agency filter, deduplicated by
    # observation identity, and is labelled as such rather than as a total.
    assert _query(
        factory,
        """
        SELECT DISTINCT observation_grain, result_label
        FROM gold_fbi.agency_observation_area_filter
        WHERE filter_geography_type = 'county'
        """,
    ) == [("agency", "agency-reported for agencies associated with this county")]
    rows, distinct_observations = _query(
        factory,
        """
        SELECT COUNT(*), COUNT(DISTINCT observation_sk)
        FROM gold_fbi.agency_observation_area_filter
        WHERE ori = %s AND filter_geography_type = 'county'
        """,
        (TWO_COUNTY_AGENCY,),
    )[0]
    assert rows == 2 * OBSERVATIONS_PER_SUBJECT
    assert distinct_observations == OBSERVATIONS_PER_SUBJECT
    assert _query(
        factory,
        """
        SELECT COUNT(*) FROM gold_fbi.agency_observation_area_filter
        WHERE observation_grain <> 'agency'
        """,
    ) == [(0,)]

    published_before = _query(
        factory, "SELECT COUNT(*) FROM gold_fbi.crime_observation"
    )
    latest_before = _query(
        factory,
        "SELECT DISTINCT release_key FROM gold_fbi.latest_release_observation",
    )
    assert latest_before == [(captured.release_key,)]

    with real_api_client() as client:
        harvested = harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA))
        assert harvested == len(measure_identity)

        catalog = client.get(
            "/api/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        )
        assert catalog.status_code == 200
        catalog_payload = catalog.json()
        assert catalog_payload["total"] == harvested
        assert {item["source_code"] for item in catalog_payload["items"]} == {
            SOURCE_CODE
        }
        # Rate and absolute-total measures reach the neutral catalog as
        # separate metrics with their own units, so a consumer cannot add them.
        units = {
            item["metric_code"]: item["units"] for item in catalog_payload["items"]
        }
        assert len(units) == harvested
        assert set(units.values()) == {"count", "per_100000_population"}
        assert all(
            key.startswith(f"{SOURCE_CODE}:{PRODUCT.product_id}:") for key in units
        )

        sources = client.get("/api/catalog/sources")
        assert sources.status_code == 200
        assert SOURCE_CODE in {item["source_code"] for item in sources.json()}

        # Covers: E2E-004 — replaying the identical release republishes the
        # same rows rather than duplicating or dropping any.
        replayed_transformed, replayed_published = fbi_release.run_pipeline(
            factory, captured
        )
        assert (replayed_transformed, replayed_published) == (transformed, published)
        assert (
            _query(factory, "SELECT COUNT(*) FROM gold_fbi.crime_observation")
            == published_before
        )

        # Covers: E2E-005 — a revised refresh date is added, not substituted.
        revised = fbi_release.persist_fixture_release(
            factory, national_fixture="summarized_national_V_revised"
        )
        assert revised.release_key != captured.release_key
        fbi_release.run_pipeline(factory, revised)

        assert _query(
            factory,
            """
            SELECT release_key, status FROM silver_fbi.dim_ucr_dataset_release
            ORDER BY release_key
            """,
        ) == [(captured.release_key, "published"), (revised.release_key, "published")]
        assert _query(
            factory,
            "SELECT DISTINCT release_key FROM gold_fbi.latest_release_observation",
        ) == [(revised.release_key,)]
        national_by_release = _query(
            factory,
            """
            SELECT release_key, value FROM gold_fbi.crime_observation
            WHERE subject_type = 'national' AND period = '01-2023'
              AND measure_form = 'absolute_total'
              AND counted_entity_basis = 'offense'
            ORDER BY release_key
            """,
        )
        assert [row[0] for row in national_by_release] == [
            captured.release_key,
            revised.release_key,
        ]
        assert national_by_release[1][1] == national_by_release[0][1] + 25

        # The catalog follows the newest published release without gaining or
        # losing a measure identity.
        assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == harvested
        refreshed = client.get(
            "/api/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        assert refreshed["total"] == harvested

    # Every registered period is accounted for on every subject, so a fixture
    # that reports fewer months cannot silently shrink the published window.
    assert _query(
        factory,
        """
        SELECT COUNT(DISTINCT period) FROM gold_fbi.crime_observation
        WHERE release_key = %s
        """,
        (revised.release_key,),
    ) == [(PERIODS,)]
