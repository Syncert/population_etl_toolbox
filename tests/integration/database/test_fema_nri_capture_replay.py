"""Real PostgreSQL FEMA National Risk Index and declarations capture-to-gold contract.

Covers: ETL-067
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fema_nri.client import FemaPayloadError
from data_ingestion_toolbox.fema_nri.config import FemaConfig
from data_ingestion_toolbox.fema_nri.registry import DECLARATIONS, NRI
from data_ingestion_toolbox.fema_nri.schema import (
    REQUIRED_RELATIONS,
    ensure_fema_nri_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    fema_run_reconciliation,
    fema_value_and_geography,
)
from tests.support import fema_nri as fema

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def fema_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return fema.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def _outcome(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def test_the_nri_reaches_gold_with_its_version_and_ratings(fema_warehouse) -> None:
    """Covers: ETL-067 — losses in dollars per year, a Not Applicable hazard with no number."""
    factory = fema_warehouse
    _run, status, rows, published = fema.run_to_gold(factory, NRI)
    assert (status, published) == ("captured", 1) and rows == 6 * 24
    kent = dict(
        _rows(
            factory,
            "SELECT metric_key, value FROM gold_fema_nri.observation_latest WHERE geo_id = %s",
            (fema.KENT,),
        )
    )
    expected = Decimal(str(fema.fixture_records(NRI)[0]["EAL_VALT"]))
    assert kent["expected_annual_loss"] == expected
    assert len(kent) == 24
    tsunami = _rows(
        factory,
        """
        SELECT value, value_status, missing_reason, rating, release_key, year
        FROM gold_fema_nri.observation_latest WHERE geo_id = %s AND metric_key = 'expected_annual_loss_tsunami'
        """,
        (fema.ADJUNTAS,),
    )
    assert tsunami == [
        (
            None,
            "not_applicable",
            "hazard_not_applicable",
            "Not Applicable",
            "December 2025",
            2025,
        )
    ]
    # American Samoa is in the fixture but not in the seeded geography: kept
    # in silver as unmapped, recorded, not served.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_fema_nri.observation_latest WHERE geo_id = 'state:60|county:010'",
    ) == [(0,)]
    assert _rows(
        factory,
        """
        SELECT status FROM silver_ref.geography_resolution
        WHERE provider_source = 'FEMA_NRI' AND provider_dataset = 'fema_nri' AND source_code = '60010'
        """,
    ) == [("unmapped",)]


def test_declarations_are_counted_per_county_and_year(fema_warehouse) -> None:
    """Covers: ETL-067 — distinct declarations per county; areas and legacy counties not counted."""
    factory = fema_warehouse
    fema.run_to_gold(
        factory,
        DECLARATIONS,
        config=FemaConfig(
            min_spacing_seconds=0, max_attempts=1, declaration_page_size=25
        ),
    )
    records = fema.fixture_records(DECLARATIONS)
    expected: dict[tuple[str, int], set[int]] = {}
    for record in records:
        if (
            record["fipsStateCode"] == "10"
            and record["fipsCountyCode"] == "001"
            and record["declarationType"] == "DR"
        ):
            expected.setdefault(("DR", int(record["declarationDate"][:4])), set()).add(
                record["disasterNumber"]
            )
    served = dict(
        _rows(
            factory,
            """
            SELECT year, value FROM gold_fema_nri.observation_latest
            WHERE geo_id = %s AND metric_key = 'major_disaster_declarations'
            """,
            (fema.KENT,),
        )
    )
    assert served == {
        year: Decimal(len(numbers)) for (_kind, year), numbers in expected.items()
    }
    assert _rows(
        factory,
        "SELECT page_count FROM control.fema_nri_run WHERE stream = 'declarations'",
    ) == [(4,)]
    # Statewide and tribal-area rows are kept as areas; Connecticut's legacy
    # county codes are recorded unmapped and not counted.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_fema_nri.declaration_revision WHERE geography_status = 'area'",
    ) == [(15,)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_fema_nri.observation_latest WHERE geo_id LIKE 'state:09%%'",
    ) == [(0,)]


def test_reruns_and_revisions(fema_warehouse) -> None:
    """Covers: ETL-067 — the same NRI pages replay nothing; a changed hash keeps both revisions."""
    factory = fema_warehouse
    fema.run_to_gold(factory, NRI)
    _run, status, rows, published = fema.run_to_gold(factory, NRI)
    assert (status, rows, published) == ("unchanged", 0, 0)
    fema.run_to_gold(factory, DECLARATIONS)
    _run, status, rows, _published = fema.run_to_gold(factory, DECLARATIONS)
    assert status == "captured" and rows == 0
    records = fema.fixture_records(DECLARATIONS)
    kent = next(
        r
        for r in records
        if r["fipsStateCode"] == "10" and r["fipsCountyCode"] == "001"
    )
    revised = [
        dict(
            r,
            hash="f" * 40,
            lastRefresh="2026-12-01T00:00:00.000Z",
            declarationType="EM",
        )
        if r["id"] == kent["id"]
        else r
        for r in records
    ]
    _run, status, rows, published = fema.run_to_gold(
        factory, DECLARATIONS, client=fema.FixtureClient({DECLARATIONS: revised})
    )
    assert (status, rows, published) == ("captured", 1, 1)
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_fema_nri.declaration_revision WHERE declaration_id = %s",
        (kent["id"],),
    ) == [(2,)]


def test_a_refused_page_and_the_rules(fema_warehouse) -> None:
    """Covers: ETL-067 — an error body fails capture; DQ-FEMA-002 and -004 pass or warn, then catch a fault."""
    factory = fema_warehouse
    assert _outcome(factory, fema_run_reconciliation).result == "not_applicable"
    with pytest.raises(FemaPayloadError, match="service_error"):
        fema.run_to_gold(
            factory, NRI, client=fema.FixtureClient({NRI: b'{"error": {"code": 498}}'})
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.fema_nri_run") == [(0,)]
    run_id, _status, _rows_written, _published = fema.run_to_gold(factory, NRI)
    assert _outcome(factory, fema_run_reconciliation).result == "pass"
    coverage = _outcome(factory, fema_value_and_geography)
    # American Samoa's Eastern District is unmapped in the seeded geography.
    assert coverage.result == "warn"
    before = coverage.observed_count
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "UPDATE silver_fema_nri.nri_fact SET value = -1 WHERE geo_id = %s AND field = 'EAL_VALT'",
                (fema.KENT,),
            )
        writer.commit()
    finally:
        writer.close()
    assert _outcome(factory, fema_value_and_geography).observed_count == before + 1
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_fema_nri.nri_fact WHERE run_id = %s", (str(run_id),)
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, fema_run_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_fema_nri_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_every_published_measure(fema_warehouse) -> None:
    """Covers: ETL-067 — county metrics with something to publish; modelled and counted say which."""
    factory = fema_warehouse
    fema.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, measure_kind FROM gold_fema_nri.metric_publisher",
        )
    )
    assert published["expected_annual_loss"] == "modelled_estimate"
    assert published["major_disaster_declarations"] == "derived_count"
    # The fixture's two fire management declarations designate Connecticut
    # legacy counties, which do not resolve: that measure has nothing to
    # publish yet, so it is not offered.
    assert len(published) == 26 and "fire_management_declarations" not in published
    assert harvest_publisher(factory, Publisher("gold_fema_nri")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'FEMA_NRI'",
    ) == [(26,)]
