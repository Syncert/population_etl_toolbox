"""Real PostgreSQL EPA AirData capture-to-gold contract.

Covers: ETL-068
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.epa_aqs.client import AqsPayloadError
from data_ingestion_toolbox.epa_aqs.schema import (
    REQUIRED_RELATIONS,
    ensure_epa_aqs_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    aqs_file_reconciliation,
    aqs_value_and_geography,
)
from tests.support import epa_aqs as aqs

pytestmark = [pytest.mark.integration, pytest.mark.database]

ZIP_NAME = "annual_conc_by_monitor_2024.zip"


@pytest.fixture
def aqs_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return aqs.reviewed_warehouse(postgres_connection_factory, request)


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


def test_county_figures_come_from_the_highest_complete_monitor(aqs_warehouse) -> None:
    """Covers: ETL-068 — the highest complete monitor per county; incomplete-only counties have no row."""
    factory = aqs_warehouse
    _run, status, facts, published = aqs.run_to_gold(factory, aqs.Y2024)
    assert (status, facts, published) == ("captured", 26, 1)
    highest = _rows(
        factory,
        """
        SELECT MAX(value) FROM silver_epa_aqs.monitor_fact
        WHERE geo_id = %s AND measure = 'pm25_annual_mean' AND completeness = 'Y'
          AND event_type IN ('No Events', 'Events Included')
        """,
        (aqs.NEW_CASTLE,),
    )[0][0]
    served = _rows(
        factory,
        """
        SELECT value, complete_monitors, highest_monitor, unit FROM gold_epa_aqs.observation_latest
        WHERE geo_id = %s AND metric_key = 'pm25_annual_mean' AND year = 2024
        """,
        (aqs.NEW_CASTLE,),
    )
    assert (
        served[0][0] == highest
        and served[0][1] == 5
        and served[0][3] == "micrograms per cubic meter"
    )
    # Kent's PM2.5 monitors are all incomplete: no county figure, not a zero.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_epa_aqs.observation_latest WHERE geo_id = %s AND metric_key = 'pm25_annual_mean'",
        (aqs.KENT,),
    ) == [(0,)]
    assert _rows(
        factory,
        "SELECT value FROM gold_epa_aqs.observation_latest WHERE geo_id = %s AND metric_key = 'ozone_8hour_4th_max'",
        (aqs.KENT,),
    )[0][0] > Decimal("0")
    # New Haven's monitors are kept and recorded, never served under a name.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_epa_aqs.monitor_observation WHERE geo_id = %s",
        (aqs.NEW_HAVEN,),
    ) == [(7,)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_epa_aqs.observation_latest WHERE geo_id = %s",
        (aqs.NEW_HAVEN,),
    ) == [(0,)]


def test_an_unchanged_read_replays_nothing_and_a_regenerated_file_is_kept(
    aqs_warehouse,
) -> None:
    """Covers: ETL-068 — the same bytes add nothing; a regenerated file keeps both reads."""
    factory = aqs_warehouse
    aqs.run_to_gold(factory, aqs.Y2024)
    _run, status, facts, published = aqs.run_to_gold(factory, aqs.Y2024)
    assert (status, facts, published) == ("unchanged", 0, 0)
    raw = aqs.fixture_bytes(aqs.Y2024)
    text = zipfile.ZipFile(io.BytesIO(raw)).read(aqs.Y2024.member)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(aqs.Y2024.member, text.replace(b'"2025-', b'"2026-', 1))
    _run, status, facts, published = aqs.run_to_gold(
        factory, aqs.Y2024, client=aqs.FixtureClient({ZIP_NAME: buffer.getvalue()})
    )
    assert (status, facts, published) == ("captured", 26, 1)
    assert _rows(
        factory,
        """
        SELECT COUNT(DISTINCT release_key) FROM gold_epa_aqs.observation_revision
        WHERE geo_id = %s AND metric_key = 'ozone_8hour_4th_max'
        """,
        (aqs.KENT,),
    ) == [(2,)]


def test_a_refused_file_and_the_rules(aqs_warehouse) -> None:
    """Covers: ETL-068 — a non-zip fails capture; DQ-AQS-002 and -004 behave, then catch a fault."""
    factory = aqs_warehouse
    assert _outcome(factory, aqs_file_reconciliation).result == "not_applicable"
    with pytest.raises(AqsPayloadError, match="member_missing"):
        aqs.run_to_gold(
            factory,
            aqs.Y2024,
            client=aqs.FixtureClient({ZIP_NAME: b"<html>gone</html>"}),
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.epa_aqs_file") == [(0,)]
    run_id, _status, _facts, _published = aqs.run_to_gold(factory, aqs.Y2024)
    assert _outcome(factory, aqs_file_reconciliation).result == "pass"
    coverage = _outcome(factory, aqs_value_and_geography)
    # New Haven's legacy county code is not in the seeded geography.
    assert (coverage.result, coverage.observed_count) == ("warn", 7)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_epa_aqs.monitor_fact WHERE run_id = %s",
                (str(run_id),),
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, aqs_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_epa_aqs_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_both_measures(aqs_warehouse) -> None:
    """Covers: ETL-068 — two county metrics, each a derived summary."""
    factory = aqs_warehouse
    aqs.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, measure_kind FROM gold_epa_aqs.metric_publisher",
        )
    )
    assert published == {
        "pm25_annual_mean": "derived_summary",
        "ozone_8hour_4th_max": "derived_summary",
    }
    assert harvest_publisher(factory, Publisher("gold_epa_aqs")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'EPA_AQS'",
    ) == [(2,)]
