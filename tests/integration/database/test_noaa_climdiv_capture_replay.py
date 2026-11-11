"""Real PostgreSQL NOAA nClimDiv county capture-to-gold contract.

Covers: ETL-126
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.noaa_climdiv.schema import (
    REQUIRED_RELATIONS,
    ensure_noaa_climdiv_schema,
)
from data_ingestion_toolbox.quality.sources import (
    climdiv_file_reconciliation,
    climdiv_value_range,
)
from tests.support import noaa_climdiv as climdiv

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def climdiv_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return climdiv.reviewed_warehouse(postgres_connection_factory, request)


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


def _outcomes(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})
    finally:
        reader.close()


def test_county_months_load_as_published_with_missing_months_kept_apart(
    climdiv_warehouse,
) -> None:
    """Covers: ETL-126 — NCEI state codes crosswalked; values as published; markers missing, not zero."""
    factory = climdiv_warehouse
    run_id, statuses, written, published = climdiv.run_to_gold(factory)
    # Six files of ten lines, twelve months each.
    assert statuses == {element: "captured" for element in climdiv.ELEMENTS}
    assert (written, published) == (720, 6)
    # Kent County (NCEI 07 -> FIPS 10) is served; Dane County (NCEI 47 ->
    # FIPS 55) is kept unmapped because the shared geography lacks it.
    assert _rows(
        factory,
        """
        SELECT ncei_state_code, state_fips, geography_status, COUNT(*)
        FROM silver_noaa_climdiv.county_month GROUP BY 1, 2, 3 ORDER BY 1
        """,
    ) == [("07", "10", "resolved", 360), ("47", "55", "unmapped", 360)]
    served = _rows(
        factory,
        """
        SELECT year, month, value, value_source, period_start::TEXT, period_end::TEXT, release_key
        FROM gold_noaa_climdiv.observation_latest
        WHERE geo_id = %s AND metric_key = 'monthly_precipitation_total'
        ORDER BY year, month
        """,
        (climdiv.KENT,),
    )
    # January 1895, as NCEI published it.
    assert served[0] == (
        1895,
        1,
        Decimal("4.51"),
        "4.51",
        "1895-01-01",
        "1895-01-31",
        "nClimDiv climdiv-pcpncy-v1.0.0-20261105",
    )
    # November and December 2026 are NCEI's -9.99 marker: kept missing in
    # silver, never served, never zero.
    assert (2026, 11) not in {(row[0], row[1]) for row in served}
    assert len(served) == 58
    assert _rows(
        factory,
        """
        SELECT value_source, value, value_status, missing_reason
        FROM silver_noaa_climdiv.county_month
        WHERE ncei_state_code = '07' AND county_fips = '001'
          AND measure = 'monthly_precipitation_total' AND year = 2026 AND month = 12
        """,
    ) == [("-9.99", None, "missing", "not_yet_published")]
    # A temperature below zero is a value; the -99.90 marker is not.
    assert _rows(
        factory,
        """
        SELECT value_status FROM silver_noaa_climdiv.county_month
        WHERE ncei_state_code = '07' AND measure = 'monthly_average_temperature'
          AND year = 2026 AND month = 11
        """,
    ) == [("missing",)]
    # Every served row names its capture and line.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_noaa_climdiv.observation_latest
        WHERE capture_id IS NULL OR source_line < 1
        """,
    ) == [(0,)]


def test_a_new_release_writes_only_the_months_that_changed(climdiv_warehouse) -> None:
    """Covers: ETL-126 — churn rule: unchanged months are not rewritten; replaced ones are kept."""
    factory = climdiv_warehouse
    first_run, *_ = climdiv.run_to_gold(factory)
    # The same names again: nothing is downloaded or written.
    client = climdiv.FixtureClient()
    _run, statuses, written, published = climdiv.run_to_gold(factory, client=client)
    assert statuses == {element: "already_loaded" for element in climdiv.ELEMENTS}
    assert (written, published) == (0, 0)
    assert len(client.calls) == 1  # the listing only
    # The December release fills Kent's November 2026 precipitation and
    # revises its October; everything else is the same text.
    original = (
        (climdiv.FIXTURE_DIR / climdiv.file_name("pcpn")).read_text().splitlines()
    )
    kent_2026 = original[4]
    revised = kent_2026[:74] + "   2.40" + "   3.10" + kent_2026[88:]
    files = climdiv.next_release("20261205", {("pcpn", 5): revised})
    run_id, statuses, written, published = climdiv.run_to_gold(
        factory, client=climdiv.FixtureClient(files)
    )
    # Five files are new names with identical lines; pcpn changes two months.
    assert statuses["pcpn"] == "captured" and written == 2
    assert _rows(
        factory,
        """
        SELECT month, value_source, run_id::TEXT FROM silver_noaa_climdiv.county_month
        WHERE ncei_state_code = '07' AND measure = 'monthly_precipitation_total'
          AND year = 2026 AND month IN (10, 11) ORDER BY month
        """,
    ) == [(10, "2.40", str(run_id)), (11, "3.10", str(run_id))]
    # The replaced values are kept as they were published, with their run.
    assert _rows(
        factory,
        """
        SELECT month, value_source, run_id::TEXT, superseded_by_run_id::TEXT
        FROM silver_noaa_climdiv.county_month_revision ORDER BY month
        """,
    ) == [
        (10, kent_2026[74:81].strip(), str(first_run), str(run_id)),
        (11, "-9.99", str(first_run), str(run_id)),
    ]
    # Released history: October 2026 has two published values, November one.
    assert _rows(
        factory,
        """
        SELECT month, COUNT(*) FROM gold_noaa_climdiv.observation_revision
        WHERE geo_id = %s AND metric_key = 'monthly_precipitation_total'
          AND year = 2026 AND month IN (10, 11) GROUP BY month ORDER BY month
        """,
        (climdiv.KENT,),
    ) == [(10, 2), (11, 1)]
    # Unchanged months keep their first run.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM silver_noaa_climdiv.county_month
        WHERE run_id = %s
        """,
        (str(run_id),),
    ) == [(2,)]


def test_an_unreadable_line_is_quarantined_alone(climdiv_warehouse) -> None:
    """Covers: ETL-126 — a negative precipitation or a foreign state code never publishes."""
    factory = climdiv_warehouse
    original = (
        (climdiv.FIXTURE_DIR / climdiv.file_name("pcpn")).read_text().splitlines()
    )
    negative = original[0][:11] + "  -1.00" + original[0][18:]
    foreign = "99" + original[1][2:]
    files = climdiv.next_release(
        "20261105", {("pcpn", 1): negative, ("pcpn", 2): foreign}
    )
    _run, statuses, written, _published = climdiv.run_to_gold(
        factory, client=climdiv.FixtureClient(files)
    )
    assert statuses["pcpn"] == "captured"
    assert sorted(
        _rows(
            factory,
            "SELECT source_row_index, error_code FROM silver_noaa_climdiv.quarantine",
        )
    ) == [(1, "negative_value"), (2, "unknown_state_code")]
    assert _rows(
        factory,
        """
        SELECT line_count, value_count FROM control.noaa_climdiv_file WHERE file_element = 'pcpn'
        """,
    ) == [(10, 96)]


def test_the_harvest_names_six_measures(climdiv_warehouse) -> None:
    """Covers: ETL-126 — six county measures, each a source fact on a monthly grain."""
    factory = climdiv_warehouse
    climdiv.run_all(factory)
    published = _rows(
        factory,
        """
        SELECT source_object_key, measure_kind, valid_time_grains[1]
        FROM gold_noaa_climdiv.metric_publisher ORDER BY 1
        """,
    )
    assert len(published) == 6
    assert {row[1:] for row in published} == {("source_fact", "MONTHLY")}
    assert harvest_publisher(factory, Publisher("gold_noaa_climdiv")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'NOAA_CLIMDIV'",
    ) == [(6,)]


def test_the_rules_pass_the_fixtures_then_catch_a_fault(climdiv_warehouse) -> None:
    """Covers: ETL-126 — DQ-CLIMDIV-002 and -004 pass the fixtures, then catch a fault."""
    factory = climdiv_warehouse
    assert [
        outcome.result for outcome in _outcomes(factory, climdiv_file_reconciliation)
    ] == ["not_applicable"]
    run_id, _statuses, _written, _published = climdiv.run_to_gold(factory)
    assert [
        outcome.result for outcome in _outcomes(factory, climdiv_file_reconciliation)
    ] == ["pass", "pass"]
    assert [outcome.result for outcome in _outcomes(factory, climdiv_value_range)] == [
        "pass"
    ]
    writer = factory()
    try:
        with writer.cursor() as cursor:
            # A value that slipped past the parser, and a file whose rows
            # stay behind after its ledger row is no longer published.
            cursor.execute(
                """
                UPDATE silver_noaa_climdiv.county_month SET value = -1, value_source = '-1.00'
                WHERE measure = 'monthly_precipitation_total' AND year = 1895 AND month = 1
                  AND ncei_state_code = '07'
                """
            )
            cursor.execute(
                """
                UPDATE control.noaa_climdiv_file SET status = 'silver_ready'
                WHERE run_id = %s AND file_element = 'tmax'
                """,
                (str(run_id),),
            )
        writer.commit()
    finally:
        writer.close()
    ledger, orphans = _outcomes(factory, climdiv_file_reconciliation)
    assert (ledger.result, ledger.observed_count) == ("fail", 1)
    assert (orphans.result, orphans.observed_count) == ("fail", 1)
    (value_range,) = _outcomes(factory, climdiv_value_range)
    assert (value_range.result, value_range.observed_count) == ("warn", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_noaa_climdiv_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
