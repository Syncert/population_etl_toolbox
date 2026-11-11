"""Real PostgreSQL NOAA Storm Events capture-to-gold contract.

Covers: ETL-127
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.noaa_storm_events.schema import (
    REQUIRED_RELATIONS,
    ensure_noaa_storm_events_schema,
)
from tests.support import noaa_storm_events as storm

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def storm_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return storm.reviewed_warehouse(postgres_connection_factory, request)


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


def _served(
    factory: Callable[[], connection], geo_id: str, year: int
) -> dict[str, tuple]:
    return {
        row[0]: row[1:]
        for row in _rows(
            factory,
            """
            SELECT metric_key, value, county_reported, zone_reported
            FROM gold_noaa_storm_events.observation_latest
            WHERE geo_id = %s AND year = %s
            """,
            (geo_id, year),
        )
    }


def test_events_load_and_are_counted_where_reported(storm_warehouse) -> None:
    """Covers: ETL-127 — county events on their county; zone events on every county of a matching zone."""
    factory = storm_warehouse
    _run, statuses, written, published = storm.run_to_gold(factory)
    assert set(statuses.values()) == {"captured"}
    # 14 zone rows, 13 + 34 events; five files published.
    assert (written, published) == (14 + 13 + 34, 5)
    assert _rows(
        factory,
        """
        SELECT year, placement_status, COUNT(*) FROM silver_noaa_storm_events.event
        GROUP BY 1, 2 ORDER BY 1, 2
        """,
    ) == [
        (1995, "county", 13),
        (2024, "county", 3),
        (2024, "zone_matched", 31),
    ]
    # Dane County 1995: thirteen county-reported events (all three types of
    # the era are possible; only those reported have rows).
    dane_1995 = _served(factory, storm.DANE, 1995)
    assert dane_1995["storm_events_all_types"] == (Decimal(13), 13, 0)
    # Dane County 2024: three county events plus the seven events reported
    # for forecast zone WI063 "Dane".
    dane_2024 = _served(factory, storm.DANE, 2024)
    assert dane_2024["storm_events_all_types"] == (Decimal(10), 3, 7)
    assert dane_2024["storm_events_winter_weather"] == (Decimal(4), 0, 4)
    assert dane_2024["storm_events_tornado"] == (Decimal(1), 1, 0)
    # County damage sums the county-reported events only: 0 + 6K + 3K.
    assert dane_2024["storm_event_property_damage"] == (Decimal("9000.00"), 3, 0)
    # Kent County, Delaware: zone DE002 "Kent" (four events).
    assert _served(factory, storm.KENT, 2024)["storm_events_all_types"] == (
        Decimal(4),
        0,
        4,
    )
    # The state counts every event reported in it, the nation every event of
    # the 50 states and DC.
    assert _served(factory, storm.WISCONSIN, 2024)["storm_events_all_types"] == (
        Decimal(12),
        3,
        9,
    )
    assert _served(factory, storm.NATION, 2024)["storm_events_all_types"] == (
        Decimal(34),
        3,
        31,
    )
    # Northern Marinette matches zone WI013 ("Northern Marinette County"),
    # whose county is not in the shared geography: matched, counted on no county.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM silver_noaa_storm_events.event AS event
        JOIN silver_noaa_storm_events.event_place USING (event_id)
        WHERE event.cz_name = 'NORTHERN MARINETTE'
        """,
    ) == [(0,)]
    # 1950 had no row in the fixture: a year with a published file and no events.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_noaa_storm_events.year_release WHERE year = 1950",
    ) == [(1,)]
    # Every event read for a county names its capture.
    assert _rows(
        factory,
        """
        SELECT COUNT(*), COUNT(*) FILTER (WHERE capture_id IS NULL)
        FROM gold_noaa_storm_events.event_detail WHERE geo_id = %s
        """,
        (storm.DANE,),
    ) == [(23, 0)]


def test_a_zone_event_whose_name_matches_no_zone_file_is_not_placed(
    storm_warehouse,
) -> None:
    """Covers: ETL-127 — zone code AND name must match; otherwise unplaced with a coverage note."""
    factory = storm_warehouse
    rows = storm.details_rows(storm.DETAILS_2024)
    header = rows[0]
    columns = {name: index for index, name in enumerate(header)}
    changed = 0
    for row in rows[1:]:
        if row[columns["STATE_FIPS"]] == "10" and row[columns["CZ_FIPS"]] == "2":
            row[columns["CZ_NAME"]] = "KENT COASTAL"
            changed += 1
    assert changed == 4
    files = storm.fixture_files()
    files[storm.DETAILS_2024] = storm.details_file(rows)
    storm.run_to_gold(factory, client=storm.FixtureClient(files))
    assert "storm_events_all_types" not in _served(factory, storm.KENT, 2024)
    assert _rows(
        factory,
        """
        SELECT zone_events_placed, zone_events_unplaced, coverage_note
        FROM gold_noaa_storm_events.state_coverage WHERE geo_id = %s AND year = 2024
        """,
        (storm.DELAWARE,),
    ) == [
        (
            18,
            4,
            "4 of 22 events reported in DELAWARE in 2024 are not counted on any county: "
            "their forecast zone code and name match no published NWS zone-county file, "
            "or their county is not in the shared geography.",
        )
    ]
    # The state still counts them.
    assert _served(factory, storm.DELAWARE, 2024)["storm_events_all_types"][0] == 22


def test_a_restamped_year_writes_only_changed_events(storm_warehouse) -> None:
    """Covers: ETL-127 — churn rule; replaced and withdrawn events kept in the revision table."""
    factory = storm_warehouse
    first_run, *_ = storm.run_to_gold(factory)
    client = storm.FixtureClient()
    _run, statuses, written, published = storm.run_to_gold(factory, client=client)
    assert set(statuses.values()) == {"already_loaded"}
    assert (written, published) == (0, 0)
    assert len(client.calls) == 2  # the NWS page and the NCEI listing only
    rows = storm.details_rows(storm.DETAILS_2024)
    header = rows[0]
    columns = {name: index for index, name in enumerate(header)}
    dane_tornado = next(
        row
        for row in rows[1:]
        if row[columns["CZ_TYPE"]] == "C" and row[columns["EVENT_TYPE"]] == "Tornado"
    )
    dane_tornado[columns["DAMAGE_PROPERTY"]] = "25.00K"
    withdrawn = next(row for row in rows[1:] if row[columns["STATE_FIPS"]] == "10")
    withdrawn_id = int(withdrawn[columns["EVENT_ID"]])
    rows.remove(withdrawn)
    files = storm.fixture_files()
    del files[storm.DETAILS_2024]
    restamped = "StormEvents_details-ftp_v1.0_d2024_c20261026.csv.gz"
    files[restamped] = storm.details_file(rows)
    run_id, statuses, written, published = storm.run_to_gold(
        factory, client=storm.FixtureClient(files)
    )
    assert statuses[restamped] == "captured"
    assert (written, published) == (1, 1)
    assert _rows(
        factory,
        """
        SELECT change_kind, run_id::TEXT, superseded_by_run_id::TEXT
        FROM silver_noaa_storm_events.event_revision ORDER BY change_kind
        """,
    ) == [
        ("replaced", str(first_run), str(run_id)),
        ("withdrawn", str(first_run), str(run_id)),
    ]
    assert _rows(
        factory,
        "SELECT event_status FROM silver_noaa_storm_events.event WHERE event_id = %s",
        (withdrawn_id,),
    ) == [("withdrawn",)]
    assert _served(factory, storm.DANE, 2024)["storm_event_property_damage"][
        0
    ] == Decimal("34000.00")
    assert _served(factory, storm.NATION, 2024)["storm_events_all_types"][0] == 33
    # The 2024 figures name the restamped file.
    assert _rows(
        factory,
        """
        SELECT DISTINCT release_key FROM gold_noaa_storm_events.observation_latest
        WHERE year = 2024
        """,
    ) == [("Storm Events " + restamped,)]


def test_an_unreadable_row_is_quarantined_alone(storm_warehouse) -> None:
    """Covers: ETL-127 — a row without a readable place type never publishes."""
    factory = storm_warehouse
    rows = storm.details_rows(storm.DETAILS_1995)
    columns = {name: index for index, name in enumerate(rows[0])}
    rows[1][columns["CZ_TYPE"]] = "X"
    files = storm.fixture_files()
    files[storm.DETAILS_1995] = storm.details_file(rows)
    storm.run_to_gold(factory, client=storm.FixtureClient(files))
    assert _rows(
        factory,
        "SELECT source_row_index, error_code FROM silver_noaa_storm_events.quarantine",
    ) == [(2, "unknown_cz_type")]
    assert _rows(
        factory,
        """
        SELECT row_count, quarantined_count, status FROM control.noaa_storm_events_file
        WHERE data_year = 1995
        """,
    ) == [(13, 1, "published")]
    assert _served(factory, storm.DANE, 1995)["storm_events_all_types"][0] == 12


def test_schema_reapplies_and_publisher_serves_three_grains(storm_warehouse) -> None:
    """Covers: ETL-127 — DDL is idempotent; the publisher reads grains from served rows."""
    factory = storm_warehouse
    storm.run_to_gold(factory)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_noaa_storm_events_schema(_Hook())  # type: ignore[arg-type]
    for relation in REQUIRED_RELATIONS:
        assert _rows(factory, "SELECT to_regclass(%s) IS NOT NULL", (relation,)) == [
            (True,)
        ]
    publisher = dict(
        _rows(
            factory,
            """
            SELECT source_object_key, valid_geo_grains
            FROM gold_noaa_storm_events.metric_publisher
            """,
        )
    )
    assert publisher["storm_events_all_types"] == ["COUNTY", "NATIONAL", "STATE"]
    assert publisher["storm_events_tornado"] == ["COUNTY", "NATIONAL", "STATE"]
