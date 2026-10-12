"""Real PostgreSQL Building Permits Survey capture-to-gold contract.

Covers: ETL-057
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_bps.client import BpsPayloadError
from data_ingestion_toolbox.census_bps.registry import ANNUAL, MONTHLY, BpsSlice
from data_ingestion_toolbox.census_bps.schema import (
    REQUIRED_RELATIONS,
    ensure_census_bps_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import bps_file_reconciliation
from tests.support import census_bps as bps

pytestmark = [pytest.mark.integration, pytest.mark.database]

ANNUAL_FILES = (
    BpsSlice("county", ANNUAL, 2024, 12),
    BpsSlice("place", ANNUAL, 2024, 12, "south"),
)


@pytest.fixture
def bps_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return bps.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple = ()
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_a_month_and_a_year_reach_gold_with_reported_figures_kept(
    bps_warehouse,
) -> None:
    """Covers: ETL-057 — county and state months, county and place years, by code."""
    factory = bps_warehouse
    _run, facts, published = bps.run_to_gold(factory, MONTHLY, 2024, 3)
    # County file: 4 counties x 12 figures; state file: 3 rows x 8 (no valuation).
    assert (facts, published) == (4 * 12 + 3 * 8, 2)
    _run, facts, published = bps.run_to_gold(
        factory, ANNUAL, 2024, 12, files=ANNUAL_FILES
    )
    assert (facts, published) == (3 * 12 + 16 * 12, 2)

    kent = _rows(
        factory,
        """
        SELECT metric_key, period_start::TEXT, value::TEXT, reported_value::TEXT, geo_level, observation_basis
        FROM gold_census_bps.observation_latest
        WHERE geo_id = 'state:10|county:001' AND measure_id = 'units' AND structure_type = '1_unit'
        ORDER BY period_start, metric_key
        """,
    )
    assert [row[:5] for row in kent] == [
        ("units:1_unit:annual", "2024-01-01", "1056", "1053", "COUNTY"),
        ("units:1_unit:monthly", "2024-03-01", "81", "81", "COUNTY"),
    ]
    assert kent[0][5].startswith("authorized by building permits")

    places = _rows(
        factory,
        """
        SELECT geo_id, geography_status, value_status, value::TEXT
        FROM gold_census_bps.observation_latest
        WHERE geo_level = 'PLACE' AND measure_id = 'units' AND structure_type = '5_plus_units'
          AND geo_id IN ('state:10|place:21200', 'state:10|place:50670')
        ORDER BY geo_id
        """,
    )
    assert places == [
        ("state:10|place:21200", "resolved", "valid", "108"),
        ("state:10|place:50670", "unmapped", "not_reported", None),
    ]
    assert _rows(
        factory,
        """
        SELECT DISTINCT geo_level FROM gold_census_bps.observation_latest
        WHERE metric_key = 'units:1_unit:monthly' ORDER BY 1
        """,
    ) == [("COUNTY",), ("NATIONAL",), ("STATE",)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_census_bps.observation_latest WHERE geo_level = 'STATE' AND measure_id = 'valuation'",
    ) == [(0,)]


def test_the_publisher_says_authorized_and_harvests(bps_warehouse) -> None:
    """Covers: ETL-057 — one metric per measure, structure type and frequency."""
    factory = bps_warehouse
    bps.run_to_gold(factory, MONTHLY, 2024, 3)
    bps.run_to_gold(factory, ANNUAL, 2024, 12, files=ANNUAL_FILES)
    published = _rows(
        factory,
        """
        SELECT source_object_key, metric_display_name, valid_geo_grains, valid_time_grains
        FROM gold_census_bps.metric_publisher ORDER BY source_object_key
        """,
    )
    assert len(published) == 3 * 4 * 2
    by_key = {row[0]: row for row in published}
    assert by_key["units:1_unit:monthly"][2] == ["COUNTY", "NATIONAL", "STATE"]
    assert by_key["units:1_unit:annual"][2] == ["COUNTY", "PLACE"]
    assert by_key["valuation:5_plus_units:monthly"][2] == ["COUNTY"]
    assert by_key["units:1_unit:annual"][3] == ["ANNUAL"]
    assert all("authorized, not started or completed" in row[1] for row in published)
    assert harvest_publisher(factory, Publisher("gold_census_bps")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'CENSUS_BPS'",
    ) == [(24,)]


def test_a_rerun_is_idempotent_and_a_changed_file_keeps_both_checksums(
    bps_warehouse,
) -> None:
    """Covers: ETL-057 — the same bytes add nothing new; a revised file is a second row."""
    factory = bps_warehouse
    first, _f, _p = bps.run_to_gold(factory, MONTHLY, 2024, 3)
    second, _f, _p = bps.run_to_gold(factory, MONTHLY, 2024, 3)
    count = _rows(factory, "SELECT COUNT(*) FROM gold_census_bps.observation_latest")
    revised = bps.fixture_bytes("/County/co2403c.txt").replace(
        b"Kent County                                                 ,81,81,",
        b"Kent County                                                 ,82,82,",
    )
    assert revised != bps.fixture_bytes("/County/co2403c.txt")
    third, _f, _p = bps.run_to_gold(
        factory,
        MONTHLY,
        2024,
        3,
        client=bps.FixtureClient(
            {"/County/co2403c.txt": httpx.Response(200, content=revised)}
        ),
    )
    history = _rows(
        factory,
        """
        SELECT revision.value::TEXT, capture.payload_checksum, revision.run_id::TEXT
        FROM gold_census_bps.observation_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        WHERE revision.metric_key = 'buildings:1_unit:monthly' AND revision.geo_id = 'state:10|county:001'
        ORDER BY revision.retrieved_at
        """,
    )
    assert [row[2] for row in history] == [str(first), str(second), str(third)]
    assert history[0][1] == history[1][1] != history[2][1]
    assert _rows(
        factory,
        """
        SELECT value::TEXT FROM gold_census_bps.observation_latest
        WHERE metric_key = 'buildings:1_unit:monthly' AND geo_id = 'state:10|county:001'
        """,
    ) == [("82",)]
    assert (
        _rows(factory, "SELECT COUNT(*) FROM gold_census_bps.observation_latest")
        == count
    )


def test_a_malformed_file_is_quarantined_and_the_ledger_rule_runs(
    bps_warehouse,
) -> None:
    """Covers: ETL-057 — a refused file is held back; DQ-BPS-002 passes, then catches a loss."""
    factory = bps_warehouse

    def outcome():
        reader = factory()
        try:
            with reader.cursor() as cursor:
                return bps_file_reconciliation(cursor, {})[0]
        finally:
            reader.close()

    assert outcome().result == "not_applicable"
    bad_row = bps.fixture_bytes("/County/co2403c.txt").replace(
        b"202403,10,005,", b"202402,10,005,"
    )
    run_id, facts, published = bps.run_to_gold(
        factory,
        MONTHLY,
        2024,
        3,
        client=bps.FixtureClient(
            {"/County/co2403c.txt": httpx.Response(200, content=bad_row)}
        ),
    )
    assert (facts, published) == (3 * 12 + 3 * 8, 2)
    assert _rows(
        factory,
        "SELECT error_code FROM silver_census_bps.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [("unexpected_period",)]
    passed = outcome()
    assert (passed.result, passed.observed_count) == ("pass", 0)

    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census_bps.fact_observation WHERE geo_id = 'state:10|county:001'"
            )
            cursor.execute(
                "DELETE FROM silver_census_bps.observation_revision WHERE geo_id = 'state:10|county:001'"
            )
        writer.commit()
    finally:
        writer.close()
    failed = outcome()
    assert (failed.result, failed.observed_count) == ("fail", 1)
    # A file whose layout moved is refused before it is captured.
    with pytest.raises(BpsPayloadError):
        bps.run_to_gold(
            factory,
            MONTHLY,
            2024,
            3,
            client=bps.FixtureClient(
                {
                    "/State/st2403c.txt": httpx.Response(
                        200, content=b"<html>moved</html>"
                    )
                }
            ),
        )


def test_an_unpublished_month_is_recorded_empty_and_the_schema_reapplies(
    bps_warehouse,
) -> None:
    """Covers: ETL-057 — a 404 file is empty, not a failure; the DDL is rerunnable."""
    factory = bps_warehouse
    run_id, facts, published = bps.run_to_gold(factory, MONTHLY, 2026, 9)
    assert (facts, published) == (0, 0)
    assert _rows(
        factory,
        "SELECT slice_key, status FROM control.census_bps_slice WHERE run_id = %s ORDER BY 1",
        (str(run_id),),
    ) == [("county", "empty"), ("state", "empty")]

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_census_bps_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
