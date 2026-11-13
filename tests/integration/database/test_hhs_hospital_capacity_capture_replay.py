"""Real PostgreSQL HHS hospital capacity capture-to-gold contract.

Covers: ETL-133
"""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.hhs_hospital_capacity.client import HhsFetchError
from tests.support import hhs_hospital_capacity as hhs

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def hhs_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return hhs.reviewed_warehouse(postgres_connection_factory, request)


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


def _fixture_rows() -> list[dict]:
    rows: list[dict] = []
    for offset in hhs.PAGE_OFFSETS:
        rows.extend(json.loads(hhs.page_bytes(offset)))
    return rows


def test_every_hospital_week_reaches_gold_and_dane_sums_its_hospitals(
    hhs_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-133 -- every row stored; Dane's weekly sums are its hospitals' published figures."""
    snapshot_id, stored, published = hhs.load_snapshot(hhs_warehouse)
    assert snapshot_id is not None
    assert (stored, published) == (1044, 1)
    pages = _rows(
        hhs_warehouse,
        "SELECT page_index, row_offset, row_count FROM control.hhs_hospital_capacity_page ORDER BY page_index",
    )
    assert pages == [(0, 0, 400), (1, 400, 400), (2, 800, 244)]
    rows = _fixture_rows()
    week = "2020-08-23T00:00:00.000"
    published_cells = [
        row.get("inpatient_beds_7_day_avg")
        for row in rows
        if row["collection_week"] == week
    ]
    valid = [Decimal(cell) for cell in published_cells if cell and cell != "-999999.0"]
    served = _rows(
        hhs_warehouse,
        """
        SELECT value, value_status, facility_count, reporting_facility_count,
               suppressed_count, missing_count, period_end::TEXT
        FROM gold_hhs_hospital_capacity.area_latest
        WHERE geo_id = %s AND metric_key = 'hospital:inpatient_beds'
          AND period_start = DATE '2020-08-23'
        """,
        (hhs.DANE,),
    )
    assert served == [
        (
            sum(valid),
            "valid",
            len(published_cells),
            len(valid),
            published_cells.count("-999999.0"),
            published_cells.count(None),
            "2020-08-29",
        )
    ]
    facilities = _rows(
        hhs_warehouse,
        """
        SELECT hospital_pk, hospital_subtype, county_status, placement_source, week_count
        FROM gold_hhs_hospital_capacity.facility_latest
        WHERE county_geo_id = %s ORDER BY hospital_pk
        """,
        (hhs.DANE,),
    )
    assert [row[0] for row in facilities] == [
        "520083",
        "520089",
        "520098",
        "521343",
        "522008",
    ]
    assert {row[2] for row in facilities} == {"resolved"}
    assert facilities[0][3] == "HHS published county FIPS code"
    assert sum(row[4] for row in facilities) == 1044


def test_a_suppressed_cell_stays_suppressed_and_an_absent_one_is_missing(
    hhs_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-133 -- -999999 is `suppressed`, never zero; an omitted cell is `missing`."""
    hhs.load_snapshot(hhs_warehouse)
    statuses = dict(
        _rows(
            hhs_warehouse,
            """
            SELECT value_status, COUNT(*) FROM silver_hhs_hospital_capacity.facility_week_value
            GROUP BY value_status
            """,
        )
    )
    rows = _fixture_rows()
    from data_ingestion_toolbox.hhs_hospital_capacity.registry import MEASURES

    cells = [row.get(measure.column) for row in rows for measure in MEASURES]
    assert statuses.get("suppressed", 0) == sum(
        1 for cell in cells if cell == "-999999.0"
    )
    assert statuses.get("missing", 0) == cells.count(None)
    assert sum(statuses.values()) == len(rows) * len(MEASURES)
    zero_for_suppressed = _rows(
        hhs_warehouse,
        """
        SELECT COUNT(*) FROM silver_hhs_hospital_capacity.facility_week_value
        WHERE value_status = 'suppressed' AND value IS NOT NULL
        """,
    )
    assert zero_for_suppressed == [(0,)]


def test_a_complete_snapshot_is_never_read_again_and_history_repeats_unchanged(
    hhs_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-133 -- a frozen dataset is read once; the same bytes again are `unchanged`."""
    hhs.load_snapshot(hhs_warehouse)
    client = hhs.FixtureClient()
    assert hhs.load_snapshot(hhs_warehouse, client=client) == (None, 0, 0)
    assert client.calls == []
    again = hhs.FixtureClient()
    assert hhs.load_snapshot(hhs_warehouse, client=again, conf={"history": True}) == (
        None,
        0,
        0,
    )
    assert len(again.calls) == 4
    assert _rows(
        hhs_warehouse,
        "SELECT status, COUNT(*) FROM control.hhs_hospital_capacity_snapshot GROUP BY status ORDER BY status",
    ) == [("published", 1), ("unchanged", 1)]
    assert _rows(
        hhs_warehouse,
        "SELECT COUNT(DISTINCT snapshot_id) FROM silver_hhs_hospital_capacity.facility_week_value",
    ) == [(1,)]


def test_a_run_that_stops_part_way_resumes_at_the_next_page(
    hhs_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-133 -- a failed page leaves the captured pages; the next run asks only for the rest."""
    failing = hhs.FixtureClient({400: httpx.Response(503, content=b"busy")})
    with pytest.raises(HhsFetchError):
        hhs.load_snapshot(hhs_warehouse, client=failing)
    assert _rows(
        hhs_warehouse, "SELECT COUNT(*) FROM control.hhs_hospital_capacity_page"
    ) == [(1,)]
    resumed = hhs.FixtureClient()
    _snapshot, stored, published = hhs.load_snapshot(hhs_warehouse, client=resumed)
    assert [call[1]["$offset"] for call in resumed.calls] == [400, 800]
    assert (stored, published) == (1044, 1)
