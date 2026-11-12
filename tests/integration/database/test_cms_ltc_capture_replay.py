"""Real PostgreSQL CMS long-term care capture-to-gold contract.

Covers: ETL-130
"""

from __future__ import annotations

import csv
import io
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from tests.support import cms_ltc

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def cms_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return cms_ltc.reviewed_warehouse(postgres_connection_factory, request)


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


def _csv(name: str) -> list[dict[str, str]]:
    return list(
        csv.DictReader(io.StringIO(cms_ltc.file_bytes(name).decode("utf-8-sig")))
    )


def test_every_release_reaches_gold_and_dane_is_placed_by_its_cost_reports(
    cms_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-130 -- every row stored; Dane's sums are its facilities' published figures."""
    found, results = cms_ltc.load_releases(cms_warehouse)
    assert [(item.series_id, item.release_key) for item in found] == [
        ("cost_report", "2024-01-01"),
        ("characteristics", "2026-04-01"),
        ("characteristics", "2026-07-01"),
    ]
    assert [result[2] for result in results] == [314, 323, 323]
    assert all(result[3] == 1 for result in results)

    cost = _csv("CostReportsnf_Final_24_wi.csv")
    dane_beds = sum(
        int(row["Number of Beds"] or 0) for row in cost if row["County"] == "DANE"
    )
    dane_ccns = {row["Provider CCN"] for row in cost if row["County"] == "DANE"}
    q3 = _csv("Form_671_Q3_2026_wi.csv")
    dane_residents = sum(
        int(row["Total Residents"]) for row in q3 if row["Provider Number"] in dane_ccns
    )
    served = dict(
        _rows(
            cms_warehouse,
            """
            SELECT metric_key || '@' || period_start::TEXT, value
            FROM gold_cms_ltc.area_latest WHERE geo_id = %s
            """,
            (cms_ltc.DANE,),
        )
    )
    assert served["cost_report:number_of_beds@2024-01-01"] == dane_beds == 1521
    assert served["characteristics:total_residents@2026-07-01"] == dane_residents == 883

    state = _rows(
        cms_warehouse,
        """
        SELECT value, facility_count FROM gold_cms_ltc.area_latest
        WHERE geo_id = 'state:55' AND metric_key = 'characteristics:total_residents'
          AND period_start = DATE '2026-07-01'
        """,
    )
    assert state == [(sum(int(row["Total Residents"]) for row in q3), len(q3))]


def test_county_names_are_matched_exactly_or_left_unplaced(
    cms_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-130 -- ST CROIX and ST. CROIX are St. Croix County; a Florida county name in Wisconsin is not placed."""
    cms_ltc.load_releases(cms_warehouse)
    placed = dict(
        _rows(
            cms_warehouse,
            """
            SELECT county_name, MIN(COALESCE(county_geo_id, county_status))
            FROM silver_cms_ltc.facility_report
            WHERE series_id = 'cost_report' AND county_name IN ('ST CROIX', 'ST. CROIX', 'OKALOOSA')
            GROUP BY county_name
            """,
        )
    )
    assert placed == {
        "ST CROIX": "state:55|county:109",
        "ST. CROIX": "state:55|county:109",
        "OKALOOSA": "unmapped",
    }


def test_a_facility_filing_two_reports_counts_once(
    cms_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-130 -- the report whose fiscal year ends last is the one summed."""
    cms_ltc.load_releases(cms_warehouse)
    totals = _rows(
        cms_warehouse,
        """
        SELECT value, facility_count FROM gold_cms_ltc.area_latest
        WHERE geo_id = 'state:55' AND metric_key = 'cost_report:number_of_beds'
        """,
    )
    cost = _csv("CostReportsnf_Final_24_wi.csv")
    newest: dict[str, dict[str, str]] = {}
    for row in cost:
        newest.setdefault(row["Provider CCN"], []).append(row)  # type: ignore[arg-type]

    def _end(row: dict[str, str]) -> tuple[str, int]:
        month, day, year = (row["Fiscal Year End Date"] or "00/00/0000").split("/")
        return (f"{year}{month}{day}", int(row["rpt_rec_num"]))

    chosen = [max(rows, key=_end) for rows in newest.values()]  # type: ignore[arg-type]
    with_beds = [row for row in chosen if row["Number of Beds"]]
    assert totals == [
        (sum(int(row["Number of Beds"]) for row in with_beds), len(with_beds))
    ]


def test_rerun_reads_nothing_new_and_the_facility_list_names_dane(
    cms_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-130 -- the same bytes are `unchanged`; the list carries both periods."""
    cms_ltc.load_releases(cms_warehouse)
    _found, again = cms_ltc.load_releases(cms_warehouse)
    assert [result[1] for result in again] == ["unchanged"] * 3
    assert [result[2] for result in again] == [0, 0, 0]
    dane = _rows(
        cms_warehouse,
        """
        SELECT COUNT(*), SUM(number_of_beds), SUM(total_residents),
               MIN(residents_quarter_start)::TEXT, MIN(cost_report_year)
        FROM gold_cms_ltc.facility_latest WHERE county_geo_id = %s
        """,
        (cms_ltc.DANE,),
    )
    assert dane == [(17, 1521, 883, "2026-07-01", 2024)]
