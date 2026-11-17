"""Real PostgreSQL CDC COVID-19 case count capture-to-gold contract.

Covers: ETL-135
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc_covid_cases.capture import CdcCovidSliceError
from data_ingestion_toolbox.cdc_covid_cases.client import CdcCovidFetchError
from tests.support import cdc_covid_cases as covid

pytestmark = [pytest.mark.integration, pytest.mark.database]

WI_SLICE = "res_state='WI' AND case_month='2021-11'"


@pytest.fixture
def covid_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return covid.reviewed_warehouse(postgres_connection_factory, request)


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


def _latest(
    factory: Callable[[], connection], metric_key: str, geo_id: str, month: str
):
    rows = _rows(
        factory,
        """
        SELECT value, unplaced_record_count, geography_status
        FROM gold_cdc_covid_cases.area_latest
        WHERE metric_key = %s AND geo_id = %s AND period_start = %s::DATE
        """,
        (metric_key, geo_id, month),
    )
    return rows[0] if rows else None


def test_every_record_is_counted_and_dane_shows_its_published_records(
    covid_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-135 -- every group stored; Dane 2021-11 counts CDC's published records."""
    snapshot_id, records, published = covid.load_snapshot(covid_warehouse)
    assert snapshot_id is not None
    assert (records, published) == (82_251 + 2_808_221, 1)
    slices = _rows(
        covid_warehouse,
        """
        SELECT res_state, case_month, index_record_count, record_count, group_count
        FROM control.cdc_covid_cases_slice ORDER BY slice_index
        """,
    )
    assert slices == [
        ("CA", "2022-01", 2_808_221, 2_808_221, 1114),
        ("WI", "2021-11", 82_251, 82_251, 1506),
    ]
    assert _rows(
        covid_warehouse, "SELECT COUNT(*) FROM silver_cdc_covid_cases.quarantine"
    ) == [(0,)]
    dane = covid.DANE
    assert _latest(
        covid_warehouse, "covid:case_records:all_ages", dane, "2021-11-01"
    ) == (
        3651,
        0,
        "resolved",
    )
    assert (
        _latest(
            covid_warehouse, "covid:hospitalized_records:all_ages", dane, "2021-11-01"
        )[0]
        == 83
    )
    assert (
        _latest(
            covid_warehouse,
            "covid:hospitalized_records:age_65_plus",
            dane,
            "2021-11-01",
        )[0]
        == 38
    )
    assert (
        _latest(covid_warehouse, "covid:case_records:age_unknown", dane, "2021-11-01")[
            0
        ]
        == 16
    )
    assert (
        _latest(
            covid_warehouse,
            "covid:hospitalization_unknown_records:all_ages",
            dane,
            "2021-11-01",
        )[0]
        == 1317
    )
    assert (
        _latest(
            covid_warehouse, "covid:death_unknown_records:all_ages", dane, "2021-11-01"
        )[0]
        == 1699
    )
    # Dane had no published record with death_yn = Yes that month: a true zero
    # beside 3,651 records, not an absence.
    assert (
        _latest(covid_warehouse, "covid:death_records:all_ages", dane, "2021-11-01")[0]
        == 0
    )
    # CDC withheld the county (NA) of 6,163 Wisconsin records: in the state only.
    assert _latest(
        covid_warehouse, "covid:case_records:all_ages", "state:55", "2021-11-01"
    ) == (
        82_251,
        6_163,
        "resolved",
    )
    assert (
        _latest(
            covid_warehouse, "covid:death_records:all_ages", "state:55", "2021-11-01"
        )[0]
        == 239
    )
    # California is not in the seeded geography: its records are on no county.
    assert _latest(
        covid_warehouse, "covid:case_records:all_ages", "us:1", "2022-01-01"
    ) == (
        2_808_221,
        2_808_221,
        "resolved",
    )
    # An age group with no published record in an area has no row, never zero.
    assert _rows(
        covid_warehouse,
        "SELECT COUNT(*) FROM silver_cdc_covid_cases.area_month WHERE case_record_count = 0",
    ) == [(0,)]
    publisher = _rows(
        covid_warehouse,
        """
        SELECT source_object_key, valid_geo_grains, source_watermark
        FROM gold_cdc_covid_cases.metric_publisher
        WHERE source_object_key = 'covid:case_records:all_ages'
        """,
    )
    assert publisher == [
        ("covid:case_records:all_ages", ["COUNTY", "NATIONAL", "STATE"], "2022-01")
    ]
    assert _rows(
        covid_warehouse, "SELECT COUNT(*) FROM gold_cdc_covid_cases.metric_publisher"
    ) == [(30,)]


def test_a_published_snapshot_is_not_read_again_and_history_repeats_unchanged(
    covid_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-135 -- the frozen dataset is read once; an identical history read writes nothing."""
    covid.load_snapshot(covid_warehouse)
    client = covid.FixtureClient()
    assert covid.load_snapshot(covid_warehouse, client=client) == (None, 0, 0)
    assert client.calls == []
    assert covid.load_snapshot(covid_warehouse, conf={"history": True}) == (None, 0, 0)
    statuses = _rows(
        covid_warehouse,
        "SELECT status FROM control.cdc_covid_cases_snapshot ORDER BY created_at",
    )
    assert statuses == [("published",), ("unchanged",)]
    assert _rows(
        covid_warehouse,
        "SELECT COUNT(DISTINCT snapshot_id) FROM silver_cdc_covid_cases.area_month",
    ) == [(1,)]


def test_a_run_that_stops_resumes_at_the_next_slice(
    covid_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-135 -- an interrupted read resumes without asking again for captured slices."""
    failing = covid.FixtureClient({WI_SLICE: httpx.Response(503, content=b"{}")})
    with pytest.raises(CdcCovidFetchError):
        covid.load_snapshot(covid_warehouse, client=failing)
    client = covid.FixtureClient()
    snapshot_id, records, published = covid.load_snapshot(
        covid_warehouse, client=client
    )
    assert snapshot_id is not None and published == 1
    assert records == 82_251 + 2_808_221
    assert [params.get("$where") for _url, params in client.calls] == [WI_SLICE]


def test_a_slice_that_does_not_add_up_to_the_index_is_refused(
    covid_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-135 -- groups that do not add to the index quarantine the snapshot."""
    short = covid.FixtureClient(
        {
            WI_SLICE: httpx.Response(
                200,
                content=b'[{"county_fips_code":"55025","age_group":"65+ years","hosp_yn":"No","death_yn":"No","n":"5"}]',
            )
        }
    )
    with pytest.raises(CdcCovidSliceError, match="slice_total_differs"):
        covid.load_snapshot(covid_warehouse, client=short)
    assert _rows(
        covid_warehouse, "SELECT status FROM control.cdc_covid_cases_snapshot"
    ) == [("quarantined",)]
    assert covid.load_snapshot(covid_warehouse) == (None, 0, 0)
    assert _rows(
        covid_warehouse, "SELECT COUNT(*) FROM gold_cdc_covid_cases.area_latest"
    ) == [(0,)]
