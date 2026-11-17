"""Real PostgreSQL contract for the CDC PLACES tract release.

Covers: ETL-134
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc.capture import CdcCaptureError
from data_ingestion_toolbox.cdc.client import SocrataRetryExhausted
from tests.support import cdc_places_tract as tract

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def tract_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return tract.reviewed_warehouse(postgres_connection_factory, request)


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


def test_every_dane_tract_row_reaches_gold_as_cdc_published_it(
    tract_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-134 -- 4,920 rows, 123 tracts x 40 measures, value and limits as published."""
    release, published = tract.load_release(tract_warehouse)
    assert (release.row_count, release.counted_row_count, published) == (
        tract.ROW_COUNT,
        tract.ROW_COUNT,
        tract.ROW_COUNT,
    )
    pages = _rows(
        tract_warehouse,
        "SELECT page_offset, page_limit, row_count FROM control.cdc_release_page "
        "WHERE run_id = %s ORDER BY page_offset",
        (str(release.run_id),),
    )
    assert pages == [(0, 2000, 2000), (2000, 2000, 2000), (4000, 2000, 920)]
    served = _rows(
        tract_warehouse,
        """
        SELECT COUNT(DISTINCT geo_id), COUNT(DISTINCT measure_id),
               COUNT(*) FILTER (WHERE geography_status = 'resolved'),
               COUNT(*) FILTER (WHERE geo_type <> 'tract')
        FROM gold_cdc.latest_release_observation WHERE asset_id = 'places_tract'
        """,
    )
    assert served == [(123, 40, 4920, 0)]
    first = next(
        row
        for row in tract.all_rows()
        if row["locationid"] == "55025000100" and row["measureid"] == "ACCESS2"
    )
    access = _rows(
        tract_warehouse,
        """
        SELECT value, confidence_lower, confidence_upper, total_population,
               period_start, value_type_id, estimate_method
        FROM gold_cdc.latest_release_observation
        WHERE asset_id = 'places_tract' AND geo_id = %s AND measure_id = 'ACCESS2'
        """,
        (tract.FIRST_TRACT,),
    )
    assert access == [
        (
            Decimal(first["data_value"]),
            Decimal(first["low_confidence_limit"]),
            Decimal(first["high_confidence_limit"]),
            Decimal(first["totalpopulation"]),
            int(first["year"]),
            "CrdPrv",
            "model_based_small_area_estimate",
        )
    ]


def test_an_interrupted_read_resumes_at_the_next_page(
    tract_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-134 -- the pages already captured are reused, never read twice."""
    failing = tract.FixtureClient({2000: httpx.Response(503, content=b"busy")})
    with pytest.raises(SocrataRetryExhausted):
        tract.load_release(tract_warehouse, client=failing)
    resumed = tract.FixtureClient()
    release, published = tract.load_release(tract_warehouse, client=resumed)
    asked = [
        int(params["$offset"]) for _url, params in resumed.calls if "$offset" in params
    ]
    assert asked == [2000, 4000]
    assert published == tract.ROW_COUNT
    reused = _rows(
        tract_warehouse,
        "SELECT page_offset, resumed_from_run_id IS NOT NULL FROM control.cdc_release_page "
        "WHERE run_id = %s ORDER BY page_offset",
        (str(release.run_id),),
    )
    assert reused == [(0, True), (2000, False), (4000, False)]


def test_a_paged_total_that_differs_from_cdcs_count_is_refused(
    tract_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-134 -- a short read is never published as the whole release."""
    with pytest.raises(CdcCaptureError) as refused:
        tract.load_release(tract_warehouse, client=tract.FixtureClient(count=4921))
    assert refused.value.code == "paged_total_differs"
    assert _rows(
        tract_warehouse,
        "SELECT COUNT(*) FROM control.cdc_dataset_release WHERE asset_id = 'places_tract'",
    ) == [(0,)]


def test_a_second_run_of_the_same_release_reads_only_the_metadata(
    tract_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-134 -- an unchanged watermark is `unchanged` and writes no row."""
    tract.load_release(tract_warehouse)
    again = tract.FixtureClient()
    release, published = tract.load_release(tract_warehouse, client=again)
    assert (release.decision.value, published) == ("unchanged", 0)
    assert [url for url, _params in again.calls] == [
        "https://data.cdc.gov/api/views/cwsq-ngmh"
    ]
