"""Real PostgreSQL CDC FluView ILINet capture-to-gold contract.

Covers: ETL-136
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc_ilinet.client import CdcIlinetFetchError
from tests.support import cdc_ilinet as ilinet

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIRST_LOAD_FIGURES = (3 + 4 + 10) * 11


@pytest.fixture
def ilinet_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ilinet.reviewed_warehouse(postgres_connection_factory, request)


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
    factory: Callable[[], connection], measure: str, geo_id: str, week_start: str
):
    rows = _rows(
        factory,
        """
        SELECT value, value_status, value_source, mmwr_year, mmwr_week, geography_status
        FROM gold_cdc_ilinet.area_latest
        WHERE metric_key = %s AND geo_id = %s AND period_start = %s::DATE
        """,
        (f"ilinet:{measure}", geo_id, week_start),
    )
    return rows[0] if rows else None


def _zip_with(name: str, old: str, new: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(ilinet.fixture_bytes(name))) as archive:
        text = archive.read("ILINet.csv").decode("utf-8")
    assert old in text
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("ILINet.csv", text.replace(old, new))
    return buffer.getvalue()


def test_wisconsin_and_the_nation_show_cdc_figures_as_published(
    ilinet_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-136 -- every row stored; Wisconsin 2025 week 1 shows CDC's figures."""
    client = ilinet.FixtureClient()
    release_id, changed, published = ilinet.load_release(
        ilinet_warehouse, client=client
    )
    assert release_id is not None
    assert (changed, published) == (FIRST_LOAD_FIGURES, 1)
    assert _rows(
        ilinet_warehouse, "SELECT COUNT(*) FROM silver_cdc_ilinet.quarantine"
    ) == [(0,)]
    assert _rows(
        ilinet_warehouse,
        "SELECT row_count, kept_row_count, changed_value_count, status "
        "FROM control.cdc_ilinet_release",
    ) == [(17, 17, FIRST_LOAD_FIGURES, "published")]

    posts = {
        body["RegionTypeId"]: body
        for method, _url, body in client.calls
        if method == "POST"
    }
    assert [season["ID"] for season in posts[3]["SeasonsDT"]] == [49, 64, 65]
    assert [season["ID"] for season in posts[5]["SeasonsDT"]] == [64, 65]
    assert [region["ID"] for region in posts[1]["SubRegionsDT"]] == list(range(1, 11))
    assert posts[3]["SubRegionsDT"] == [{"ID": 0, "Name": ""}]

    week_1 = "2024-12-29"
    wisconsin = ilinet.WISCONSIN
    assert _latest(ilinet_warehouse, "unweighted_ili_pct", wisconsin, week_1) == (
        Decimal("3.04037"),
        "valid",
        "3.04037",
        2025,
        1,
        "resolved",
    )
    assert _latest(ilinet_warehouse, "ili_visits_total", wisconsin, week_1)[0] == 961
    assert _latest(ilinet_warehouse, "reporting_providers", wisconsin, week_1)[0] == 65
    assert (
        _latest(ilinet_warehouse, "patient_visits_total", wisconsin, week_1)[0] == 31608
    )
    assert _latest(ilinet_warehouse, "weighted_ili_pct", wisconsin, week_1)[:3] == (
        None,
        "not_published",
        "X",
    )
    nation = ilinet.NATION
    assert _latest(ilinet_warehouse, "weighted_ili_pct", nation, week_1)[0] == Decimal(
        "6.25398"
    )
    assert _latest(ilinet_warehouse, "ili_visits_age_0_4", nation, week_1)[0] == 33451
    assert _latest(ilinet_warehouse, "ili_visits_age_25_64", nation, week_1)[:2] == (
        None,
        "not_published",
    )
    assert _latest(ilinet_warehouse, "weighted_ili_pct", nation, "2024-12-22")[
        0
    ] == Decimal("6.74349")


def test_regions_this_site_does_not_serve_are_kept_with_a_reason(
    ilinet_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-136 -- HHS regions and New York City kept in silver, not served."""
    ilinet.load_release(ilinet_warehouse)
    regions = dict(
        ((region_type, region), (geo_id, status, note))
        for region_type, region, geo_id, status, note in _rows(
            ilinet_warehouse,
            """
            SELECT region_type, region, geo_id, geography_status, jurisdiction_note
            FROM silver_cdc_ilinet.region
            """,
        )
    )
    assert regions[("hhs_region", "Region 5")][:2] == (None, "unsupported")
    assert regions[("state", "New York City")][:2] == (None, "unsupported")
    assert "beside New York" in regions[("state", "New York City")][2]
    assert regions[("state", "New York")][:2] == (ilinet.NEW_YORK, "resolved")
    assert "New York City" in regions[("state", "New York")][2]
    assert regions[("state", "California")][:2] == ("state:06", "unmapped")
    assert regions[("state", "Puerto Rico")][:2] == ("state:72", "unmapped")
    assert regions[("national", "National")][:2] == (ilinet.NATION, "resolved")
    assert _rows(
        ilinet_warehouse,
        "SELECT COUNT(*) FROM gold_cdc_ilinet.area_revision "
        "WHERE source_region IN ('Region 2', 'Region 5', 'New York City')",
    ) == [(0,)]
    hhs = _rows(
        ilinet_warehouse,
        """
        SELECT value_status, value_source FROM silver_cdc_ilinet.week_value
        WHERE region = 'Region 5' AND mmwr_year = 2025 AND mmwr_week = 1
          AND measure_id = 'ili_visits_age_25_64'
        """,
    )
    assert hhs == [("missing", None)]


def test_a_repeated_release_writes_nothing_and_a_revision_writes_only_the_change(
    ilinet_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-136 -- unchanged files replay nothing; a revised figure adds one row."""
    first, _changed, _published = ilinet.load_release(ilinet_warehouse)
    assert ilinet.load_release(ilinet_warehouse) == (None, 0, 0)
    assert _rows(
        ilinet_warehouse,
        "SELECT status, COUNT(*) FROM control.cdc_ilinet_release GROUP BY status ORDER BY 1",
    ) == [("published", 1), ("unchanged", 1)]

    revised = _zip_with(
        "ilinet_state.zip",
        "States,Wisconsin,2025,2,X,2.93977,X,X,X,X,X,X,920,66,31295",
        "States,Wisconsin,2025,2,X,2.93977,X,X,X,X,X,X,925,66,31295",
    )
    client = ilinet.FixtureClient({5: httpx.Response(200, content=revised)})
    second, changed, published = ilinet.load_release(ilinet_warehouse, client=client)
    assert second is not None and second != first
    assert (changed, published) == (1, 1)
    assert (
        _latest(ilinet_warehouse, "ili_visits_total", ilinet.WISCONSIN, "2025-01-05")[0]
        == 925
    )
    assert _rows(
        ilinet_warehouse,
        """
        SELECT value FROM gold_cdc_ilinet.area_revision
        WHERE metric_key = 'ilinet:ili_visits_total' AND geo_id = %s
          AND period_start = '2025-01-05'
        ORDER BY published_at
        """,
        (ilinet.WISCONSIN,),
    ) == [(Decimal("920"),), (Decimal("925"),)]
    assert (
        _latest(ilinet_warehouse, "ili_visits_total", ilinet.WISCONSIN, "2024-12-29")[0]
        == 961
    )


def test_a_refused_download_fails_the_release_and_publishes_nothing(
    ilinet_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-136 -- a refusal is reported, never worked around."""
    client = ilinet.FixtureClient({1: httpx.Response(403, content=b"")})
    with pytest.raises(CdcIlinetFetchError) as raised:
        ilinet.load_release(ilinet_warehouse, client=client)
    assert raised.value.status == 403
    assert _rows(ilinet_warehouse, "SELECT status FROM control.cdc_ilinet_release") == [
        ("failed",)
    ]
    assert _rows(
        ilinet_warehouse, "SELECT COUNT(*) FROM gold_cdc_ilinet.area_revision"
    ) == [(0,)]
    posts = [body for method, _url, body in client.calls if method == "POST"]
    assert len(posts) == 2


def test_a_changed_layout_sets_the_release_aside_whole(
    ilinet_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-136 -- an unregistered header quarantines the release."""
    changed_header = _zip_with("ilinet_national.zip", "TOTAL PATIENTS", "PATIENTS")
    client = ilinet.FixtureClient({3: httpx.Response(200, content=changed_header)})
    release_id, changed, published = ilinet.load_release(
        ilinet_warehouse, client=client
    )
    assert release_id is not None
    assert (changed, published) == (0, 0)
    assert _rows(ilinet_warehouse, "SELECT status FROM control.cdc_ilinet_release") == [
        ("quarantined",)
    ]
    assert _rows(
        ilinet_warehouse, "SELECT error_code FROM silver_cdc_ilinet.quarantine"
    ) == [("unregistered_layout",)]
    assert _rows(
        ilinet_warehouse, "SELECT COUNT(*) FROM gold_cdc_ilinet.area_revision"
    ) == [(0,)]
