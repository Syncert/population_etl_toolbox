"""NDCP county childcare prices from a captured API read to gold, offline.

Covers: DB-073
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.dol_childcare.capture import JSON_MEDIA_TYPE
from data_ingestion_toolbox.dol_childcare.registry import MEASURES
from tests.support import dol_childcare as ndcp

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def ndcp_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ndcp.reviewed_warehouse(postgres_connection_factory, request)


def _price(factory, geo_id: str, metric_key: str, year: int) -> tuple:
    (row,) = ndcp.rows(
        factory,
        """
        SELECT value, value_status, imputed, imputation_code, geo_level, release_key
        FROM gold_dol_childcare.observation_latest
        WHERE metric_key = %s AND geo_id = %s AND year = %s
        """,
        (metric_key, geo_id, year),
    )
    return row


def test_the_fixture_records_reach_gold_for_two_counties_and_two_years(
    ndcp_warehouse,
) -> None:
    """Covers: DB-073 — Dane and Rock 2021-2022 and Los Angeles 2022 are served, imputed
    values flagged and a blank cell missing, every county ledgered."""
    factory = ndcp_warehouse
    run_id, status, facts, published = ndcp.run_to_gold(factory)
    assert (status, published) == ("captured", 1)
    assert facts == 5 * len(MEASURES)

    assert _price(factory, ndcp.DANE, "center_infant", 2022) == (
        Decimal(293),
        "valid",
        False,
        "00000",
        "COUNTY",
        "ndcp2022",
    )
    value, _status, imputed, code, _level, _release = _price(
        factory, ndcp.DANE, "center_infant", 2021
    )
    assert (value, imputed, code) == (Decimal("299.75"), True, "00001")
    value, status_text, imputed, _code, _level, _release = _price(
        factory, ndcp.ROCK, "home_school_age", 2022
    )
    assert value is None and status_text == "missing"
    assert ndcp.rows(
        factory,
        "SELECT COUNT(*) FROM gold_dol_childcare.observation_latest WHERE geo_id = %s",
        (ndcp.LOS_ANGELES,),
    ) == [(len(MEASURES),)]

    assert ndcp.rows(
        factory,
        """
        SELECT captured_row_count, quarantined_row_count, repeated_price_count, status
        FROM control.dol_childcare_file WHERE run_id = %s
        """,
        (str(run_id),),
    ) == [(6, 1, 0, "published")]
    assert ndcp.rows(
        factory,
        "SELECT error_code FROM silver_dol_childcare.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [("unreadable_row",)]
    assert ndcp.rows(
        factory,
        """
        SELECT source_code, status FROM silver_ref.geography_resolution
        WHERE provider_source = 'DOL_CHILDCARE' ORDER BY source_code
        """,
    ) == [("06037", "resolved"), ("55025", "resolved"), ("55105", "resolved")]
    assert ndcp.rows(
        factory,
        """
        SELECT media_type FROM raw_capture.response_capture AS capture
        JOIN control.dol_childcare_file AS file USING (capture_id)
        WHERE file.run_id = %s
        """,
        (str(run_id),),
    ) == [(JSON_MEDIA_TYPE,)]

    publisher = ndcp.rows(
        factory,
        """
        SELECT source_object_key, valid_geo_grains, source_watermark
        FROM gold_dol_childcare.metric_publisher ORDER BY source_object_key
        """,
    )
    assert [row[0] for row in publisher] == sorted(m.metric_key for m in MEASURES)
    assert {tuple(row[1]) for row in publisher} == {("COUNTY",)}
    assert {row[2] for row in publisher} == {"2022"}


def test_a_rerun_over_the_unchanged_records_writes_no_row(ndcp_warehouse) -> None:
    """Covers: DB-073 — CHURN RULE: the second read is unchanged and replays nothing."""
    factory = ndcp_warehouse
    ndcp.run_to_gold(factory)
    before = ndcp.rows(factory, "SELECT COUNT(*) FROM silver_dol_childcare.fact_price")
    run_id, status, facts, published = ndcp.run_to_gold(factory)
    assert (status, facts, published) == ("unchanged", 0, 0)
    assert (
        ndcp.rows(factory, "SELECT COUNT(*) FROM silver_dol_childcare.fact_price")
        == before
    )
    assert ndcp.rows(
        factory,
        "SELECT COUNT(*) FROM silver_dol_childcare.price_revision WHERE run_id = %s",
        (str(run_id),),
    ) == [(0,)]


def test_changed_records_store_only_the_prices_that_changed(ndcp_warehouse) -> None:
    """Covers: DB-073 — a restated price is a second row; every repeat is counted, not stored."""
    factory = ndcp_warehouse
    ndcp.run_to_gold(factory)
    records = [
        ndcp.api_record(MCINFANT=301),
        ndcp.api_record(COUNTY_FIPS_CODE=55105, COUNTY_NAME="Rock County"),
    ]
    run_id, status, facts, published = ndcp.run_to_gold(
        factory, client=ndcp.FixtureClient(records)
    )
    assert (status, published) == ("captured", 1)
    # Dane's 2022 prices in the new records are all 987.5 except infant 301:
    # every one differs from the fixture's, as do Rock's.
    assert facts == 2 * len(MEASURES)
    run_id, status, facts, _published = ndcp.run_to_gold(
        factory,
        client=ndcp.FixtureClient(
            [
                ndcp.api_record(MCINFANT=302),
                ndcp.api_record(COUNTY_FIPS_CODE=55105, COUNTY_NAME="Rock County"),
            ]
        ),
    )
    assert facts == 1
    assert ndcp.rows(
        factory,
        "SELECT repeated_price_count FROM control.dol_childcare_file WHERE run_id = %s",
        (str(run_id),),
    ) == [(2 * len(MEASURES) - 1,)]
    assert _price(factory, ndcp.DANE, "center_infant", 2022)[0] == Decimal(302)
    assert ndcp.rows(
        factory,
        """
        SELECT value FROM gold_dol_childcare.observation_revision
        WHERE metric_key = 'center_infant' AND geo_id = %s AND year = 2022
        ORDER BY retrieved_at
        """,
        (ndcp.DANE,),
    ) == [(Decimal(293),), (Decimal(301),), (Decimal(302),)]


def test_records_with_a_changed_layout_are_quarantined_and_serve_nothing(
    ndcp_warehouse,
) -> None:
    """Covers: DB-073 — records without a registered field leave the read quarantined, with no price."""
    factory = ndcp_warehouse
    record = ndcp.api_record()
    del record["mcsa"]
    run_id, status, facts, published = ndcp.run_to_gold(
        factory, client=ndcp.FixtureClient([record])
    )
    assert (status, facts, published) == ("captured", 0, 0)
    assert ndcp.rows(
        factory,
        "SELECT status FROM control.dol_childcare_file WHERE run_id = %s",
        (str(run_id),),
    ) == [("quarantined",)]
    assert ndcp.rows(
        factory,
        """
        SELECT source_row_index, error_code FROM silver_dol_childcare.observation_quarantine
        WHERE run_id = %s
        """,
        (str(run_id),),
    ) == [(0, "unexpected_header")]
    assert ndcp.rows(
        factory, "SELECT COUNT(*) FROM gold_dol_childcare.metric_publisher"
    ) == [(0,)]
