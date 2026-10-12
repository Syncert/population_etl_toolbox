"""Regional price parities reach gold at states, CBSAs and BEA's portions.

Covers: ETL-079
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.sources import bea_price_parity_reference
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
)
from data_ingestion_toolbox.silver_ref.provider_areas import (
    parse_bea_portions,
    provider_area_records,
)
from tests.support import bea
from tests.support.capture_seed import delete_geography, seed_capture

pytestmark = [pytest.mark.integration, pytest.mark.database]

AREAS = ("cbsa:20100", "area:bea:00999", "area:bea:10998", "area:bea:10999")


@pytest.fixture
def parity_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    factory = bea.reviewed_warehouse(postgres_connection_factory, request)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()
    finally:
        writer.close()
    records = [
        GeographyRecord(
            "metro",
            "cbsa:20100",
            "20100",
            None,
            None,
            None,
            "Dover, DE",
            2023,
            lsad="Metropolitan Statistical Area",
            area_code="20100",
        ),
        *provider_area_records(
            "bea",
            parse_bea_portions(bea.fixture_bytes("PARPP"))
            + parse_bea_portions(bea.fixture_bytes("MARPP")),
            vintage=2026,
        ),
    ]
    GeographyRepository(factory).load_attributes(records, capture_id=capture_id)

    def cleanup() -> None:
        remover = factory()
        try:
            with remover.cursor() as cursor:
                for geo_id in AREAS:
                    delete_geography(cursor, geo_id)
            remover.commit()
        finally:
            remover.close()

    # Registered after the BEA cleanup, so it runs first: facts go before areas.
    request.addfinalizer(cleanup)
    return factory


def _rows(factory: Callable[[], connection], sql: str, parameters: tuple = ()) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_parities_are_served_at_states_metros_and_portions(parity_warehouse) -> None:
    """Covers: ETL-079 — by code, with the nation at 100 and an absent portion not a zero."""
    factory = parity_warehouse
    for code in ("SARPP", "MARPP", "PARPP"):
        _run, facts, published = bea.run_to_gold(factory, code)
        assert facts > 0 and published > 0

    served = _rows(
        factory,
        """
        SELECT table_code, geo_id, geo_level, geography_status
        FROM gold_bea.observation_latest
        WHERE table_code IN ('SARPP', 'MARPP', 'PARPP') AND line_code = '1' AND year = 2024
        ORDER BY 1, 2
        """,
    )
    assert ("MARPP", "cbsa:20100", "METRO", "resolved") in served
    assert ("MARPP", "area:bea:00999", "PROVIDER_AREA", "resolved") in served
    assert ("PARPP", "area:bea:10998", "PROVIDER_AREA", "resolved") in served
    assert ("SARPP", "state:10", "STATE", "resolved") in served
    # Abilene is a CBSA this warehouse does not hold: served under its own
    # code as `unmapped`, as every BEA row is, and ledgered rather than guessed.
    assert ("MARPP", "cbsa:10180", "METRO", "unmapped") in served
    assert _rows(
        factory,
        """
        SELECT status FROM silver_ref.geography_resolution
        WHERE provider_source = 'BEA' AND provider_dataset = 'MARPP'
          AND source_geo_type = 'metro' AND source_code = '10180'
        """,
    ) == [("unmapped",)]

    nonmetro = _rows(
        factory,
        """
        SELECT DISTINCT value, value_status, value_source FROM gold_bea.observation_latest
        WHERE table_code = 'PARPP' AND geo_id = 'area:bea:10999'
        """,
    )
    assert nonmetro == [(None, "not_meaningful", "0.000")]

    nation = _rows(
        factory,
        """
        SELECT DISTINCT value FROM gold_bea.observation_latest
        WHERE table_code = 'SARPP' AND geo_id = 'us:1' AND line_code = '1'
        """,
    )
    assert nation == [(Decimal("100.000"),)]
    basis = _rows(
        factory,
        "SELECT DISTINCT dollar_basis, observation_basis FROM silver_bea.dim_line "
        "WHERE table_code = 'MARPP'",
    )
    assert len(basis) == 1 and basis[0][0] == "price_level_us_100"
    assert "food is inside goods" in basis[0][1]

    reader = factory()
    try:
        with reader.cursor() as cursor:
            (passing,) = bea_price_parity_reference(cursor, {})
            cursor.execute(
                "UPDATE silver_bea.fact_observation SET value = 99.5, value_source = '99.500' "
                "WHERE table_code = 'SARPP' AND geo_id = 'us:1' AND line_code = '1' AND year = 2020"
            )
            (failing,) = bea_price_parity_reference(cursor, {})
        reader.rollback()
    finally:
        reader.close()
    assert passing.result == "pass"
    assert failing.result == "fail" and failing.observed_count == 1
    assert failing.evidence == ["SARPP|2020|99.500"]
