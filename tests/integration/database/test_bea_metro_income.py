"""Metro areas summed from BEA's counties reach gold, marked as derived.

Covers: ETL-101
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
)
from tests.support import bea
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

METROS = {"20100": "Dover, DE", "41540": "Salisbury, MD-DE"}
#: Dover is Kent County alone; Salisbury adds a Maryland county BEA's
#: fixture does not carry, so its sum would be partial.
MEMBERS = {"20100": ("10001",), "41540": ("10005", "24045")}
OWNED = ("cbsa:20100", "cbsa:41540", "state:24|county:045")


@pytest.fixture
def metro_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    factory = bea.reviewed_warehouse(postgres_connection_factory, request)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            capture_id = seed_capture(cursor, "CENSUS_GEO")
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="24",
                county_fips="045",
                vintage=2024,
                name="Wicomico County",
            )
        writer.commit()
    finally:
        writer.close()
    GeographyRepository(factory).load_attributes(
        [
            GeographyRecord(
                "metro",
                f"cbsa:{code}",
                code,
                None,
                None,
                None,
                name,
                2023,
                lsad="Metropolitan Statistical Area",
                area_code=code,
            )
            for code, name in METROS.items()
        ],
        capture_id=capture_id,
    )
    writer = factory()
    try:
        with writer.cursor() as cursor:
            for metro, counties in MEMBERS.items():
                for county in counties:
                    cursor.execute(
                        """
                        INSERT INTO silver_ref.bridge_geo_relationship_version (
                            parent_geo_sk, related_geo_sk, relationship_type,
                            geography_vintage, evidence_source, source_snapshot_id
                        )
                        SELECT parent.geo_sk, member.geo_sk, 'contains', 2023,
                               'census_cbsa_delineation', %s
                        FROM silver_ref.dim_geo_entity AS parent,
                             silver_ref.dim_geo_entity AS member
                        WHERE parent.geo_id = %s AND member.geo_id = %s
                        """,
                        (
                            str(capture_id),
                            f"cbsa:{metro}",
                            f"state:{county[:2]}|county:{county[2:]}",
                        ),
                    )
        writer.commit()
    finally:
        writer.close()

    def cleanup() -> None:
        remover = factory()
        try:
            with remover.cursor() as cursor:
                for geo_id in OWNED:
                    delete_geography(cursor, geo_id)
            remover.commit()
        finally:
            remover.close()

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


def test_metro_income_and_gdp_are_summed_from_counties(metro_warehouse) -> None:
    """Covers: ETL-101 — summed, marked derived, partial sums refused, chained GDP not derived."""
    factory = metro_warehouse
    for code in ("CAINC1", "CAGDP1", "MAIRPD"):
        _run, facts, published = bea.run_to_gold(factory, code)
        assert facts > 0 and published == 1

    served = {
        (row[0], row[1]): row[2:]
        for row in _rows(
            factory,
            """
            SELECT metric_key, geo_id, value, value_status, derivation, geo_level,
                   geography_status
            FROM gold_bea.observation_latest
            WHERE year = 2024 AND metric_key IN ('CAINC1:1', 'CAINC1:3', 'CAGDP1:1', 'CAGDP1:3')
            """,
        )
    }
    kent = "state:10|county:001"
    for key in ("CAINC1:1", "CAINC1:3", "CAGDP1:3"):
        dover = served[(key, "cbsa:20100")]
        assert dover[0] == served[(key, kent)][0]
        assert dover[1:] == (
            "valid",
            "sum_of_member_counties"
            if key != "CAINC1:3"
            else "ratio_of_member_county_sums",
            "METRO",
            "resolved",
        )
        assert served[(key, kent)][2] is None
        salisbury = served[(key, "cbsa:41540")]
        assert salisbury[:2] == (None, "not_available")
    assert ("CAGDP1:1", "cbsa:20100") not in served

    grains = _rows(
        factory,
        "SELECT valid_geo_grains FROM gold_bea.metric_publisher "
        "WHERE source_object_key IN ('CAINC1:1', 'CAGDP1:1') ORDER BY source_object_key",
    )
    assert grains == [
        (["COUNTY", "NATIONAL", "STATE"],),
        (["COUNTY", "METRO", "NATIONAL", "STATE"],),
    ]

    deflator = _rows(
        factory,
        """
        SELECT value::TEXT, dollar_basis, derivation FROM gold_bea.observation_latest
        WHERE metric_key = 'MAIRPD:1' AND geo_id = 'cbsa:20100' AND year = 2024
        """,
    )
    assert deflator == [("120.583", "price_index", None)]
