"""Tract decade successions reach the reference by code and decade.

Covers: ETL-102
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.tract_crosswalk import (
    CrosswalkPart,
    parse_2000_2010,
    publish_crosswalk,
)
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/silver_ref/tract_crosswalk"
#: (tract code, vintages the reference holds it for). 040100 is the same
#: tract in both decades; 040500 split into 040501 and 040502 (and a sliver
#: of 043202, which this reference does not hold).
SEEDED = {
    "040100": (2000, 2010),
    "040500": (2000,),
    "040501": (2010,),
    "040502": (2010,),
}


def _geo_id(code: str) -> str:
    return f"state:10|county:001|tract:{code}"


@pytest.fixture
def decade_reference(postgres_connection_factory: Callable[[], connection]):
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for code, vintages in SEEDED.items():
                for vintage in vintages:
                    seed_geography(
                        cursor,
                        geo_type="tract",
                        state_fips="10",
                        county_fips="001",
                        tract_code=code,
                        vintage=vintage,
                        name=f"Census Tract {code}",
                    )
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()
    finally:
        writer.close()
    yield capture_id
    remover = postgres_connection_factory()
    try:
        with remover.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_ref.tract_decade_crosswalk WHERE source_snapshot_id = %s",
                (str(capture_id),),
            )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_dataset = 'census_tract_relationship'"
            )
            for code in SEEDED:
                delete_geography(cursor, _geo_id(code))
        remover.commit()
    finally:
        remover.close()


def _rows(factory, sql: str, parameters: tuple = ()) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_successions_are_matched_by_code_and_decade(
    postgres_connection_factory: Callable[[], connection], decade_reference
) -> None:
    """Covers: ETL-102 -- weights as published; a code from another decade is unmapped."""
    parts = [
        part
        for part in parse_2000_2010((FIXTURES / "us2010trf.excerpt.txt").read_bytes())
        if part.earlier_geo_id in {_geo_id("040100"), _geo_id("040500")}
    ]
    # 040501 is a 2010 tract; a part naming it as a 2000 tract must not match.
    parts.append(
        CrosswalkPart(2000, _geo_id("040501"), _geo_id("040502"), 1, 1, None, None)
    )
    database = postgres_connection_factory()
    try:
        counts = publish_crosswalk(database, parts, capture_id=decade_reference)
        database.commit()
    finally:
        database.close()
    assert counts == {"parts": 3, "unmapped_tracts": 2}

    stored = _rows(
        postgres_connection_factory,
        """
        SELECT earlier.geo_id, later.geo_id, crosswalk.later_vintage,
               crosswalk.earlier_population_share, crosswalk.later_population_part
        FROM silver_ref.tract_decade_crosswalk AS crosswalk
        JOIN silver_ref.dim_geo_entity AS earlier ON earlier.geo_sk = crosswalk.earlier_geo_sk
        JOIN silver_ref.dim_geo_entity AS later ON later.geo_sk = crosswalk.later_geo_sk
        WHERE crosswalk.source_snapshot_id = %s
        ORDER BY 1, 2
        """,
        (str(decade_reference),),
    )
    assert stored == [
        (_geo_id("040100"), _geo_id("040100"), 2010, Decimal("1.0000"), 6541),
        (_geo_id("040500"), _geo_id("040501"), 2010, Decimal("0.7131"), 4923),
        (_geo_id("040500"), _geo_id("040502"), 2010, Decimal("0.2869"), 1981),
    ]
    assert sorted(
        _rows(
            postgres_connection_factory,
            """
            SELECT source_code, source_vintage, status FROM silver_ref.geography_resolution
            WHERE provider_dataset = 'census_tract_relationship'
            """,
        )
    ) == [
        (_geo_id("040501"), 2000, "unmapped"),
        (_geo_id("043202"), 2010, "unmapped"),
    ]

    # A replay writes no second row.
    database = postgres_connection_factory()
    try:
        publish_crosswalk(database, parts, capture_id=decade_reference)
        database.commit()
    finally:
        database.close()
    assert (
        len(
            _rows(
                postgres_connection_factory,
                "SELECT 1 FROM silver_ref.tract_decade_crosswalk WHERE source_snapshot_id = %s",
                (str(decade_reference),),
            )
        )
        == 3
    )
