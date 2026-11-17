"""Earlier-decade tract counts on 2020 tracts, derived and labelled.

Covers: ETL-103
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: (code, vintage). Tract 000100 (2010) split into 000101 and 000102 (2020);
#: 000200 is the same area in both decades; 000300 and 000400 (2010) merged
#: into 000500 (2020); 000600 (2000) became part of 000700 (2010), which is
#: unchanged in 2020.
TRACTS = {
    "000100": 2010,
    "000101": 2020,
    "000102": 2020,
    "000200": 2010,
    "000300": 2010,
    "000400": 2010,
    "000500": 2020,
    "000600": 2000,
    "000700": 2010,
}
#: (earlier vintage, earlier, later, earlier land share, later land share,
#: earlier population share)
STEPS = [
    (2010, "000100", "000101", "0.6", "1", None),
    (2010, "000100", "000102", "0.4", "1", None),
    (2010, "000200", "000200", "1", "1", None),
    (2010, "000300", "000500", "1", "0.5", None),
    (2010, "000400", "000500", "1", "0.5", None),
    (2000, "000600", "000700", "0.7", "0.3", "0.5"),
    (2010, "000700", "000700", "1", "1", None),
]
#: (code, year, table, variable, estimate)
PUBLISHED = [
    ("000100", 2015, "B01003", "B01003_001", "1000"),
    ("000100", 2015, "B19013", "B19013_001", "52000"),
    ("000200", 2015, "B01003", "B01003_001", "500"),
    ("000300", 2015, "B01003", "B01003_001", "300"),
    ("000400", 2015, "B01003", "B01003_001", None),
    ("000600", 2009, "B01003", "B01003_001", "800"),
]


def _geo_id(code: str) -> str:
    return f"state:98|county:001|tract:{code}"


@pytest.fixture
def decades(postgres_connection_factory: Callable[[], connection]):
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            sk = {}
            for code, vintage in TRACTS.items():
                sk[code] = seed_geography(
                    cursor,
                    geo_type="tract",
                    state_fips="98",
                    county_fips="001",
                    tract_code=code,
                    vintage=vintage,
                    name=f"Census Tract {code}",
                )
            capture_id = seed_capture(cursor, "CENSUS_GEO")
            for earlier_vintage, earlier, later, e_share, l_share, p_share in STEPS:
                cursor.execute(
                    """
                    INSERT INTO silver_ref.tract_decade_crosswalk (
                        earlier_vintage, later_vintage, earlier_geo_sk, later_geo_sk,
                        land_area_part_m2, earlier_land_share, later_land_share,
                        earlier_population_share, source_snapshot_id
                    ) VALUES (%s, %s, %s, %s, 1, %s, %s, %s, %s)
                    """,
                    (
                        earlier_vintage,
                        earlier_vintage + 10,
                        sk[earlier],
                        sk[later],
                        e_share,
                        l_share,
                        p_share,
                        str(capture_id),
                    ),
                )
            for code, year, table, variable, estimate in PUBLISHED:
                cursor.execute(
                    """
                    INSERT INTO gold_census.rpt_acs_observations (
                        observation_date, as_of_date, updated_at, geo_id, geo_level,
                        state_fips, value, dataset_code, vintage_year, table_id,
                        variable_code, estimate_value, metric_code, value_status
                    ) VALUES (MAKE_DATE(%s, 1, 1), MAKE_DATE(%s, 1, 1), NOW(), %s,
                              'TRACT', '98', %s, 'acs5', %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        year,
                        year,
                        _geo_id(code),
                        estimate,
                        year,
                        table,
                        variable,
                        estimate,
                        f"CENSUS_ACS:acs5:{variable}",
                        "valid" if estimate is not None else "absent",
                    ),
                )
        writer.commit()
    finally:
        writer.close()
    yield
    remover = postgres_connection_factory()
    try:
        with remover.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gold_census.rpt_acs_observations WHERE state_fips = '98' "
                "AND geo_level = 'TRACT'"
            )
            cursor.execute(
                "DELETE FROM silver_ref.tract_decade_crosswalk WHERE source_snapshot_id = %s",
                (str(capture_id),),
            )
            for code in TRACTS:
                delete_geography(cursor, _geo_id(code))
        remover.commit()
    finally:
        remover.close()


def test_earlier_tract_counts_are_apportioned_to_2020_tracts(
    postgres_connection_factory: Callable[[], connection], decades
) -> None:
    """Covers: ETL-103 -- split, unchanged, merged-with-a-gap and two-decade cases."""
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT geo_id, vintage_year, variable_code, comparable_value,
                       comparable_status, weight_basis, source_tract_count, derivation
                FROM gold_census.acs_tract_comparable
                WHERE geo_id LIKE 'state:98|%%'
                ORDER BY geo_id, vintage_year
                """
            )
            rows = {(row[0][-6:], row[1]): row[2:] for row in cursor.fetchall()}
            cursor.execute(
                "SELECT COUNT(*) FROM gold_census.rpt_acs_observations "
                "WHERE state_fips = '98' AND geo_level = 'TRACT'"
            )
            published = cursor.fetchone()[0]
    finally:
        reader.close()
    land = "share of land area"
    assert rows[("000101", 2015)] == (
        "B01003_001",
        Decimal("600.0"),
        "apportioned",
        land,
        1,
        "decade_crosswalk",
    )
    assert rows[("000102", 2015)][1:3] == (Decimal("400.0"), "apportioned")
    assert rows[("000200", 2015)][1:3] == (Decimal("500"), "unchanged")
    # A merged tract one of whose parts was not published has no value.
    assert rows[("000500", 2015)][1:4] == (None, "incomplete", land)
    # 2009 (Census 2000 tracts): half the 2010 population, then all of it.
    assert rows[("000700", 2009)][1:4] == (
        Decimal("400.0"),
        "apportioned",
        "share of 2010 population, then share of land area",
    )
    # A median is never apportioned, and the published rows are untouched.
    assert all(key[1] in {2009, 2015} for key in rows)
    assert not [row for row in rows.values() if row[0] == "B19013_001"]
    assert published == len(PUBLISHED)
