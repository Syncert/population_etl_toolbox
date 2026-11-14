"""Real PostgreSQL 2020 Census capture-to-gold contract.

Covers: ETL-129
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_decennial.registry import PL, DecennialSlice
from data_ingestion_toolbox.census_decennial.schema import (
    REQUIRED_RELATIONS,
    ensure_census_decennial_schema,
)
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.quality.sources import (
    decennial_part_sums,
    decennial_tract_county_sums,
)
from tests.support import census_decennial

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def decennial_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return census_decennial.reviewed_warehouse(postgres_connection_factory, request)


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


def _fixture_row_count() -> int:
    return sum(
        len(census_decennial.fixture_rows(slice_.geo_level, slice_.scope))
        for slice_ in census_decennial.FIXTURE_SLICES
    )


def test_every_fixture_row_reaches_silver_and_resolved_areas_reach_gold(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 -- each count is the published integer; only seeded areas are served."""
    _run_id, facts, published = census_decennial.run_to_gold(decennial_warehouse)
    assert facts == _fixture_row_count() * len(PL.variables)
    assert published == len(census_decennial.FIXTURE_SLICES)

    served = _rows(
        decennial_warehouse,
        """
        SELECT geo_level, COUNT(DISTINCT geo_id)
        FROM gold_census_decennial.count_latest
        WHERE geography_status = 'resolved'
        GROUP BY geo_level ORDER BY geo_level
        """,
    )
    assert served == [
        ("COUNTY", 1),
        ("NATIONAL", 1),
        ("STATE", 1),
        ("TRACT", len(census_decennial.dane_tract_codes())),
    ]
    dane = dict(
        _rows(
            decennial_warehouse,
            """
            SELECT variable_id, value::BIGINT FROM gold_census_decennial.count_latest
            WHERE geo_id = %s
            """,
            (census_decennial.DANE,),
        )
    )
    assert dane["P5_001N"] == 17465
    assert dane["P5_003N"] == 1541
    assert dane["P5_005N"] == 1806
    assert dane["P5_008N"] == 9925
    assert dane["P1_001N"] == 561504
    assert (dane["H1_001N"], dane["H1_002N"], dane["H1_003N"]) == (
        248795,
        238417,
        10378,
    )
    period = _rows(
        decennial_warehouse,
        "SELECT DISTINCT period_start::TEXT, period_end::TEXT FROM gold_census_decennial.count_latest",
    )
    assert period == [("2020-04-01", "2020-04-01")]


def test_parts_and_tracts_add_to_their_published_totals(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 -- DQ-DECENNIAL-003 and -004 pass on the Bureau's own figures."""
    reader = decennial_warehouse()
    try:
        with reader.cursor() as cursor:
            assert decennial_part_sums(cursor, {})[0].result == "not_applicable"
    finally:
        reader.close()
    census_decennial.run_to_gold(decennial_warehouse)
    reader = decennial_warehouse()
    try:
        with reader.cursor() as cursor:
            assert decennial_part_sums(cursor, {})[0].result == "pass"
            assert decennial_tract_county_sums(cursor, {})[0].result == "pass"
    finally:
        reader.close()


def test_publisher_names_every_variable_and_harvests(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 -- one glossary row per variable, grains derived from served rows."""
    census_decennial.run_to_gold(decennial_warehouse)
    published = _rows(
        decennial_warehouse,
        """
        SELECT source_object_key, valid_geo_grains
        FROM gold_census_decennial.metric_publisher ORDER BY source_object_key
        """,
    )
    assert [row[0] for row in published] == sorted(
        f"pl:{variable.variable_id}" for variable in PL.variables
    )
    for _key, grains in published:
        assert grains == ["COUNTY", "NATIONAL", "STATE", "TRACT"]
    assert harvest_publisher(
        decennial_warehouse, Publisher("gold_census_decennial")
    ) == len(PL.variables)


def test_rerun_with_the_same_bytes_writes_nothing(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 -- the churn rule: an unchanged answer is recorded, not stored again."""
    census_decennial.run_to_gold(decennial_warehouse)
    second_run, facts, _published = census_decennial.run_to_gold(decennial_warehouse)
    assert facts == 0
    assert _rows(
        decennial_warehouse,
        "SELECT DISTINCT status FROM control.census_decennial_slice WHERE run_id = %s",
        (str(second_run),),
    ) == [("unchanged",)]


def test_a_missing_count_is_missing_and_a_stray_row_is_set_aside(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 -- a null is never zero; a row of another state is quarantined alone."""
    rows = json.loads(census_decennial.fixture_bytes("tract", "55"))
    header = rows[0]
    rows[1][header.index("P5_008N")] = None
    rows[2][header.index("state")] = "17"
    override = httpx.Response(200, content=json.dumps(rows).encode())
    run_id, _facts, _published = census_decennial.run_to_gold(
        decennial_warehouse,
        client=census_decennial.FixtureClient({"tract": override}),
        slices=(DecennialSlice("tract", "55"),),
    )
    assert _rows(
        decennial_warehouse,
        "SELECT source_row_index, error_code FROM silver_census_decennial.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [(2, "unreadable_geography")]
    first_tract = f"{census_decennial.DANE}|tract:{rows[1][header.index('tract')]}"
    assert _rows(
        decennial_warehouse,
        """
        SELECT value, value_status FROM gold_census_decennial.count_latest
        WHERE geo_id = %s AND variable_id = 'P5_008N'
        """,
        (first_tract,),
    ) == [(None, "missing")]


def test_a_block_is_kept_unsupported_and_never_served(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 -- blocks wait for a block geography; the DDL is rerunnable."""
    block_rows = [
        [*json.loads(census_decennial.fixture_bytes("tract", "55"))[0], "block"],
        [
            "Block 1000, Block Group 1, Census Tract 1, Dane County, Wisconsin",
            *(["0"] * 10),
            "12",
            "5",
            "5",
            "0",
            "55",
            "025",
            "000100",
            "1000",
        ],
    ]
    override = httpx.Response(200, content=json.dumps(block_rows).encode())
    run_id, facts, _published = census_decennial.run_to_gold(
        decennial_warehouse,
        client=census_decennial.FixtureClient({"block": override}),
        slices=(DecennialSlice("block", "55025"),),
    )
    assert facts == len(PL.variables)
    assert _rows(
        decennial_warehouse,
        "SELECT DISTINCT geography_status FROM silver_census_decennial.fact_count WHERE run_id = %s",
        (str(run_id),),
    ) == [("unsupported",)]
    assert _rows(
        decennial_warehouse,
        "SELECT COUNT(*) FROM gold_census_decennial.count_revision WHERE geo_id LIKE '%%block%%'",
    ) == [(0,)]

    class _Hook:
        def get_conn(self) -> connection:
            return decennial_warehouse()

    ensure_census_decennial_schema(_Hook())
    present = _rows(
        decennial_warehouse,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    )
    assert present == [(len(REQUIRED_RELATIONS),)]
