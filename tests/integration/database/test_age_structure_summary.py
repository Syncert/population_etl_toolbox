"""The age structure summary is built from the served lines, a release at a time.

Covers: DB-068 -- `age_summary.build_release` reads one release's B01001 and
        B01002 lines from `gold_census.rpt_acs_observations` a state at a time
        (plus the rows no state holds), and replaces that release's rows of
        `gold_census.age_structure_summary` in one transaction, recording the
        partition watermark it read in `age_structure_summary_build`;
        `list_changed_releases` offers a release again only once its year
        partition has changed since, and the map read's statement is an index
        scan on `ix_age_structure_summary_release_level_state`.

The geographies are fictional (state 96) and the release is one no other suite
seeds, so the assertions name only rows this test wrote.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_acs.gold_census import age_summary
from data_ingestion_toolbox.semantics.age_bands import default_scheme

pytestmark = [pytest.mark.integration, pytest.mark.database]

VINTAGE = 2007
DATASET = "acs5"
COUNTY = "state:96|county:901"
OTHER = "state:96|county:903"
SCHEME = default_scheme()


def _lines(scale: int) -> dict[str, tuple[int, int | None]]:
    values: dict[str, tuple[int, int | None]] = {}
    men = women = 0
    for n in range(3, 26):
        values[f"B01001_{n:03d}"] = (scale, 40)
        men += scale
    for n in range(27, 50):
        values[f"B01001_{n:03d}"] = (scale, 40)
        women += scale
    values["B01001_002"] = (men, 60)
    values["B01001_026"] = (women, 60)
    values["B01001_001"] = (men + women, 90)
    values["B01002_001"] = (38, 1)
    values["B01002_002"] = (37, 1)
    values["B01002_003"] = (39, 1)
    return values


class _Hook:
    def __init__(self, factory: Callable[[], connection]) -> None:
        self._factory = factory

    def get_conn(self) -> connection:
        return self._factory()


def _insert(cursor, geo_id: str, scale: int, updated: str) -> None:
    for variable, (value, moe) in _lines(scale).items():
        cursor.execute(
            """
            INSERT INTO gold_census.rpt_acs_observations (
                observation_date, as_of_date, updated_at, geo_id, geo_level,
                state_fips, value, dataset_code, vintage_year, table_id,
                variable_code, estimate_value, margin_of_error, metric_code,
                duration_start, duration_end
            ) VALUES (
                MAKE_DATE(%(v)s, 1, 1), MAKE_DATE(%(v)s, 1, 1),
                %(updated)s::TIMESTAMPTZ, %(geo)s, 'COUNTY', '96', %(value)s,
                %(dataset)s, %(v)s, %(table)s, %(variable)s, %(value)s,
                %(moe)s, %(metric)s, MAKE_DATE(%(v)s - 4, 1, 1),
                MAKE_DATE(%(v)s, 12, 31)
            )
            """,
            {
                "v": VINTAGE,
                "updated": updated,
                "geo": geo_id,
                "value": value,
                "dataset": DATASET,
                "table": variable.split("_")[0],
                "variable": variable,
                "moe": moe,
                "metric": f"CENSUS_ACS:{DATASET}:{variable}",
            },
        )


@pytest.fixture
def seeded(postgres_connection_factory) -> Iterator[Callable[[], connection]]:
    def clean() -> None:
        database = postgres_connection_factory()
        database.autocommit = True
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gold_census.rpt_acs_observations "
                "WHERE geo_id = ANY(%s) AND observation_date = MAKE_DATE(%s, 1, 1)",
                ([COUNTY, OTHER], VINTAGE),
            )
            cursor.execute(
                "DELETE FROM gold_census.age_structure_summary "
                "WHERE dataset_code = %s AND vintage_year = %s",
                (DATASET, VINTAGE),
            )
            cursor.execute(
                "DELETE FROM gold_census.age_structure_summary_build "
                "WHERE dataset_code = %s AND vintage_year = %s",
                (DATASET, VINTAGE),
            )
            cursor.execute(
                "DELETE FROM gold_census.dim_acs_table "
                "WHERE dataset_code = %s AND vintage_year = %s AND table_id = 'B01001'",
                (DATASET, VINTAGE),
            )
        database.close()

    clean()
    database = postgres_connection_factory()
    database.autocommit = True
    with database.cursor() as cursor:
        _insert(cursor, COUNTY, 100, "2026-01-01")
        _insert(cursor, OTHER, 300, "2026-01-01")
        cursor.execute(
            "INSERT INTO gold_census.dim_acs_table "
            "(dataset_code, vintage_year, table_id, survey_span_years) "
            "VALUES (%s, %s, 'B01001', 5) ON CONFLICT DO NOTHING",
            (DATASET, VINTAGE),
        )
    database.close()
    yield postgres_connection_factory
    clean()


def _summary(factory) -> dict[str, dict]:
    database = factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id, state_fips, old_age_dependency_ratio, "
                "age_65_plus_share, median_age, observation_date "
                "FROM gold_census.age_structure_summary "
                "WHERE dataset_code = %s AND vintage_year = %s AND geo_id = ANY(%s)",
                (DATASET, VINTAGE, [COUNTY, OTHER]),
            )
            names = [c[0] for c in cursor.description]
            return {row[0]: dict(zip(names, row)) for row in cursor.fetchall()}
    finally:
        database.close()


def _changed(hook) -> list[tuple[str, int]]:
    return [
        (r.dataset_code, r.vintage_year)
        for r in age_summary.list_changed_releases(hook, SCHEME)
    ]


def test_a_release_is_built_from_its_served_lines_and_rebuilt_only_when_they_change(
    seeded,
) -> None:
    """Covers: DB-068"""
    hook = _Hook(seeded)
    assert (DATASET, VINTAGE) in _changed(hook)
    (release,) = [
        r
        for r in age_summary.list_changed_releases(hook, SCHEME)
        if (r.dataset_code, r.vintage_year) == (DATASET, VINTAGE)
    ]
    written = age_summary.build_release(
        hook, release, scheme=SCHEME, states=["96"], workers=2
    )
    assert written >= 2
    rows = _summary(seeded)
    assert set(rows) == {COUNTY, OTHER}
    # Six 65+ lines per sex of 23: 1,200 of 4,600 people; 18 to 64 is
    # thirteen lines per sex, 2,600 people.
    assert rows[COUNTY]["state_fips"] == "96"
    assert round(float(rows[COUNTY]["age_65_plus_share"]), 2) == round(
        1200 / 4600 * 100, 2
    )
    assert round(float(rows[COUNTY]["old_age_dependency_ratio"]), 1) == round(
        1200 / 2600 * 100, 1
    )
    assert rows[COUNTY]["median_age"] == 38
    assert rows[COUNTY]["observation_date"] == date(VINTAGE, 1, 1)
    assert (DATASET, VINTAGE) not in _changed(hook), "built, and nothing changed"

    database = seeded()
    database.autocommit = True
    with database.cursor() as cursor:
        cursor.execute(
            "UPDATE gold_census.rpt_acs_observations SET updated_at = NOW() "
            "WHERE geo_id = %s AND observation_date = MAKE_DATE(%s, 1, 1)",
            (OTHER, VINTAGE),
        )
    database.close()
    assert (DATASET, VINTAGE) in _changed(hook), "its partition moved"


def test_the_map_read_is_an_index_scan(seeded) -> None:
    """Covers: DB-068"""
    database = seeded()
    try:
        with database.cursor() as cursor:
            cursor.execute("SET LOCAL enable_seqscan = off")
            cursor.execute(
                "EXPLAIN SELECT geo_id, old_age_dependency_ratio "
                "FROM gold_census.age_structure_summary "
                "WHERE dataset_code = 'acs5' AND vintage_year = 2007 "
                "AND geo_level = 'COUNTY' AND state_fips = '96'"
            )
            plan = "\n".join(row[0] for row in cursor.fetchall())
    finally:
        database.rollback()
        database.close()
    assert "ix_age_structure_summary_release_level_state" in plan
