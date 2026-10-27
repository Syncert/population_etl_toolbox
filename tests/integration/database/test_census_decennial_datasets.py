"""Migration 037 admits the 2000 census datasets (decennial-2000-and-1990-history).

Covers: DB-063
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import psycopg2
import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "sql"
    / "migrations"
    / "037_census_decennial_datasets.sql"
)

#: Each relation 037 widens, its column, and the CHECK it leaves.
TARGETS = (
    (
        "control.acs_ingestion_slices",
        "dataset",
        "acs_ingestion_slices_dataset_census_check",
    ),
    (
        "gold_census.dim_acs_table",
        "dataset_code",
        "dim_acs_table_dataset_code_census_check",
    ),
    (
        "gold_census.dim_acs_variable",
        "dataset_code",
        "dim_acs_variable_dataset_code_census_check",
    ),
    (
        "gold_census.rpt_acs_observations",
        "dataset_code",
        "rpt_acs_observations_dataset_code_census_check",
    ),
)


def _dataset_checks(cursor, relation: str, column: str) -> list[tuple[str, bool, str]]:
    cursor.execute(
        """
        SELECT conname, convalidated, pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = %s::regclass AND contype = 'c' AND conislocal
          AND pg_get_constraintdef(oid) LIKE '%%' || %s || '%%acs1%%'
        ORDER BY conname
        """,
        (relation, column),
    )
    return cursor.fetchall()


def test_every_dataset_check_admits_the_census_and_nothing_else(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-063 -- one named CHECK per relation, census codes admitted."""
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            for relation, column, name in TARGETS:
                checks = _dataset_checks(cursor, relation, column)
                assert [check[0] for check in checks] == [name], relation
                assert "dec2000sf1" in checks[0][2] and "dec2000sf3" in checks[0][2]
            cursor.execute("BEGIN")
            cursor.execute(
                "INSERT INTO gold_census.dim_acs_table (dataset_code, vintage_year, "
                "table_id, survey_span_years) VALUES ('dec2000sf1', 2000, 'B01003', 0)"
            )
            cursor.execute("ROLLBACK")
            cursor.execute("BEGIN")
            with pytest.raises(psycopg2.errors.CheckViolation):
                cursor.execute(
                    "INSERT INTO gold_census.dim_acs_table (dataset_code, "
                    "vintage_year, table_id, survey_span_years) "
                    "VALUES ('dec1990stf9', 1990, 'B01003', 0)"
                )
            cursor.execute("ROLLBACK")
    finally:
        database.close()


def test_a_deployed_warehouse_is_widened_without_scanning_the_serving_fact(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-063 -- the old CHECKs are replaced; the fact's is NOT VALID; reruns are no-ops."""
    database = postgres_connection_factory()
    database.autocommit = False
    try:
        with database.cursor() as cursor:
            # Put each relation back as a warehouse deployed before 037 has it.
            for relation, column, name in TARGETS:
                cursor.execute(f"ALTER TABLE {relation} DROP CONSTRAINT {name}")
                cursor.execute(
                    f"ALTER TABLE {relation} ADD CHECK ({column} IN ('acs1', 'acs5'))"
                )
            sql = MIGRATION.read_text(encoding="utf-8")
            cursor.execute(sql)
            first = {
                relation: _dataset_checks(cursor, relation, column)
                for relation, column, _ in TARGETS
            }
            cursor.execute(sql)
            second = {
                relation: _dataset_checks(cursor, relation, column)
                for relation, column, _ in TARGETS
            }
    finally:
        database.rollback()
        database.close()

    assert first == second
    for relation, _column, name in TARGETS:
        assert [check[0] for check in first[relation]] == [name], relation
    validated = {relation: checks[0][1] for relation, checks in first.items()}
    assert validated == {
        "control.acs_ingestion_slices": True,
        "gold_census.dim_acs_table": True,
        "gold_census.dim_acs_variable": True,
        "gold_census.rpt_acs_observations": False,
    }
