"""The serving layer holds a value the provider withheld.

Covers: DB-061 -- ACS and BLS served only published numbers. The gold fact
        views filtered on a non-null value and the reporting tables declared
        theirs `NOT NULL`, so there was nowhere to put a withheld cell even if
        the filter had allowed one. Measured on the internal stack, that
        removed 31,481,530 of 99,783,997 ACS fact rows: every cell Census
        suppressed, indistinguishable to a consumer from a geography that was
        never published.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: (relation, the column the status is about, the token meaning "published").
SERVED = [
    ("gold_census.rpt_acs_observations", "estimate_value", "valid", "absent"),
    ("gold_census.mv_acs_latest", "estimate_value", "valid", "absent"),
    ("gold_bls.rpt_bls_observations", "value", "valid", "missing"),
    ("gold_bls.mv_bls_latest", "value", "valid", "missing"),
]
IDS = [relation for relation, *_ in SERVED]


@pytest.mark.parametrize(
    ("relation", "value_column", "published", "withheld"), SERVED, ids=IDS
)
def test_the_served_relation_carries_the_value_state(
    postgres_connection_factory: Callable[[], connection],
    relation: str,
    value_column: str,
    published: str,
    withheld: str,
) -> None:
    """Covers: DB-061 — the columns exist and the value column is nullable.

    Asserted against the catalog rather than the DDL text, because a column a
    migration failed to add reads exactly like one it did.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.attname, a.attnotnull
                FROM pg_attribute a
                WHERE a.attrelid = %s::regclass
                  AND a.attname IN ('value_status', 'source_value',
                                    'capture_id', 'value', %s)
                  AND NOT a.attisdropped
                """,
                (relation, value_column),
            )
            columns = dict(cursor.fetchall())
    finally:
        database.close()

    for required in ("value_status", "source_value", "capture_id"):
        assert required in columns, (
            f"{relation} does not carry {required}, so a served row cannot say "
            f"why its value is absent or which response it came from"
        )
    assert columns["value_status"] is True, (
        f"{relation}.value_status is nullable; a row with no state is a row "
        f"that says nothing, which is what this change exists to end"
    )
    assert columns[value_column] is False, (
        f"{relation}.{value_column} is still NOT NULL, so a withheld value has "
        f"nowhere to go and is dropped rather than served"
    )


@pytest.mark.parametrize(
    ("relation", "value_column", "published", "withheld"),
    [entry for entry in SERVED if entry[0].startswith("gold_bls.rpt")],
    ids=["gold_bls.rpt_bls_observations"],
)
def test_a_served_row_cannot_claim_a_published_value_it_does_not_have(
    postgres_connection_factory: Callable[[], connection],
    relation: str,
    value_column: str,
    published: str,
    withheld: str,
) -> None:
    """Covers: DB-061 — the state means something because the row is refused.

    The withheld row is inserted first and must be accepted, so the refusal
    below is shown to be about the *claim* rather than about the null.
    """
    token = uuid4().hex[:8]
    insert = f"""
        INSERT INTO {relation} (
            observation_date, as_of_date, updated_at, geo_id, geo_level,
            series_id, program_code, {value_column}, value_status
        )
        VALUES (DATE '2095-01-01', DATE '2095-01-01',
                '2095-01-01'::TIMESTAMPTZ, %s, 'STATE', %s, 'LA', NULL, %s)
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(insert, (f"state:95:{token}", f"S{token}", withheld))

            with pytest.raises(psycopg2.errors.CheckViolation):
                cursor.execute(insert, (f"state:95:{token}b", f"S{token}b", published))
    finally:
        database.rollback()
        database.close()


def test_the_gold_views_no_longer_drop_a_withheld_row(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-061 — the filter that removed them is gone from both views.

    Read from `pg_get_viewdef` rather than the checked-in SQL: what matters is
    the definition the warehouse holds, and a phase file that failed to replace
    a view leaves the old body in place while the file reads correctly.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            bodies = {}
            for view in (
                "gold_census.fact_acs_observation",
                "gold_bls.fact_bls_observation",
            ):
                cursor.execute("SELECT pg_get_viewdef(%s::regclass, true)", (view,))
                bodies[view] = cursor.fetchone()[0]
    finally:
        database.close()

    acs = bodies["gold_census.fact_acs_observation"]
    assert "estimate_value IS NOT NULL" not in acs, (
        "the ACS fact view still drops rows with no estimate, which is every "
        "cell the Bureau suppressed"
    )
    assert "value_status" in acs

    bls = bodies["gold_bls.fact_bls_observation"]
    assert "s.value IS NOT NULL" not in bls and "value IS NOT NULL" not in bls, (
        "the BLS fact view still drops rows with no value"
    )
    assert "value_status" in bls
