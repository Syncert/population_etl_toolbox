"""A step that widens a vocabulary rewrites its rows before it constrains them.

Covers: DB-057 -- `025_county_label_is_not_reviewed.sql` shipped with its two
        `ADD CONSTRAINT`s before the `UPDATE` that makes the rows satisfy
        them. `ADD CONSTRAINT` validates every existing row immediately, so on
        any warehouse actually holding what the step exists to correct it
        failed outright, and on a fresh bootstrap -- where the table is empty
        when it runs -- the order could not matter. The one shape it had to
        work on is the one it could not.

        Found by applying the manifest to the internal stack, which holds
        4,161 of those rows.
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extensions import connection

from tests.support.capture_seed import delete_seed_captures, seed_capture

pytestmark = [pytest.mark.integration, pytest.mark.database]

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[3]
MIGRATION = REPOSITORY_ROOT / "sql/migrations/025_county_label_is_not_reviewed.sql"

#: What the code before this step wrote, and what it must become.
SUPERSEDED_METHOD = "reviewed_county_name_crosswalk"
CORRECTED_METHOD = "county_label_match"


@pytest.fixture
def a_relationship_row_from_before_the_step(
    postgres_connection_factory: Callable[[], connection],
):
    """One pre-025 row, with the constraints the step replaces removed first.

    The bootstrap has already applied 025 here, so the new vocabulary is in
    place and the superseded value cannot be inserted. Dropping the two
    constraints is what makes this warehouse look like one that has not run
    the step yet, which is the only state in which the defect is reachable.
    """
    token = uuid4().hex[:10]
    ori = f"ZZ{token[:7].upper()}"
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            capture_id = seed_capture(cursor, "FBI_UCR")

            cursor.execute(
                """
                INSERT INTO silver_fbi.dim_agency (
                    ori, state_code, first_seen_release, last_seen_release
                )
                VALUES (%s, 'ZZ', 'fixture', 'fixture')
                ON CONFLICT (ori) DO NOTHING
                """,
                (ori,),
            )

            cursor.execute(
                "ALTER TABLE silver_fbi.agency_geography_relationship "
                "DROP CONSTRAINT IF EXISTS "
                "agency_geography_relationship_resolution_method_check"
            )
            cursor.execute(
                "ALTER TABLE silver_fbi.agency_geography_relationship "
                "DROP CONSTRAINT IF EXISTS "
                "agency_geography_relationship_confidence_class_check"
            )

            cursor.execute(
                """
                INSERT INTO silver_fbi.agency_geography_relationship (
                    ori, relationship_type, source_label, resolution_method,
                    resolution_status, confidence_class, effective_start,
                    effective_end, geography_vintage, evidence_source,
                    evidence_capture_id, product_id, release_key
                )
                VALUES (
                    %s, 'county', %s, %s, 'unresolved', 'reviewed',
                    DATE '2020-01-01', DATE '2029-12-31', 2020, 'fixture',
                    %s, 'fixture', 'fixture'
                )
                RETURNING relationship_sk
                """,
                (ori, f"LABEL {token}", SUPERSEDED_METHOD, capture_id),
            )
            relationship_sk = int(cursor.fetchone()[0])
    finally:
        database.close()

    yield relationship_sk

    cleanup = postgres_connection_factory()
    cleanup.autocommit = True
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_fbi.agency_geography_relationship "
                "WHERE relationship_sk = %s",
                (relationship_sk,),
            )
            cursor.execute("DELETE FROM silver_fbi.dim_agency WHERE ori = %s", (ori,))
            delete_seed_captures(cursor, [capture_id])
            # Leave the step applied, whatever this test did to it.
            cursor.execute(MIGRATION.read_text(encoding="utf-8"))
    finally:
        cleanup.close()


def test_the_county_label_step_applies_to_a_warehouse_holding_the_old_rows(
    postgres_connection_factory: Callable[[], connection],
    a_relationship_row_from_before_the_step: int,
) -> None:
    """Covers: DB-057 — the step runs where it is needed, not only where it is not.

    Before the fix this raised, and the message named the constraint rather
    than the ordering that caused it:

        check constraint "agency_geography_relationship_resolution_method_check"
        of relation "agency_geography_relationship" is violated by some row
    """
    relationship_sk = a_relationship_row_from_before_the_step
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            try:
                cursor.execute(MIGRATION.read_text(encoding="utf-8"))
            except psycopg2.errors.CheckViolation as refusal:
                pytest.fail(
                    f"025 cannot be applied to a warehouse holding the rows it "
                    f"exists to correct: {refusal}"
                )

            cursor.execute(
                """
                SELECT resolution_method, confidence_class, reason_code
                FROM silver_fbi.agency_geography_relationship
                WHERE relationship_sk = %s
                """,
                (relationship_sk,),
            )
            method, confidence, _reason = cursor.fetchone()

    finally:
        database.close()

    assert method == CORRECTED_METHOD, (
        f"the row still claims {method!r}; the step's whole purpose is that a "
        f"county resolved from a provider label is not a reviewed crosswalk"
    )
    assert confidence == "derived", (
        f"the row still claims confidence {confidence!r}, the token the place "
        f"path earns from a table with a reviewer and an evidence URL"
    )


def test_the_step_is_rerunnable_once_its_rows_are_already_corrected(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-057 — applying it twice is a no-op, not a second failure.

    The manifest re-applies every asset on every bootstrap and upgrade, so a
    step that only worked the first time would break the path it is delivered
    on.
    """
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            for _ in range(2):
                cursor.execute(MIGRATION.read_text(encoding="utf-8"))

            cursor.execute(
                """
                SELECT count(*)
                FROM silver_fbi.agency_geography_relationship
                WHERE resolution_method = %s
                """,
                (SUPERSEDED_METHOD,),
            )
            assert cursor.fetchone()[0] == 0
    finally:
        database.close()
