"""Fact-level defect injections for Census PEP and FBI UCR (DQ-004).

The other DQ-004 injections reach the revision, ledger, and control layers.
These reach the *facts*: the rows a consumer is actually served. Each test
builds real state from the reviewed fixtures through the production pipeline,
proves the rule passes on it, then injects one defect and proves the rule
fails naming the exact offending row.

Several of these invariants are also enforced by DDL CHECK constraints, so the
injection drops the constraint first. That is deliberate rather than a
shortcut. A quality rule that only ever runs behind an intact constraint is
untested: it exists for the case where the constraint is relaxed, where a
future migration writes through a path the constraint does not cover, or where
data arrives from a restore. Every injection here runs inside a transaction the
fixture always rolls back, so no dropped constraint outlives the test.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)
from data_ingestion_toolbox.quality.sources import (
    fbi_aggregation_boundary,
    fbi_participation_coverage,
    fbi_reported_vs_absent,
    pep_registry_reconciliation,
    pep_release_completeness,
)
from tests.integration.database.test_fbi_ucr_pipeline import (  # noqa: F401
    _persist_fixture_release,
    _run_pipeline,
    fbi_warehouse,
)
from tests.integration.database.test_pep_capture_flow import (  # noqa: F401
    _capture_fixture,
    pep_database_scope,
)
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


def _outcome(cursor, executor):
    [outcome] = executor(cursor, {})
    return outcome


def _drop_check(cursor, table: str, constraint: str) -> None:
    """Remove one CHECK for the life of this rolled-back transaction."""
    cursor.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}")


def _check_constraints(cursor, schema: str, table: str) -> list[str]:
    """Return the table's declared CHECK constraints.

    ``pg_constraint`` rather than ``information_schema``: the latter also
    reports generated NOT NULL entries whose names are not valid identifiers.
    """
    cursor.execute(
        """
        SELECT conname
          FROM pg_constraint
         WHERE contype = 'c'
           AND conrelid = TO_REGCLASS(%s)
         ORDER BY conname
        """,
        (f"{schema}.{table}",),
    )
    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Census PEP
# ---------------------------------------------------------------------------


@pytest.fixture
def pep_facts(
    postgres_connection_factory: Callable[[], connection],
    pep_database_scope,  # noqa: F811
):
    """Load reviewed PEP release bytes all the way to silver facts."""
    capture = _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2025,
        fixture_name="nst_2025.csv",
    )
    inserted = transform_pep_to_silver(PostgresHookStub(postgres_connection_factory))
    assert inserted > 0, "the reviewed fixture must produce facts to inject into"
    return capture


def test_pep_facts_from_an_incomplete_release_load_fail(
    pep_facts, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-004 — a fact whose release never completed is reported.

    ``release_load.completeness_status`` is what tells the warehouse the
    principal summary level actually arrived. A fact surviving beneath an
    incomplete load is a partial release being served as a whole one.
    """
    capture = pep_facts
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert _outcome(cursor, pep_release_completeness).result == "pass"

            cursor.execute(
                "UPDATE silver_pep.release_load "
                "SET completeness_status = 'incomplete', "
                "    completeness_reason = 'injected defect' "
                "WHERE capture_id = %s",
                (str(capture.capture_id),),
            )
            outcome = _outcome(cursor, pep_release_completeness)
            assert outcome.result == "fail"
            assert outcome.observed_count == 1
            assert outcome.evidence == [str(capture.capture_id)]

            # A fact with no release load at all is the same defect.
            cursor.execute(
                "DELETE FROM silver_pep.release_load WHERE capture_id = %s",
                (str(capture.capture_id),),
            )
            orphaned = _outcome(cursor, pep_release_completeness)
            assert orphaned.result == "fail"
            assert orphaned.evidence == [str(capture.capture_id)]
    finally:
        database_connection.rollback()
        database_connection.close()


def test_pep_facts_outside_the_release_registry_fail(
    pep_facts, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-004 — a fact claiming an unregistered vintage is reported.

    The registry is the declared scope. A fact whose ``release_vintage`` is not
    in it was produced by something the warehouse never registered, so its
    provenance cannot be checked against a published Census release.
    """
    del pep_facts
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            # The registry legitimately holds releases this fixture did not
            # load, so the "unloaded" arm is expected to be reporting already;
            # this test owns the "unregistered" arm.
            before = _outcome(cursor, pep_registry_reconciliation)
            assert not [
                entry for entry in before.evidence if entry.startswith("unregistered:")
            ]

            # observation_year <= release_vintage is CHECK-enforced, so the
            # unregistered vintage moves forward rather than backward.
            cursor.execute(
                "UPDATE silver_pep.fact_population_estimate "
                "SET release_vintage = 2099 "
                "WHERE dataset_code = 'pep_nst_alldata' "
                "  AND ctid IN (SELECT ctid FROM silver_pep.fact_population_estimate "
                "               WHERE dataset_code = 'pep_nst_alldata' LIMIT 1)"
            )
            outcome = _outcome(cursor, pep_registry_reconciliation)
            assert outcome.result == "fail"
            assert "unregistered:pep_nst_alldata|2099" in outcome.evidence
    finally:
        database_connection.rollback()
        database_connection.close()


def test_a_published_pep_release_with_no_complete_load_fails(
    pep_facts, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-004 — registered work the warehouse never loaded is reported.

    The reconciliation runs in both directions: a registered, published release
    with no complete load is missing configured work, not valid emptiness.
    """
    del pep_facts
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT dataset_code, vintage_year FROM silver_pep.pep_release "
                "WHERE status = 'published' "
                "  AND (dataset_code, vintage_year) NOT IN ("
                "      SELECT dataset_code, release_vintage "
                "        FROM silver_pep.release_load "
                "       WHERE completeness_status = 'complete') "
                "ORDER BY dataset_code, vintage_year LIMIT 1"
            )
            unloaded = cursor.fetchone()
            if unloaded is None:
                pytest.skip("every registered PEP release is already loaded")
            outcome = _outcome(cursor, pep_registry_reconciliation)
            assert outcome.result == "fail"
            assert f"unloaded:{unloaded[0]}|{unloaded[1]}" in outcome.evidence
    finally:
        database_connection.rollback()
        database_connection.close()


# ---------------------------------------------------------------------------
# FBI UCR
# ---------------------------------------------------------------------------


@pytest.fixture
def fbi_facts(fbi_warehouse: Callable[[], connection]):  # noqa: F811
    """Publish the reviewed Wisconsin release through the real pipeline."""
    captured = _persist_fixture_release(fbi_warehouse)
    transformed, published = _run_pipeline(fbi_warehouse, captured)
    assert transformed > 0 and published == transformed
    return fbi_warehouse, captured


def test_a_published_crime_observation_without_coverage_fails(fbi_facts) -> None:
    """Covers: DQ-004 — an observation with no participation row is reported.

    Coverage is what makes a crime count interpretable: without it a consumer
    cannot tell a complete month from a partially reporting one. The failure
    is doubly serious because ``gold_fbi.crime_observation`` inner joins
    participation, so an observation that loses coverage does not surface as
    uncovered -- it silently disappears from what the warehouse serves.
    """
    connection_factory, _captured = fbi_facts
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            silver, gold = fbi_participation_coverage(cursor, {})
            assert (silver.result, gold.result) == ("pass", "pass")
            assert gold.observed_count == gold.expected_count > 0

            # The foreign key is what normally makes this impossible, so the
            # injection runs with referential triggers disabled inside this
            # rolled-back transaction -- proving the rule is a real second line
            # of defence rather than an assertion the constraint already holds.
            cursor.execute(
                """
                SELECT product_id, release_key, subject_type, subject_code,
                       period, source_record_id
                  FROM silver_fbi.fact_crime_observation
                 WHERE geography_status NOT IN ('ambiguous', 'unsupported')
                 ORDER BY source_record_id
                 LIMIT 1
                """
            )
            product, release, subject_type, subject_code, period, record = (
                cursor.fetchone()
            )
            cursor.execute("SET session_replication_role = replica")
            cursor.execute(
                """
                DELETE FROM silver_fbi.fact_reporting_participation
                 WHERE product_id = %s AND release_key = %s
                   AND subject_type = %s AND subject_code = %s AND period = %s
                """,
                (product, release, subject_type, subject_code, period),
            )
            removed = cursor.rowcount
            cursor.execute("SET session_replication_role = origin")
            assert removed == 1

            silver, gold = fbi_participation_coverage(cursor, {})
            assert silver.result == "fail"
            assert f"{product}|{release}|{record}" in silver.evidence
            # Every observation for that subject and period vanished from the
            # served view, which the count reconciliation catches.
            assert gold.result == "fail"
            assert gold.observed_count < gold.expected_count
            assert gold.evidence == [
                f"publishable={gold.expected_count}",
                f"served={gold.observed_count}",
            ]
    finally:
        database_connection.rollback()
        database_connection.close()


def test_an_absent_agency_month_carrying_a_number_fails(fbi_facts) -> None:
    """Covers: DQ-004 — a month nobody reported cannot hold a value.

    This is the defect the whole FBI contract exists to prevent: turning "no
    agency reported" into a number a consumer reads as low crime.
    """
    connection_factory, _captured = fbi_facts
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert _outcome(cursor, fbi_reported_vs_absent).result == "pass"

            constraints = _check_constraints(
                cursor, "silver_fbi", "fact_crime_observation"
            )
            for constraint in constraints:
                _drop_check(cursor, "silver_fbi.fact_crime_observation", constraint)

            cursor.execute(
                """
                UPDATE silver_fbi.fact_crime_observation
                   SET value = 0
                 WHERE value_status = 'not_reported'
                   AND source_record_id = (
                       SELECT source_record_id
                         FROM silver_fbi.fact_crime_observation
                        WHERE value_status = 'not_reported'
                        ORDER BY source_record_id
                        LIMIT 1
                   )
                """
            )
            assert cursor.rowcount == 1
            outcome = _outcome(cursor, fbi_reported_vs_absent)
            assert outcome.result == "fail"
            assert outcome.observed_count == 1
            assert outcome.evidence[0].endswith("|not_reported")

            # The mirror defect: a reported month that lost its value.
            cursor.execute(
                """
                UPDATE silver_fbi.fact_crime_observation
                   SET value = NULL
                 WHERE value_status = 'reported'
                   AND source_record_id = (
                       SELECT source_record_id
                         FROM silver_fbi.fact_crime_observation
                        WHERE value_status = 'reported'
                        ORDER BY source_record_id
                        LIMIT 1
                   )
                """
            )
            both = _outcome(cursor, fbi_reported_vs_absent)
            assert both.result == "fail"
            assert both.observed_count == 2
            assert {entry.rsplit("|", 1)[-1] for entry in both.evidence} == {
                "not_reported",
                "reported",
            }
    finally:
        database_connection.rollback()
        database_connection.close()


def test_an_overlapping_agency_relationship_fans_out_and_fails(fbi_facts) -> None:
    """Covers: DQ-004 — a duplicated area filter row is reported, not served.

    The area filter legitimately emits one row per associated area. A second
    relationship covering the same agency, area, and period multiplies every
    observation, which is exactly how an agency filter starts to look like an
    area total.
    """
    connection_factory, _captured = fbi_facts
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert _outcome(cursor, fbi_aggregation_boundary).result == "pass"

            cursor.execute(
                """
                INSERT INTO silver_fbi.agency_geography_relationship (
                    product_id, release_key, ori, relationship_type, geo_id,
                    geo_sk, source_label, resolution_method, resolution_status,
                    confidence_class, reason_code, effective_start,
                    effective_end, geography_vintage, evidence_source,
                    evidence_capture_id
                )
                SELECT product_id, release_key, ori, relationship_type, geo_id,
                       geo_sk, source_label || ' (injected duplicate)',
                       resolution_method, resolution_status, confidence_class,
                       reason_code, effective_start, effective_end,
                       geography_vintage, evidence_source, evidence_capture_id
                  FROM silver_fbi.agency_geography_relationship
                 WHERE resolution_status = 'resolved'
                 ORDER BY relationship_sk
                 LIMIT 1
                """
            )
            assert cursor.rowcount == 1

            outcome = _outcome(cursor, fbi_aggregation_boundary)
            assert outcome.result == "fail"
            assert outcome.observed_count > outcome.expected_count
            assert outcome.evidence[0].startswith("rows=")
            assert outcome.evidence[1].startswith("distinct_agency_grain=")
    finally:
        database_connection.rollback()
        database_connection.close()
