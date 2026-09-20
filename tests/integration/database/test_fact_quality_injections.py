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
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)
from data_ingestion_toolbox.fred.gold_fred import transform as fred_gold_transform
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.quality.sources import (
    fbi_aggregation_boundary,
    fbi_participation_coverage,
    fbi_reported_vs_absent,
    fred_contract_conformance,
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
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import seed_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


def _outcome(cursor, executor):
    [outcome] = executor(cursor, {})
    return outcome


def _outcome_for(cursor, executor, relation: str):
    """One rule's outcome for one relation, from an executor that reads several.

    `DQ-FBI-004` reads two: the area filter's grain and, since ETL-050, the
    confidence every resolved relationship claims. Selecting by relation keeps
    each node asserting the reading it is about.
    """
    outcomes = {outcome.object_name: outcome for outcome in executor(cursor, {})}
    assert relation in outcomes, sorted(outcomes)
    return outcomes[relation]


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


def test_a_relationship_claiming_more_than_its_evidence_fails(fbi_facts) -> None:
    """Covers: ETL-050 — DQ-FBI-004 can see a confidence a method did not earn.

    The rule declares that attribution flows through exact state codes,
    reviewed crosswalks, or a county label match published as `derived`. Its
    only reading was a fanout count, so for as long as the county path wrote
    `reviewed_county_name_crosswalk` / `reviewed` from a name join, the rule
    passed over the thing it declares. Both shapes are injected here: a
    resolved relationship claiming a confidence its method does not earn, and
    one whose method the reviewed mapping does not know at all -- which is how
    the county path's own spelling went unnoticed.
    """
    connection_factory, _captured = fbi_facts
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert (
                _outcome_for(
                    cursor,
                    fbi_aggregation_boundary,
                    "silver_fbi.agency_geography_relationship",
                ).result
                == "pass"
            )

            # The CHECK refuses the claim, so the rule is the second line of
            # defence: it reads what is stored rather than trusting the write
            # path, and the constraint is restored by the rollback below.
            _drop_check(
                cursor,
                "silver_fbi.agency_geography_relationship",
                "agency_geography_relationship_confidence_class_check",
            )
            cursor.execute(
                """
                UPDATE silver_fbi.agency_geography_relationship
                   SET confidence_class = 'reviewed'
                 WHERE resolution_method = 'county_label_match'
                   AND resolution_status = 'resolved'
                """
            )
            # Read before the rule runs its own statements on this cursor.
            falsely_claimed = cursor.rowcount
            assert falsely_claimed > 0
            claimed = _outcome_for(
                cursor,
                fbi_aggregation_boundary,
                "silver_fbi.agency_geography_relationship",
            )
            assert claimed.result == "fail"
            assert claimed.observed_count == falsely_claimed
            assert any("county_label_match" in entry for entry in claimed.evidence)

            _drop_check(
                cursor,
                "silver_fbi.agency_geography_relationship",
                "agency_geography_relationship_resolution_method_check",
            )
            cursor.execute(
                """
                UPDATE silver_fbi.agency_geography_relationship
                   SET resolution_method = 'reviewed_county_name_crosswalk'
                 WHERE resolution_method = 'county_label_match'
                """
            )
            unknown = _outcome_for(
                cursor,
                fbi_aggregation_boundary,
                "silver_fbi.agency_geography_relationship",
            )
            assert unknown.result == "fail"
            assert any(
                "reviewed_county_name_crosswalk" in entry for entry in unknown.evidence
            )
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
            assert (
                _outcome_for(
                    cursor,
                    fbi_aggregation_boundary,
                    "gold_fbi.agency_observation_area_filter",
                ).result
                == "pass"
            )

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

            outcome = _outcome_for(
                cursor,
                fbi_aggregation_boundary,
                "gold_fbi.agency_observation_area_filter",
            )
            assert outcome.result == "fail"
            assert outcome.observed_count > outcome.expected_count
            assert outcome.evidence[0].startswith("rows=")
            assert outcome.evidence[1].startswith("distinct_agency_grain=")
    finally:
        database_connection.rollback()
        database_connection.close()


# ---------------------------------------------------------------------------
# FRED serving contract (DQ-FRED-007)
# ---------------------------------------------------------------------------


FRED_SERVED_SERIES = "TEST_DQ_FRED_007"
FRED_SERVED_DATE = "2099-03-01"
FRED_SERVED_METRIC = f"FRED:{FRED_SERVED_SERIES}"


@pytest.fixture
def fred_served(
    postgres_connection_factory: Callable[[], connection],
    harvest_state_cleanup: None,
):
    """One published FRED fact, carried all the way to the served views.

    Built through the production path -- the gold element refresh, the
    publisher harvest, then the chunked serving refresh -- because the rule
    reads what that path produced and a hand-built row would prove nothing
    about it.
    """
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990301, FRED_SERVED_DATE)
            seed_geography(
                cursor, geo_type="nation", vintage=2099, name="United States"
            )
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES (
                    20990301, %s, '2099-03-31', %s,
                    %s, 'fixture', 17.5, FALSE, 'Conformance fixture',
                    'Index', 'Monthly', 'Not Adjusted', 'FRED', %s,
                    '2099-03-05'
                )
                """,
                (FRED_SERVED_DATE, FRED_SERVED_DATE, FRED_SERVED_SERIES, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    fred_gold_transform.refresh_fred_elements(hook)
    harvest_publisher(postgres_connection_factory, Publisher("gold_fred"))
    refresher = postgres_connection_factory()
    try:
        with refresher.cursor() as cursor:
            cursor.execute(
                "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                (FRED_SERVED_DATE, "2099-03-31"),
            )
            # The chunked driver seeds this row before it refreshes anything
            # (`refresh_serving_layer_in_year_chunks`), and calling the
            # procedure directly skips it. Seeding it the same way is what
            # makes the fixture a warehouse the production path could have
            # produced -- and the rule reads it to tell an unserved revision
            # from an altered value.
            cursor.execute(
                """
                INSERT INTO control.serving_refresh_state (
                    source_code, last_silver_ingested_at,
                    last_refresh_completed_at
                )
                SELECT 'FRED',
                       COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
                       NOW()
                  FROM gold_fred.rpt_fred_observations r
                ON CONFLICT (source_code) DO UPDATE
                    SET last_silver_ingested_at = EXCLUDED.last_silver_ingested_at
                """
            )
        refresher.commit()
    finally:
        refresher.close()

    yield

    cleanup = postgres_connection_factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gold_fred.mv_fred_latest WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            cursor.execute(
                "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            cursor.execute(
                "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            cursor.execute(
                "DELETE FROM gold_fred.dim_fred_series WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            cursor.execute(
                "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                (FRED_SERVED_METRIC,),
            )
            cursor.execute(
                "DELETE FROM control.serving_refresh_chunk_state "
                "WHERE source_code = 'FRED'"
            )
            cursor.execute(
                "DELETE FROM control.serving_refresh_state WHERE source_code = 'FRED'"
            )
            cursor.execute("DELETE FROM silver_ref.dim_time WHERE time_sk = 20990301")
        cleanup.commit()
    finally:
        cleanup.close()


def _served_row(cursor) -> None:
    """Assert the fixture really reached the served views before injecting."""
    cursor.execute(
        "SELECT value FROM gold_fred.fact_observation WHERE metric_code = %s",
        (FRED_SERVED_METRIC,),
    )
    assert cursor.fetchone() is not None, (
        "the fixture did not reach the served views, so the injections below "
        "would prove nothing"
    )


def test_the_fred_contract_views_pass_on_what_the_pipeline_published(
    fred_served, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-017 — DQ-FRED-007: the rule passes on a warehouse built by the pipeline."""
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            _served_row(cursor)
            for relation in (
                "gold_fred.fact_observation",
                "gold_fred.v_metric_latest_by_geo",
                "gold_fred.metric_publisher",
            ):
                outcome = _outcome_for(cursor, fred_contract_conformance, relation)
                assert outcome.result == "pass", (relation, outcome.evidence)
                assert outcome.observed_count == 0
    finally:
        database_connection.rollback()
        database_connection.close()


def test_a_served_fred_row_no_published_fact_backs_fails(
    fred_served, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-017 — DQ-FRED-007: an invented served row is a number nothing published."""
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            _served_row(cursor)
            cursor.execute(
                """
                INSERT INTO gold_fred.rpt_fred_observations (
                    observation_date, as_of_date, updated_at, series_id,
                    value, metric_code
                ) VALUES ('2099-04-01', '2099-03-05', NOW(), %s, 99.9, %s)
                """,
                (FRED_SERVED_SERIES, FRED_SERVED_METRIC),
            )
            outcome = _outcome_for(
                cursor, fred_contract_conformance, "gold_fred.fact_observation"
            )
            assert outcome.result == "fail"
            assert outcome.observed_count == 1
            assert any(FRED_SERVED_METRIC in str(item) for item in outcome.evidence)
    finally:
        database_connection.rollback()
        database_connection.close()


def test_a_served_value_the_published_fact_does_not_hold_fails(
    fred_served, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-017 — DQ-FRED-007: an altered value inside the refreshed window fails.

    The serving row is rewritten rather than the fact, which is the direction
    that matters: the API would present 1.0 where the warehouse published
    17.5, and the refresh watermark says this window has been served.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            _served_row(cursor)
            cursor.execute(
                "UPDATE gold_fred.rpt_fred_observations SET value = 1.0 "
                "WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            outcome = _outcome_for(
                cursor, fred_contract_conformance, "gold_fred.fact_observation"
            )
            assert outcome.result == "fail"
            assert outcome.observed_count == 1
    finally:
        database_connection.rollback()
        database_connection.close()


def test_a_fact_revised_after_the_refresh_watermark_is_not_a_violation(
    fred_served, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-017 — DQ-FRED-007: an unserved revision is freshness, not conformance.

    The serving layer is rebuilt a year at a time with a commit per chunk
    (DB-041), and ETL-037 advances `ingested_at` only when a row's content
    changed. So a fact revised after the last refresh is a served value the
    next refresh will replace -- which DQ-FRED-002 measures as a ledger gap.
    Failing it here would make the rule fire on every warehouse between a
    revision and its next serve.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            _served_row(cursor)
            cursor.execute(
                """
                UPDATE silver_fred.fact_economic_indicators
                   SET value = 18.25, ingested_at = COALESCE((
                           SELECT last_silver_ingested_at
                             FROM control.serving_refresh_state
                            WHERE source_code = 'FRED'
                       ), NOW()) + INTERVAL '1 day'
                 WHERE series_id = %s
                """,
                (FRED_SERVED_SERIES,),
            )
            outcome = _outcome_for(
                cursor, fred_contract_conformance, "gold_fred.fact_observation"
            )
            assert outcome.result == "pass", outcome.evidence
    finally:
        database_connection.rollback()
        database_connection.close()


def test_a_latest_row_the_as_released_view_does_not_carry_fails(
    fred_served, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-017 — DQ-FRED-007: the latest view is a reduction, not a second source."""
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            _served_row(cursor)
            cursor.execute(
                "UPDATE gold_fred.mv_fred_latest SET value = 2.0 WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            outcome = _outcome_for(
                cursor,
                fred_contract_conformance,
                "gold_fred.v_metric_latest_by_geo",
            )
            assert outcome.result == "fail"
            assert outcome.observed_count == 1
    finally:
        database_connection.rollback()
        database_connection.close()


def test_a_served_metric_the_publisher_export_omits_fails(
    fred_served, postgres_connection_factory: Callable[[], connection]
) -> None:
    """Covers: DQ-017 — DQ-FRED-007: a served measure the export never published fails.

    `metric_publisher` is what the glossary harvests, so a metric code served
    and not exported is a measure the catalog cannot describe -- the silent
    empty page the discovery registry exists to prevent, one layer down.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            _served_row(cursor)
            cursor.execute(
                "UPDATE gold_fred.rpt_fred_observations "
                "SET metric_code = 'FRED:NOT_A_SERIES' WHERE series_id = %s",
                (FRED_SERVED_SERIES,),
            )
            export = _outcome_for(
                cursor, fred_contract_conformance, "gold_fred.metric_publisher"
            )
            assert export.result == "fail"
            assert export.observed_count == 1
            # And the identity check catches it too: a metric code that names
            # no published series is not backed by any fact.
            backing = _outcome_for(
                cursor, fred_contract_conformance, "gold_fred.fact_observation"
            )
            assert backing.result == "fail"
    finally:
        database_connection.rollback()
        database_connection.close()
