"""Real PostgreSQL scheduled assessment, plausibility, and certification."""

from __future__ import annotations

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.assessment import (
    certify_release,
    run_scheduled_assessment,
)
from data_ingestion_toolbox.quality.plausibility import (
    fred_change_plausibility,
    record_warning_review,
)
from data_ingestion_toolbox.quality.runner import execute_rules

pytestmark = [pytest.mark.integration, pytest.mark.database]

COMMIT_SHA = "fedcba9876543210fedcba9876543210fedcba98"

#: Schemas whose base tables are emptied for a hermetic assessment. Earlier
#: tests commit deliberately defective probe rows into the append-only
#: foundation, so a whole-warehouse sweep must start from a blank slate.
_BLANKED_SCHEMAS = (
    "raw_capture",
    "control",
    "silver_census",
    "silver_bls",
    "silver_fred",
    "silver_pep",
    "silver_cdc",
    "silver_fbi",
    "silver_nass",
    "silver_ref",
    "gold_glossary",
    "gold_census",
    "gold_bls",
    "gold_fred",
    "gold_pep",
)

#: Never blanked: bootstrap state and the append-only quality evidence.
_PRESERVED_TABLES = {
    ("control", "schema_migration_state"),
    ("control", "data_quality_run"),
    ("control", "data_quality_result"),
}


def _blank_warehouse(cursor) -> None:
    """Empty the warehouse inside this never-committed transaction.

    Replica mode bypasses FK ordering and the append-only capture triggers;
    the surrounding test transaction always rolls back, so nothing durable
    changes.
    """
    cursor.execute(
        """
        SELECT table_schema, table_name
          FROM information_schema.tables
         WHERE table_schema = ANY(%s) AND table_type = 'BASE TABLE'
        """,
        (list(_BLANKED_SCHEMAS),),
    )
    tables = [tuple(row) for row in cursor.fetchall()]
    cursor.execute("SET session_replication_role = replica")
    for schema, name in tables:
        if (schema, name) in _PRESERVED_TABLES:
            continue
        cursor.execute(f'DELETE FROM "{schema}"."{name}"')
    cursor.execute("SET session_replication_role = origin")


def test_daily_assessment_persists_queryable_summaries(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-005 — one scheduled sweep leaves operator-queryable state."""
    with postgres_connection.cursor() as cursor:
        _blank_warehouse(cursor)
    record = run_scheduled_assessment(
        postgres_connection,
        cadence="daily",
        code_commit_sha=COMMIT_SHA,
    )
    assert record.overall_status in {"pass", "warn"}, record.failure_summary

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT assessment_type, overall_status, blocking_failures, results
              FROM control.data_quality_source_status
             WHERE quality_run_id = %s
            """,
            (record.quality_run_id,),
        )
        assessment_type, status, blocking, results = cursor.fetchone()
        assert assessment_type == "scheduled"
        assert status == record.overall_status
        assert blocking == 0
        assert results == sum(
            len(outcomes) for outcomes in record.rule_results.values()
        )

        cursor.execute(
            "SELECT COUNT(*) FROM control.data_quality_latest_result"
            " WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        assert cursor.fetchone()[0] == results
    postgres_connection.rollback()


def _seed_fred_series(
    cursor,
    series_id: str,
    values: list[float],
    *,
    ingested_at: str = "NOW()",
    start_index: int = 0,
) -> None:
    """Seed one series through the real silver fact the gold view projects.

    ``ingested_at`` is a SQL expression so a test can place observations on
    either side of a certification boundary; the gold view projects it as
    ``updated_at``, which is what the certified-baseline filter reads.
    """
    cursor.execute(
        """
        INSERT INTO gold_fred.dim_fred_series (series_id, series_title)
        VALUES (%s, %s)
        ON CONFLICT (series_id) DO NOTHING
        """,
        (series_id, f"Probe series {series_id}"),
    )
    for index, value in enumerate(values):
        cursor.execute(
            """
            WITH probe AS (
                SELECT (DATE '2024-01-01' + %s)::DATE AS observation_date
            ), calendar AS (
                INSERT INTO silver_ref.dim_time (
                    time_sk, date_key, year, quarter, month, day,
                    day_of_week, day_name, month_name, week_of_year,
                    is_weekend, is_month_start, is_month_end,
                    is_quarter_start, is_quarter_end, is_year_start,
                    is_year_end, ingested_at
                )
                SELECT TO_CHAR(observation_date, 'YYYYMMDD')::INT,
                       observation_date,
                       EXTRACT(YEAR FROM observation_date)::INT,
                       EXTRACT(QUARTER FROM observation_date)::INT,
                       EXTRACT(MONTH FROM observation_date)::INT,
                       EXTRACT(DAY FROM observation_date)::INT,
                       EXTRACT(ISODOW FROM observation_date)::INT,
                       TO_CHAR(observation_date, 'FMDay'),
                       TO_CHAR(observation_date, 'FMMonth'),
                       EXTRACT(WEEK FROM observation_date)::INT,
                       EXTRACT(ISODOW FROM observation_date) >= 6,
                       FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, NOW()
                  FROM probe
                ON CONFLICT (time_sk) DO NOTHING
            )
            INSERT INTO silver_fred.fact_economic_indicators (
                time_sk, duration_start, duration_end, observation_date,
                series_id, value, load_batch_id, ingested_at
            )
            SELECT TO_CHAR(observation_date, 'YYYYMMDD')::INT,
                   observation_date, observation_date, observation_date,
                   %s, %s, GEN_RANDOM_UUID(), INGESTED_AT
              FROM probe
            """.replace("INGESTED_AT", ingested_at),
            ((start_index + index) * 30, series_id, value),
        )


STEADY_HISTORY = [100.0, 101.0, 99.0, 100.5, 100.2, 99.8, 100.1, 99.9, 100.3]


def _certify(postgres_connection: connection):
    """Certify the current warehouse so a baseline may be built from it."""
    return certify_release(postgres_connection, code_commit_sha=COMMIT_SHA)


def test_plausibility_is_not_applicable_without_a_certified_baseline(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-006 — an uncertified warehouse teaches no baseline.

    Learning "normal" from whatever happens to be retained is the failure that
    matters: an uncertified value pulls the median toward itself and the next
    genuine anomaly scores lower. With nothing certified there is no baseline,
    and the honest verdict is that plausibility cannot be judged.
    """
    with postgres_connection.cursor() as cursor:
        _blank_warehouse(cursor)
        _seed_fred_series(cursor, "PROBE_SHOCK", STEADY_HISTORY + [250.0])

        [outcome] = fred_change_plausibility(cursor, {})
        assert outcome.result == "not_applicable"
        assert outcome.evidence == ["no promotable release certification exists"]
    postgres_connection.rollback()


def test_a_blocking_failure_disqualifies_its_object_as_a_baseline(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-006 — material a blocking rule rejects teaches no baseline."""
    with postgres_connection.cursor() as cursor:
        _blank_warehouse(cursor)
        _seed_fred_series(cursor, "PROBE_SHOCK", STEADY_HISTORY + [250.0])
    certification = _certify(postgres_connection)
    assert certification.promotable

    with postgres_connection.cursor() as cursor:
        [outcome] = fred_change_plausibility(cursor, {})
        assert outcome.result == "warn"

        # A later sweep finds a blocking failure on the object the baseline
        # reads. The certification itself stays valid -- it was true when it
        # ran -- but the material is no longer fit to teach a baseline.
        cursor.execute(
            """
            WITH later_run AS (
                INSERT INTO control.data_quality_run (
                    quality_run_id, source_code, assessment_type,
                    code_commit_sha, rule_set_version, finished_at,
                    overall_status
                ) VALUES (GEN_RANDOM_UUID(), 'FRED', 'scheduled', %s,
                          'probe', NOW(), 'fail')
                RETURNING quality_run_id
            )
            INSERT INTO control.data_quality_result (
                quality_run_id, rule_id, severity, layer, object_name,
                source_code, partition_key, result
            )
            SELECT quality_run_id, 'DQ-FRED-001', 'BLOCK', 'gold',
                   'gold_fred.fact_fred_observation', 'FRED', '', 'fail'
              FROM later_run
            """,
            (COMMIT_SHA,),
        )
        [refused] = fred_change_plausibility(cursor, {})
        assert refused.result == "not_applicable"
        assert refused.evidence == [
            "gold_fred.fact_fred_observation is failing a blocking deterministic rule"
        ]
    postgres_connection.rollback()


def test_uncertified_values_cannot_silence_the_alarm_they_caused(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-006 — only certified history teaches the baseline.

    This is the failure mode the restriction exists for. A run of bad values
    that is not yet certified used to join the baseline, drag the median onto
    itself, and score the next bad value as ordinary. Restricted to certified
    history, the same warehouse still warns.
    """
    with postgres_connection.cursor() as cursor:
        _blank_warehouse(cursor)
        _seed_fred_series(
            cursor,
            "PROBE_DRIFT",
            STEADY_HISTORY,
            ingested_at="NOW() - INTERVAL '2 days'",
        )
    certification = _certify(postgres_connection)
    assert certification.promotable

    with postgres_connection.cursor() as cursor:
        # Twelve uncertified values at the anomalous level: enough to become
        # the majority of a naive baseline built from everything retained.
        _seed_fred_series(
            cursor,
            "PROBE_DRIFT",
            [250.0] * 12,
            ingested_at="NOW() + INTERVAL '1 day'",
            start_index=len(STEADY_HISTORY),
        )

        [outcome] = fred_change_plausibility(cursor, {})
        assert outcome.result == "warn"
        assert outcome.partition_key == "PROBE_DRIFT"
        # The baseline is the certified history alone, not the 21 retained
        # observations a naive baseline would have used.
        assert outcome.observed_count == len(STEADY_HISTORY)
        assert "latest=250.0" in outcome.evidence
    postgres_connection.rollback()


def test_extreme_but_valid_values_warn_without_mutation(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-006 — anomalies warn, open a review, and change nothing."""
    steady = STEADY_HISTORY
    with postgres_connection.cursor() as cursor:
        _blank_warehouse(cursor)
        _seed_fred_series(cursor, "PROBE_STEADY", steady + [100.2])
        _seed_fred_series(cursor, "PROBE_SHOCK", steady + [250.0])
    _certify(postgres_connection)

    with postgres_connection.cursor() as cursor:
        outcomes = fred_change_plausibility(cursor, {})
        by_partition = {outcome.partition_key: outcome for outcome in outcomes}
        assert set(by_partition) == {"PROBE_SHOCK"}
        shock = by_partition["PROBE_SHOCK"]
        assert shock.result == "warn"
        assert "latest=250.0" in shock.evidence

    record = execute_rules(
        postgres_connection,
        source_code="FRED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors={"DQ-FRED-006": fred_change_plausibility},
    )
    assert record.overall_status == "warn"

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT result_id, review_status"
            " FROM control.data_quality_result WHERE quality_run_id = %s"
            " AND partition_key = 'PROBE_SHOCK'",
            (record.quality_run_id,),
        )
        result_id, review_status = cursor.fetchone()
        assert review_status == "open"

        # The provider value is untouched: warned, never corrected.
        cursor.execute(
            """
            SELECT fact.value
              FROM gold_fred.fact_fred_observation AS fact
              JOIN gold_fred.dim_fred_series AS series
                ON series.fred_series_sk = fact.fred_series_sk
             WHERE series.series_id = 'PROBE_SHOCK'
             ORDER BY fact.observation_date DESC LIMIT 1
            """
        )
        assert float(cursor.fetchone()[0]) == 250.0

    record_warning_review(postgres_connection, result_id, "acknowledged")
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT review_status FROM control.data_quality_result"
            " WHERE result_id = %s",
            (result_id,),
        )
        assert cursor.fetchone()[0] == "acknowledged"

    with pytest.raises(ValueError, match="not a reviewable warning"):
        record_warning_review(postgres_connection, result_id + 999999, "accepted")
    postgres_connection.rollback()


def test_release_certification_reports_promotability(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-007 — certification ties totals to one immutable commit."""
    with postgres_connection.cursor() as cursor:
        _blank_warehouse(cursor)
    healthy = certify_release(postgres_connection, code_commit_sha=COMMIT_SHA)
    assert healthy.promotable, healthy.as_dict()
    assert healthy.code_commit_sha == COMMIT_SHA
    assert healthy.overall_status in {"pass", "warn"}
    payload = healthy.as_dict()
    assert payload["promotable"] is True
    assert payload["rule_set_version"] == healthy.rule_set_version

    # Inject a blocking defect: an abandoned ACS slice.
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.acs_ingestion_slices
                (dataset, year, geo_level, status, rows_loaded, started_at)
            VALUES ('acs5', 2023, 'us', 'failed', 0, NOW())
            """
        )
    damaged = certify_release(postgres_connection, code_commit_sha=COMMIT_SHA)
    assert not damaged.promotable
    assert damaged.overall_status == "fail"
    assert damaged.totals.get(("QUARANTINE", "fail"), 0) >= 1
    postgres_connection.rollback()
