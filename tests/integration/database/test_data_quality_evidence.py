"""Real PostgreSQL contracts for the quality evidence store and runner."""

from __future__ import annotations

import uuid
from collections.abc import Callable

import pytest
from psycopg2 import errors
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.runner import (
    RuleOutcome,
    commits_each_outcome,
    execute_rules,
    rule_set_version,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]

COMMIT_SHA = "0123456789abcdef0123456789abcdef01234567"


def _fixture_executors() -> dict[str, object]:
    """One executor per persisted result value, against real catalog objects."""
    return {
        "DQ-SHARED-001": lambda cursor, scope: [
            RuleOutcome(
                "raw_capture.response_capture",
                "pass",
                observed_count=0,
                expected_count=0,
            )
        ],
        "DQ-SHARED-002": lambda cursor, scope: [
            RuleOutcome(
                "control.ingestion_request",
                "fail",
                partition_key="2026-08",
                observed_count=1,
                expected_count=2,
                evidence=["request:example"],
            )
        ],
        "DQ-REF-004": lambda cursor, scope: [
            RuleOutcome(
                "silver_ref.bridge_geo_relationship_version",
                "warn",
                observed_measure=0.42,
            )
        ],
        "DQ-SHARED-004": lambda cursor, scope: [
            RuleOutcome("control.schema_migration_state", "not_applicable")
        ],
    }


def test_fresh_bootstrap_persists_every_result_value(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-002 — pass/fail/warn/not-applicable all persist with lineage."""
    record = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors=_fixture_executors(),
        scope={"partition": "2026-08"},
    )

    assert record.overall_status == "fail"  # DQ-SHARED-002 is BLOCK severity
    assert record.rule_results["DQ-SHARED-002"] == ("fail",)
    assert "DQ-SHARED-002 control.ingestion_request" in record.failure_summary

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT overall_status, rule_set_version, finished_at,
                   evaluated_scope
              FROM control.data_quality_run
             WHERE quality_run_id = %s
            """,
            (record.quality_run_id,),
        )
        status, version, finished_at, scope = cursor.fetchone()
        assert status == "fail"
        assert version == rule_set_version()
        assert finished_at is not None
        assert scope == {"partition": "2026-08"}

        cursor.execute(
            """
            SELECT rule_id, severity, layer, object_name, source_code,
                   result, review_status
              FROM control.data_quality_result
             WHERE quality_run_id = %s
             ORDER BY rule_id
            """,
            (record.quality_run_id,),
        )
        rows = cursor.fetchall()

    assert [(row[0], row[5]) for row in rows] == [
        ("DQ-REF-004", "warn"),
        ("DQ-SHARED-001", "pass"),
        ("DQ-SHARED-002", "fail"),
        ("DQ-SHARED-004", "not_applicable"),
    ]
    by_rule = {row[0]: row for row in rows}
    assert by_rule["DQ-SHARED-002"][1] == "BLOCK"
    assert by_rule["DQ-SHARED-002"][2] == "control"
    assert by_rule["DQ-SHARED-002"][4] == "SHARED"
    assert by_rule["DQ-REF-004"][6] == "open"  # warnings open a review
    assert by_rule["DQ-SHARED-001"][6] is None


def test_evidence_is_append_only_except_warning_review(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-002 — re-runs add evidence; history never rewrites."""
    executors = _fixture_executors()
    first = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors=executors,
    )
    second = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors=executors,
    )
    assert first.quality_run_id != second.quality_run_id

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM control.data_quality_result "
            "WHERE quality_run_id IN (%s, %s)",
            (first.quality_run_id, second.quality_run_id),
        )
        assert cursor.fetchone()[0] == 8  # four results per run, twice

        cursor.execute("SAVEPOINT mutation_attempt")
        with pytest.raises(errors.RaiseException, match="append-only"):
            cursor.execute(
                "UPDATE control.data_quality_result SET result = 'pass' "
                "WHERE quality_run_id = %s AND rule_id = 'DQ-SHARED-002'",
                (first.quality_run_id,),
            )
        cursor.execute("ROLLBACK TO SAVEPOINT mutation_attempt")

        with pytest.raises(errors.RaiseException, match="append-only"):
            cursor.execute(
                "DELETE FROM control.data_quality_result WHERE quality_run_id = %s",
                (first.quality_run_id,),
            )
        cursor.execute("ROLLBACK TO SAVEPOINT mutation_attempt")

    # The one permitted mutation: a warning's review lifecycle.
    record = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors={"DQ-REF-004": _fixture_executors()["DQ-REF-004"]},
    )
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE control.data_quality_result SET review_status = "
            "'acknowledged' WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        cursor.execute(
            "SELECT review_status FROM control.data_quality_result "
            "WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        assert cursor.fetchone()[0] == "acknowledged"


def test_an_executor_error_finalizes_the_run_as_errored(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-002 — a broken executor records error, never silent success."""

    def broken(cursor, scope):
        cursor.execute("SELECT * FROM control.no_such_relation")
        return []

    good = _fixture_executors()["DQ-SHARED-001"]
    record = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors={"DQ-SHARED-001": good, "DQ-SHARED-002": broken},
    )
    assert record.overall_status == "error"
    assert "no_such_relation" in record.failure_summary

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT overall_status, failure_summary "
            "FROM control.data_quality_run WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        status, summary = cursor.fetchone()
        assert status == "error"
        assert "no_such_relation" in summary

        # The successful rule's evidence survived; the broken one wrote none.
        cursor.execute(
            "SELECT rule_id FROM control.data_quality_result WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        assert [row[0] for row in cursor.fetchall()] == ["DQ-SHARED-001"]


def test_a_runaway_rule_is_cancelled_and_the_rules_after_it_still_run(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-024 — one rule cannot hold the warehouse, or hide the others.

    The whole-relation DQ-ACS-007 ran 1 h 54 min in the weekly run of
    2026-11-01 until an operator cancelled it. Each rule now runs under a
    statement timeout: the slow rule records an error naming itself, its
    evidence is withdrawn, the rule sorted after it still records its own,
    the caller's timeout is back afterwards, and the run row's finished_at
    is when the run finished rather than when its transaction began.
    """

    def slow(cursor, scope):
        cursor.execute("SELECT pg_sleep(5)")
        return [RuleOutcome("raw_capture.response_capture", "pass")]

    good = _fixture_executors()["DQ-SHARED-002"]
    with postgres_connection.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = '7min'")
    record = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors={"DQ-SHARED-001": slow, "DQ-SHARED-002": good},
        rule_statement_timeout="200ms",
    )
    assert record.overall_status == "error"
    assert "DQ-SHARED-001" in record.failure_summary
    assert "statement timeout" in record.failure_summary
    assert "DQ-SHARED-002" not in record.failure_summary
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT rule_id FROM control.data_quality_result WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        assert [row[0] for row in cursor.fetchall()] == ["DQ-SHARED-002"]
        cursor.execute("SELECT current_setting('statement_timeout')")
        assert cursor.fetchone()[0] == "7min"
        # The run row closes when the run does, not when its transaction
        # began: the slow rule ran 200 ms first.
        cursor.execute(
            "SELECT finished_at - started_at FROM control.data_quality_run"
            " WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        assert cursor.fetchone()[0].total_seconds() >= 0.2
    postgres_connection.rollback()


def test_a_streaming_rule_commits_each_part_as_it_finishes(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DQ-024 — a sweep's finished partitions are visible while it runs.

    The DQ-ACS-007 full sweep of 2026-11-01 measured 2024 in ~65 minutes and
    nothing appeared in control.data_quality_result: the run was one
    transaction, committed at its end. Under `commit_progress` a streaming
    executor's outcomes are committed as they arrive: another session sees the
    first part (and the run row) before the second is measured, and when the
    rule then fails, the committed part and its failure stand while only the
    work after it is rolled back.
    """
    marker = f"stream-{uuid.uuid4().hex[:12]}"
    seen_mid_run: list[tuple[str, str]] = []

    @commits_each_outcome
    def streaming(cursor, scope):
        yield RuleOutcome(
            "control.ingestion_request",
            "fail",
            partition_key=f"{marker}-a",
            observed_count=1,
            expected_count=0,
            evidence=["request:example"],
        )
        observer = postgres_connection_factory()
        try:
            with observer.cursor() as watching:
                watching.execute(
                    "SELECT r.partition_key, q.overall_status"
                    "  FROM control.data_quality_result AS r"
                    "  JOIN control.data_quality_run AS q USING (quality_run_id)"
                    " WHERE r.partition_key LIKE %s",
                    (f"{marker}%",),
                )
                seen_mid_run.extend(watching.fetchall())
        finally:
            observer.close()
        cursor.execute("SELECT * FROM no_such_relation_for_streaming")
        yield RuleOutcome("control.ingestion_request", "pass")

    database = postgres_connection_factory()
    try:
        record = execute_rules(
            database,
            source_code="SHARED",
            assessment_type="manual",
            code_commit_sha=COMMIT_SHA,
            executors={"DQ-SHARED-002": streaming},
            commit_progress=True,
        )
        database.commit()
        assert seen_mid_run == [(f"{marker}-a", "running")]
        assert record.overall_status == "error"
        assert "no_such_relation_for_streaming" in record.failure_summary
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT partition_key, result FROM control.data_quality_result"
                " WHERE quality_run_id = %s",
                (record.quality_run_id,),
            )
            assert cursor.fetchall() == [(f"{marker}-a", "fail")]
    finally:
        database.rollback()
        # Evidence is append-only; this test's committed rows are removed
        # the way the capture tests remove theirs.
        with database.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE control.data_quality_result "
                "DISABLE TRIGGER data_quality_result_append_only"
            )
            cursor.execute(
                "DELETE FROM control.data_quality_result WHERE partition_key LIKE %s"
                " RETURNING quality_run_id",
                (f"{marker}%",),
            )
            runs = sorted({row[0] for row in cursor.fetchall()})
            if runs:
                cursor.execute(
                    "DELETE FROM control.data_quality_run"
                    " WHERE quality_run_id = ANY(%s::UUID[])",
                    ([str(run) for run in runs],),
                )
            cursor.execute(
                "ALTER TABLE control.data_quality_result "
                "ENABLE TRIGGER data_quality_result_append_only"
            )
        database.commit()
        database.close()


def test_a_streaming_rule_is_one_transaction_without_commit_progress(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-024 — release certification stays atomic: nothing commits early."""

    @commits_each_outcome
    def streaming(cursor, scope):
        yield RuleOutcome("control.ingestion_request", "pass", partition_key="a")
        yield RuleOutcome("control.ingestion_request", "pass", partition_key="b")

    record = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors={"DQ-SHARED-002": streaming},
    )
    assert record.overall_status == "pass"
    postgres_connection.rollback()
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM control.data_quality_run WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        assert cursor.fetchone()[0] == 0


def test_duplicate_evidence_within_one_run_is_rejected(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-002 — one run cannot record the same measurement twice."""

    def doubled(cursor, scope):
        outcome = RuleOutcome("raw_capture.response_capture", "pass")
        return [outcome, outcome]

    record = execute_rules(
        postgres_connection,
        source_code="SHARED",
        assessment_type="manual",
        code_commit_sha=COMMIT_SHA,
        executors={"DQ-SHARED-001": doubled},
    )
    assert record.overall_status == "error"
    assert "duplicate" in record.failure_summary.lower()
