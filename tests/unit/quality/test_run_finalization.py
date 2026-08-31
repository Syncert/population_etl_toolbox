"""Deterministic contracts for aborted-run control finalization.

Covers: DQ-002 — a run that stops without finishing brings its own control
rows to a terminal state, so an abandoned attempt is never counted as work the
warehouse still owes. The database behavior is proven in
``tests/integration/database/test_run_finalization.py``; these tests pin the
statements and the decision boundaries without a warehouse.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from data_ingestion_toolbox.capture import (
    ABORTED_RUN_STATUSES,
    UNFINISHED_REQUEST_STATUSES,
    CaptureControl,
    RunFinalization,
    finalize_run_requests,
)
from data_ingestion_toolbox.quality.finalization import (
    LEDGER_FINALIZERS,
    FinalizationReport,
    finalize_aborted_runs,
)

pytestmark = pytest.mark.unit


class RecordingCursor:
    """Capture executed statements and answer with scripted rows."""

    def __init__(self, rowcounts: list[int] | None = None, rows: list | None = None):
        self.statements: list[tuple[str, tuple]] = []
        self._rowcounts = list(rowcounts or [])
        self._rows = list(rows or [])
        self.rowcount = 0

    def execute(self, statement: str, parameters: tuple = ()) -> None:
        self.statements.append((statement, parameters))
        self.rowcount = self._rowcounts.pop(0) if self._rowcounts else 0

    def fetchall(self) -> list:
        return self._rows.pop(0) if self._rows else []

    def __enter__(self) -> "RecordingCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_success_is_not_an_aborted_status() -> None:
    """Covers: DQ-002 — a success holding unfinished requests stays a defect."""
    assert "success" not in ABORTED_RUN_STATUSES
    assert ABORTED_RUN_STATUSES == {"failed", "cancelled", "partial"}


def test_finalization_only_targets_statuses_without_an_outcome() -> None:
    """Covers: DQ-002 — only work without an outcome is finalized."""
    assert UNFINISHED_REQUEST_STATUSES == ("planned", "running")


def test_captured_bytes_finish_as_captured_and_the_rest_fail() -> None:
    """Covers: DQ-002 — durable bytes are never discarded by a repair."""
    cursor = RecordingCursor(rowcounts=[2, 3])
    run_id = uuid4()

    result = finalize_run_requests(cursor, run_id, "CDC")

    assert result == RunFinalization(run_id, "CDC", captured=2, failed=3)
    assert result.changed == 5
    captured_statement, captured_parameters = cursor.statements[0]
    failed_statement, failed_parameters = cursor.statements[1]
    assert "status = 'captured'" in captured_statement
    assert "EXISTS (" in captured_statement
    assert "NOT EXISTS (" not in captured_statement
    assert "status = 'failed'" in failed_statement
    assert "NOT EXISTS (" in failed_statement
    # Both statements are scoped to one run, one source, and unfinished work.
    assert captured_parameters[0] == str(run_id)
    assert captured_parameters[1] == "CDC"
    assert captured_parameters[2] == list(UNFINISHED_REQUEST_STATUSES)
    assert failed_parameters[1] == str(run_id)
    assert failed_parameters[2] == "CDC"


def test_finishing_a_successful_run_repairs_nothing() -> None:
    """Covers: DQ-002 — a repair never fires on the success path."""
    cursor = RecordingCursor()
    connection = RecordingConnection(cursor)
    control = CaptureControl(lambda: connection, source_code="CDC")

    result = control.finish_run(uuid4(), status="success")

    assert result.changed == 0
    assert len(cursor.statements) == 1
    assert "control.ingestion_run" in cursor.statements[0][0]


@pytest.mark.parametrize("status", sorted(ABORTED_RUN_STATUSES))
def test_finishing_an_aborted_run_finalizes_in_the_same_transaction(
    status: str,
) -> None:
    """Covers: DQ-002 — the run and the rows it abandons stop together."""
    cursor = RecordingCursor(rowcounts=[0, 1, 2])
    connection = RecordingConnection(cursor)
    control = CaptureControl(lambda: connection, source_code="USDA_NASS")

    result = control.finish_run(uuid4(), status=status, error="aborted")

    assert result.changed == 3
    assert (
        [statement for statement, _ in cursor.statements][0]
        .strip()
        .startswith("UPDATE control.ingestion_run")
    )
    assert len(cursor.statements) == 3
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_a_sweep_with_nothing_to_finalize_changes_nothing() -> None:
    """Covers: DQ-002 — no aborted run means no statement and no report."""
    cursor = RecordingCursor(rows=[[]])
    connection = RecordingConnection(cursor)

    report = finalize_aborted_runs(connection)

    assert report == FinalizationReport()
    assert report.changed == 0
    assert len(cursor.statements) == 1


def test_the_sweep_reports_everything_it_changed() -> None:
    """Covers: DQ-002 — a repair is inspectable, never silent."""
    run_id = uuid4()
    cursor = RecordingCursor(
        rowcounts=[0, 1, 2, 4],
        rows=[[(run_id, "USDA_NASS")]],
    )
    connection = RecordingConnection(cursor)

    report = finalize_aborted_runs(connection)

    assert report.requests_changed == 3
    assert report.ledger_rows == {"control.usda_nass_slice": 4}
    assert report.changed == 7
    summary = report.as_dict()
    assert summary["runs_finalized"] == 1
    assert summary["requests_captured"] == 1
    assert summary["requests_failed"] == 2
    assert summary["run_ids"] == [str(run_id)]


def test_only_run_linked_ledgers_are_finalized() -> None:
    """Covers: DQ-004 — only run-linked ledgers are finalized.

    The ACS, BLS, and FRED registries carry no run linkage by design. A
    ``planned`` row in those ledgers means the warehouse genuinely still owes
    that slice, which is exactly what their rules exist to report; finalizing
    them would erase the finding instead of the abandonment.
    """
    assert set(LEDGER_FINALIZERS) == {"control.usda_nass_slice"}


def test_the_nass_finalizer_retires_only_preflighted_slices() -> None:
    """Covers: DQ-004 — an over-limit slice is already terminal evidence."""
    cursor = RecordingCursor(rowcounts=[7])
    changed = LEDGER_FINALIZERS["control.usda_nass_slice"](cursor, [str(uuid4())])

    statement, _parameters = cursor.statements[0]
    assert changed == 7
    assert "status = 'skipped'" in statement
    assert "slice.status = 'preflighted'" in statement
    assert "over_limit" not in statement
    assert "partial" not in statement


def test_the_nass_finalizer_is_a_no_op_without_aborted_runs() -> None:
    """Covers: DQ-004 — no aborted run means the ledger is left alone."""
    cursor = RecordingCursor(rowcounts=[3])
    assert LEDGER_FINALIZERS["control.usda_nass_slice"](cursor, []) == 0
    assert cursor.statements == []
