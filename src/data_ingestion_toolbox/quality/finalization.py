"""Finalize the control rows an aborted run abandoned, before assessment.

A run that stops without finishing owns the control state it started. Left
unfinished, that state is indistinguishable from work the warehouse still owes:
the slice-ledger and lineage rules count an abandoned attempt as missing work
forever, so a manually re-driven backfill can capture and publish the complete
registered window and the daily assessment still reports red. That was the
recorded live finding -- stale ``preflighted`` NASS slices and ``failed`` run
rows from aborted 1990 backfill iterations kept the sweep red while the
warehouse content was verified complete at every aggregation level.

The design decision (2026-08-31) is that **aborted runs finalize their control
rows, and that finalization happens before an assessment reads them**. The
alternative -- teaching every ledger rule to assess only the latest run per
partition -- would have made every rule carry its own notion of supersession
and would have left the control plane permanently inconsistent with itself.

This module is the sweep for runs that stopped before
:func:`data_ingestion_toolbox.capture.CaptureControl.finish_run` gained that
behavior, or that were stopped by something other than the capture control.
It is deliberately conservative:

- only runs already in a terminal aborted status are touched, so work still in
  flight is never cancelled by a background sweep;
- a request holding durable bytes finishes as ``captured``, never discarded;
- a ``success`` run is never repaired, because unfinished requests under a
  successful run are a real defect the assessment must keep reporting; and
- everything it changes is returned and logged, so a repair is inspectable
  rather than silent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping
from uuid import UUID

from data_ingestion_toolbox.capture import (
    ABORTED_RUN_STATUSES,
    UNFINISHED_REQUEST_STATUSES,
    RunFinalization,
    finalize_run_requests,
)

logger = logging.getLogger(__name__)

#: Bounded evidence, matching the assessment's own evidence limit.
EVIDENCE_LIMIT = 20

#: A ledger finalizer maps a cursor and the aborted run ids to a changed count.
LedgerFinalizer = Callable[[Any, list[str]], int]


@dataclass(frozen=True, slots=True)
class FinalizationReport:
    """Everything one sweep changed, for logging and operator inspection."""

    runs: tuple[RunFinalization, ...] = ()
    ledger_rows: Mapping[str, int] = field(default_factory=dict)

    @property
    def requests_changed(self) -> int:
        return sum(run.changed for run in self.runs)

    @property
    def ledger_rows_changed(self) -> int:
        return sum(self.ledger_rows.values())

    @property
    def changed(self) -> int:
        return self.requests_changed + self.ledger_rows_changed

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serializable summary for a task return value."""
        return {
            "runs_finalized": len(self.runs),
            "requests_captured": sum(run.captured for run in self.runs),
            "requests_failed": sum(run.failed for run in self.runs),
            "ledger_rows": dict(sorted(self.ledger_rows.items())),
            "run_ids": [str(run.run_id) for run in self.runs][:EVIDENCE_LIMIT],
        }


def _finalize_nass_slices(cursor: Any, run_ids: list[str]) -> int:
    """Retire slices an aborted USDA NASS run counted but never captured.

    ``preflighted`` means the provider's row count was read and the data
    request never followed. Under an aborted run it never will, so the slice
    is ``skipped`` -- a terminal state the ledger already understands. Slices
    that reached ``over_limit`` or ``partial`` are already terminal and are
    left exactly as they are: they are the evidence that the release was
    quarantined rather than ingested.
    """
    if not run_ids:
        return 0
    cursor.execute(
        """
        UPDATE control.usda_nass_slice AS slice
           SET status = 'skipped', updated_at = NOW()
         WHERE slice.run_id = ANY(%s::UUID[])
           AND slice.status = 'preflighted'
        """,
        (run_ids,),
    )
    return max(cursor.rowcount, 0)


#: Source-owned ledgers whose rows an aborted run must also finalize. The
#: ACS, BLS, and FRED ledgers are deliberately absent: they carry no run
#: linkage and are declarative registries of configured work, so a ``planned``
#: row there means the warehouse genuinely still owes that slice -- exactly
#: what those rules exist to report.
LEDGER_FINALIZERS: Mapping[str, LedgerFinalizer] = {
    "control.usda_nass_slice": _finalize_nass_slices,
}


def find_aborted_runs(
    cursor: Any, source_code: str | None = None
) -> list[tuple[UUID, str]]:
    """Return runs that stopped without finalizing their control rows."""
    clause = ""
    parameters: list[Any] = [
        sorted(ABORTED_RUN_STATUSES),
        list(UNFINISHED_REQUEST_STATUSES),
    ]
    if source_code:
        clause = " AND run.source_code = %s"
        parameters.append(source_code.strip().upper())
    cursor.execute(
        f"""
        SELECT DISTINCT run.run_id, run.source_code
          FROM control.ingestion_run AS run
         WHERE run.status = ANY(%s)
           AND (
               EXISTS (
                   SELECT 1 FROM control.ingestion_request AS request
                    WHERE request.run_id = run.run_id
                      AND request.status = ANY(%s)
               )
               OR EXISTS (
                   SELECT 1 FROM control.usda_nass_slice AS slice
                    WHERE slice.run_id = run.run_id
                      AND slice.status = 'preflighted'
               )
           ){clause}
         ORDER BY run.source_code, run.run_id
        """,
        tuple(parameters),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def finalize_aborted_runs(
    connection: Any, *, source_code: str | None = None
) -> FinalizationReport:
    """Finalize every aborted run's control rows in one transaction.

    The caller owns the transaction so this composes with the assessment that
    follows it: either the control plane is consistent before the rules read
    it, or nothing changed and the assessment reports the inconsistency.
    """
    with connection.cursor() as cursor:
        aborted = find_aborted_runs(cursor, source_code)
        if not aborted:
            return FinalizationReport()
        run_ids = [str(run_id) for run_id, _ in aborted]
        runs = tuple(
            finalize_run_requests(cursor, run_id, run_source)
            for run_id, run_source in aborted
        )
        ledger_rows = {
            name: finalizer(cursor, run_ids)
            for name, finalizer in LEDGER_FINALIZERS.items()
        }
    report = FinalizationReport(
        runs=tuple(run for run in runs if run.changed),
        ledger_rows={name: count for name, count in ledger_rows.items() if count},
    )
    if report.changed:
        logger.warning(
            "Finalized %d aborted run(s) before assessment: %d request(s) "
            "(%d already captured, %d failed) and %d source ledger row(s). "
            "Run ids: %s",
            len(report.runs),
            report.requests_changed,
            sum(run.captured for run in report.runs),
            sum(run.failed for run in report.runs),
            report.ledger_rows_changed,
            ", ".join(str(run.run_id) for run in report.runs[:EVIDENCE_LIMIT]),
        )
    return report
