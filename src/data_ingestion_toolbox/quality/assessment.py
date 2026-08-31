"""Scheduled assessment orchestration and release certification (DQ-005/007).

``run_scheduled_assessment`` is the callable behind the independent
``warehouse_data_quality`` DAG: it selects the executor set for a cadence
(or a single source/rule target), runs them through the evidence runner, and
never mutates source observations — the only tables it writes are the
quality-evidence relations.

``certify_release`` is the DQ-007 gate for deployments: one full release
assessment tied to a single immutable commit SHA, summarized into a
promotability verdict. A release with blocking failures is not promotable,
whatever else is green.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

from .plausibility import fred_change_plausibility
from .reconciliation import SHARED_RECONCILIATION_EXECUTORS
from .runner import QualityRunRecord, RuleExecutor, execute_rules
from .sources import SOURCE_EXECUTORS

#: Assessment cadences the scheduled DAG resolves at runtime.
CADENCES: tuple[str, ...] = ("daily", "weekly", "monthly")

#: WARN-only plausibility executors join only the monthly sweep.
PLAUSIBILITY_EXECUTORS: Mapping[str, RuleExecutor] = {
    "DQ-FRED-006": fred_change_plausibility,
}

#: Fast control-plane checks the daily sweep runs beside the shared lineage
#: rules: ledgers, watermark monotonicity, and publisher liveness. The full
#: silver/gold reconciliation waits for the weekly sweep.
DAILY_LEDGER_RULES: frozenset[str] = frozenset(
    {
        "DQ-ACS-002",
        "DQ-BLS-002",
        "DQ-FRED-002",
        "DQ-NASS-002",
        "DQ-CDC-002",
        "DQ-GLOSSARY-001",
    }
)

_COMMIT_SHA_PATTERN = re.compile(r"\A[0-9a-f]{40}\Z")

_COMMIT_ENVIRONMENT_VARIABLES = ("DATA_QUALITY_COMMIT_SHA", "GIT_COMMIT_SHA")


class AssessmentError(ValueError):
    """Raised when a scheduled assessment is configured inconsistently."""


def resolve_commit_sha(explicit: str | None = None) -> str:
    """Resolve the commit SHA evidence rows are certified against."""
    candidates = [explicit] if explicit else []
    candidates.extend(
        os.environ.get(name, "") for name in _COMMIT_ENVIRONMENT_VARIABLES
    )
    for candidate in candidates:
        if candidate and _COMMIT_SHA_PATTERN.match(candidate):
            return candidate
    raise AssessmentError(
        "No commit SHA available: pass one explicitly or set "
        + " or ".join(_COMMIT_ENVIRONMENT_VARIABLES)
        + " to the full 40-character sha."
    )


def select_executors(
    cadence: str,
    *,
    source_code: str | None = None,
    rule_id: str | None = None,
) -> dict[str, RuleExecutor]:
    """Choose the executor set for one scheduled assessment.

    - ``daily``: the shared lineage sweep plus every source ledger check —
      fresh failures, quarantines, and newly landed partitions.
    - ``weekly``: the full configured-scope reconciliation (all deterministic
      executors).
    - ``monthly``: the weekly set plus WARN-only plausibility baselines.
    - ``rule_id`` narrows to one rule; ``source_code`` narrows to one
      source's rules plus the shared sweep, for repair verification.
    """
    if cadence not in CADENCES:
        raise AssessmentError(
            f"Unknown cadence '{cadence}'; expected one of {', '.join(CADENCES)}."
        )
    executors: dict[str, RuleExecutor] = dict(SHARED_RECONCILIATION_EXECUTORS)
    if cadence == "daily":
        executors.update(
            {
                key: value
                for key, value in SOURCE_EXECUTORS.items()
                if key in DAILY_LEDGER_RULES
            }
        )
    else:
        executors.update(SOURCE_EXECUTORS)
    if cadence == "monthly":
        executors.update(PLAUSIBILITY_EXECUTORS)

    if rule_id is not None:
        universe = {
            **SHARED_RECONCILIATION_EXECUTORS,
            **SOURCE_EXECUTORS,
            **PLAUSIBILITY_EXECUTORS,
        }
        if rule_id not in universe:
            raise AssessmentError(f"No executor is registered for '{rule_id}'.")
        return {rule_id: universe[rule_id]}

    if source_code is not None:
        prefix = _rule_prefix(source_code)
        narrowed = {
            key: value
            for key, value in executors.items()
            if key.startswith(f"DQ-{prefix}-") or key.startswith("DQ-SHARED-")
        }
        if not any(key.startswith(f"DQ-{prefix}-") for key in narrowed):
            raise AssessmentError(
                f"No source-specific executors exist for '{source_code}'."
            )
        return narrowed
    return executors


def _rule_prefix(source_code: str) -> str:
    prefixes = {
        "CENSUS_ACS": "ACS",
        "BLS": "BLS",
        "FRED": "FRED",
        "CENSUS_PEP": "PEP",
        "CDC": "CDC",
        "FBI_UCR": "FBI",
        "USDA_NASS": "NASS",
        "SHARED": "SHARED",
    }
    if source_code not in prefixes:
        raise AssessmentError(f"Unknown source code '{source_code}'.")
    return prefixes[source_code]


def run_scheduled_assessment(
    connection: Any,
    *,
    cadence: str,
    source_code: str | None = None,
    rule_id: str | None = None,
    scope: Mapping[str, Any] | None = None,
    code_commit_sha: str | None = None,
) -> QualityRunRecord:
    """Run one scheduled assessment and persist its evidence."""
    executors = select_executors(cadence, source_code=source_code, rule_id=rule_id)
    merged_scope = dict(scope or {})
    merged_scope.setdefault("cadence", cadence)
    if source_code:
        merged_scope.setdefault("source_code", source_code)
    return execute_rules(
        connection,
        source_code=source_code or "SHARED",
        assessment_type="scheduled",
        code_commit_sha=resolve_commit_sha(code_commit_sha),
        executors=executors,
        scope=merged_scope,
    )


@dataclass(frozen=True, slots=True)
class ReleaseCertification:
    """The stored verdict of one release assessment."""

    quality_run_id: str
    code_commit_sha: str
    rule_set_version: str
    overall_status: str
    totals: Mapping[tuple[str, str], int]
    promotable: bool

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serializable release evidence artifact."""
        return {
            "quality_run_id": self.quality_run_id,
            "code_commit_sha": self.code_commit_sha,
            "rule_set_version": self.rule_set_version,
            "overall_status": self.overall_status,
            "totals": {
                f"{severity}:{result}": count
                for (severity, result), count in sorted(self.totals.items())
            },
            "promotable": self.promotable,
        }


def certify_release(
    connection: Any,
    *,
    code_commit_sha: str | None = None,
    scope: Mapping[str, Any] | None = None,
) -> ReleaseCertification:
    """Run the full deterministic suite as one release certification.

    Promotable means: the run finished (no error), and no BLOCK or QUARANTINE
    rule failed. Warnings never block promotion — but they are counted, so
    the artifact shows exactly what a reviewer is accepting.
    """
    sha = resolve_commit_sha(code_commit_sha)
    executors = {**SHARED_RECONCILIATION_EXECUTORS, **SOURCE_EXECUTORS}
    record = execute_rules(
        connection,
        source_code="SHARED",
        assessment_type="release",
        code_commit_sha=sha,
        executors=executors,
        scope=scope,
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT severity, result, COUNT(*)
              FROM control.data_quality_result
             WHERE quality_run_id = %s
             GROUP BY severity, result
            """,
            (record.quality_run_id,),
        )
        totals = {(severity, result): count for severity, result, count in cursor}
        cursor.execute(
            "SELECT rule_set_version FROM control.data_quality_run"
            " WHERE quality_run_id = %s",
            (record.quality_run_id,),
        )
        rule_set = cursor.fetchone()[0]

    blocking_failures = sum(
        count
        for (severity, result), count in totals.items()
        if result == "fail" and severity in {"BLOCK", "QUARANTINE"}
    )
    return ReleaseCertification(
        quality_run_id=record.quality_run_id,
        code_commit_sha=sha,
        rule_set_version=rule_set,
        overall_status=record.overall_status,
        totals=totals,
        promotable=(
            record.overall_status in {"pass", "warn"} and blocking_failures == 0
        ),
    )
