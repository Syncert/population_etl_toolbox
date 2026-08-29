"""Source-neutral quality rule execution and evidence persistence.

The runner owns the DQ-002 contract: every rule execution persists one
``control.data_quality_run`` row and one ``control.data_quality_result`` row
per rule and evaluated object/partition. Evidence is append-only; a re-run
adds a new run rather than rewriting history. Rule *definitions* stay in
:mod:`data_ingestion_toolbox.quality.inventory`; executors registered here
provide the measurements.

Executors never mutate observations: the runner opens no write path to source
tables, and an executor that raises records an errored run with a bounded,
sanitized summary instead of half-written evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from .inventory import (
    ALL_RULES,
    QualityInventoryError,
    QualityRule,
    objects_by_name,
)

#: Result vocabulary persisted per rule/object/partition.
RESULTS: tuple[str, ...] = ("pass", "fail", "warn", "not_applicable")

#: Terminal run statuses, worst-first for aggregation.
_STATUS_ORDER: tuple[str, ...] = ("error", "fail", "warn", "pass")

_COMMIT_SHA_PATTERN = re.compile(r"\A[0-9a-f]{40}\Z")

_SUMMARY_LIMIT = 2000

#: Substrings that must never leak into a persisted failure summary.
_SENSITIVE_MARKERS: tuple[str, ...] = (
    "password",
    "passwd",
    "secret",
    "api_key",
    "apikey",
    "token",
    "authorization",
)


class QualityRunError(ValueError):
    """Raised when a quality run is configured inconsistently."""


@dataclass(frozen=True, slots=True)
class RuleOutcome:
    """One measured outcome for a rule against one object/partition."""

    object_name: str
    result: str
    partition_key: str = ""
    partition_detail: Mapping[str, Any] = field(default_factory=dict)
    observed_count: int | None = None
    expected_count: int | None = None
    observed_measure: float | None = None
    source_watermark: str | None = None
    latest_capture_id: str | None = None
    evidence: Sequence[Any] = ()

    def __post_init__(self) -> None:
        if self.result not in RESULTS:
            raise QualityRunError(
                f"Unknown result '{self.result}'; expected one of {', '.join(RESULTS)}."
            )


#: An executor measures one rule within the requested scope. It receives an
#: open database cursor and the scope mapping, and must not write.
RuleExecutor = Callable[[Any, Mapping[str, Any]], Iterable[RuleOutcome]]


@dataclass(frozen=True, slots=True)
class QualityRunRecord:
    """Summary of one persisted quality run."""

    quality_run_id: str
    overall_status: str
    rule_results: Mapping[str, tuple[str, ...]]
    failure_summary: str


def rule_set_version(rules: Sequence[QualityRule] = ALL_RULES) -> str:
    """Return a deterministic fingerprint of the declared rule set.

    The fingerprint changes when any rule id, severity, dimension, or object
    list changes, so persisted evidence always names the contract it was
    measured against.
    """
    canonical = json.dumps(
        [
            {
                "rule_id": rule.rule_id,
                "severity": rule.severity,
                "dimension": rule.dimension,
                "objects": list(rule.objects),
            }
            for rule in sorted(rules, key=lambda entry: entry.rule_id)
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def sanitize_summary(text: str, limit: int = _SUMMARY_LIMIT) -> str:
    """Bound a failure summary and strip obviously sensitive fragments."""
    lowered = text.lower()
    for marker in _SENSITIVE_MARKERS:
        if marker in lowered:
            return "[summary withheld: possibly sensitive content]"[:limit]
    collapsed = " ".join(text.split())
    return collapsed[:limit]


def overall_status(
    outcomes: Mapping[str, Sequence[RuleOutcome]],
    rules: Mapping[str, QualityRule],
    errored: bool = False,
) -> str:
    """Aggregate per-rule outcomes into one run status.

    A failing BLOCK or QUARANTINE rule fails the run; a failing WARN or INFO
    rule, or any warn outcome, leaves the run at ``warn`` — the plan forbids
    non-blocking severities from failing publication.
    """
    if errored:
        return "error"
    status = "pass"
    for rule_id, rule_outcomes in outcomes.items():
        severity = rules[rule_id].severity
        for outcome in rule_outcomes:
            if outcome.result == "fail" and severity in {"BLOCK", "QUARANTINE"}:
                return "fail"
            if outcome.result in {"fail", "warn"}:
                status = "warn"
    return status


def _validate_setup(
    rules: Sequence[QualityRule],
    executors: Mapping[str, RuleExecutor],
    code_commit_sha: str,
) -> dict[str, QualityRule]:
    if not _COMMIT_SHA_PATTERN.match(code_commit_sha):
        raise QualityRunError("code_commit_sha must be a full 40-character commit sha.")
    by_id = {rule.rule_id: rule for rule in rules}
    unknown = sorted(set(executors) - set(by_id))
    if unknown:
        raise QualityRunError(
            f"Executor(s) registered for unknown rule(s): {', '.join(unknown)}."
        )
    if not executors:
        raise QualityRunError("A quality run needs at least one executor.")
    return by_id


def execute_rules(
    connection: Any,
    *,
    source_code: str,
    assessment_type: str,
    code_commit_sha: str,
    executors: Mapping[str, RuleExecutor],
    scope: Mapping[str, Any] | None = None,
    rules: Sequence[QualityRule] = ALL_RULES,
    ingestion_run_id: str | None = None,
    publication_event_id: str | None = None,
) -> QualityRunRecord:
    """Execute the registered rule executors and persist their evidence.

    The run row is written first with status ``running``; results are inserted
    as each executor completes; the run row is then finalized exactly once.
    An executor exception finalizes the run as ``error`` with a sanitized
    summary — never a silent partial pass.
    """
    rules_by_id = _validate_setup(rules, executors, code_commit_sha)
    catalog = objects_by_name()
    quality_run_id = str(uuid.uuid4())
    version = rule_set_version(rules)
    scope_payload = json.dumps(dict(scope or {}), sort_keys=True)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.data_quality_run (
                quality_run_id, source_code, ingestion_run_id,
                publication_event_id, assessment_type, code_commit_sha,
                rule_set_version, evaluated_scope
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::JSONB)
            """,
            (
                quality_run_id,
                source_code,
                ingestion_run_id,
                publication_event_id,
                assessment_type,
                code_commit_sha,
                version,
                scope_payload,
            ),
        )

    def _persist_rule(rule_id: str) -> list[RuleOutcome]:
        rule = rules_by_id[rule_id]
        started = time.monotonic()
        with connection.cursor() as cursor:
            measured = list(executors[rule_id](cursor, dict(scope or {})))
        duration_ms = max(0, int((time.monotonic() - started) * 1000))
        for outcome in measured:
            entry = catalog.get(outcome.object_name)
            if entry is None:
                raise QualityInventoryError(
                    f"{rule_id}: outcome names unknown object '{outcome.object_name}'."
                )
            if outcome.result == "fail":
                failures.append(
                    f"{rule_id} {outcome.object_name}"
                    + (f" [{outcome.partition_key}]" if outcome.partition_key else "")
                )
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.data_quality_result (
                        quality_run_id, rule_id, severity, layer,
                        object_name, source_code, partition_key,
                        partition_detail, result, observed_count,
                        expected_count, observed_measure,
                        source_watermark, latest_capture_id, evidence,
                        duration_ms, review_status
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s,
                        %s, %s, %s, %s, %s::JSONB, %s, %s
                    )
                    """,
                    (
                        quality_run_id,
                        rule_id,
                        rule.severity,
                        entry.layer,
                        outcome.object_name,
                        entry.source,
                        outcome.partition_key,
                        json.dumps(dict(outcome.partition_detail), sort_keys=True),
                        outcome.result,
                        outcome.observed_count,
                        outcome.expected_count,
                        outcome.observed_measure,
                        outcome.source_watermark,
                        outcome.latest_capture_id,
                        json.dumps(list(outcome.evidence)),
                        duration_ms,
                        "open" if outcome.result == "warn" else None,
                    ),
                )
        return measured

    outcomes: dict[str, list[RuleOutcome]] = {}
    failures: list[str] = []
    error_summary = ""
    try:
        for rule_id in sorted(executors):
            # A savepoint keeps one failing rule from aborting the whole
            # transaction: the run row and prior results must still finalize.
            with connection.cursor() as cursor:
                cursor.execute("SAVEPOINT quality_rule")
            try:
                outcomes[rule_id] = _persist_rule(rule_id)
            except Exception:
                with connection.cursor() as cursor:
                    cursor.execute("ROLLBACK TO SAVEPOINT quality_rule")
                raise
            with connection.cursor() as cursor:
                cursor.execute("RELEASE SAVEPOINT quality_rule")
    except Exception as error:  # noqa: BLE001 - the error becomes evidence
        error_summary = sanitize_summary(f"{type(error).__name__}: {error}")

    status = overall_status(outcomes, rules_by_id, errored=bool(error_summary))
    summary = error_summary or sanitize_summary("; ".join(failures))
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE control.data_quality_run
               SET overall_status = %s,
                   finished_at = NOW(),
                   failure_summary = %s
             WHERE quality_run_id = %s
            """,
            (status, summary, quality_run_id),
        )

    return QualityRunRecord(
        quality_run_id=quality_run_id,
        overall_status=status,
        rule_results={
            rule_id: tuple(outcome.result for outcome in measured)
            for rule_id, measured in outcomes.items()
        },
        failure_summary=summary,
    )
