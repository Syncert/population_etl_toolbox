"""DQ-002 — deterministic runner contracts that need no database."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.quality.inventory import ALL_RULES, QualityRule
from data_ingestion_toolbox.quality.runner import (
    QualityRunError,
    RuleOutcome,
    _validate_setup,
    overall_status,
    rule_set_version,
    sanitize_summary,
)

pytestmark = pytest.mark.unit


def _rule(rule_id: str, severity: str) -> QualityRule:
    return QualityRule(
        rule_id=rule_id,
        severity=severity,
        dimension="uniqueness",
        summary="Example.",
        objects=("raw_capture.response_capture",),
    )


def test_rule_set_version_is_stable_and_change_sensitive() -> None:
    """Covers: DQ-001 — evidence must name the exact rule contract it measured."""
    baseline = rule_set_version()
    assert baseline == rule_set_version(list(reversed(ALL_RULES)))

    changed = (*ALL_RULES[:-1], _rule("DQ-SHARED-900", "BLOCK"))
    assert rule_set_version(changed) != baseline


def test_blocking_failure_fails_the_run() -> None:
    """Covers: DQ-002 — a failing BLOCK rule makes publication unsafe."""
    rules = {"DQ-A-001": _rule("DQ-A-001", "BLOCK")}
    outcomes = {"DQ-A-001": [RuleOutcome("raw_capture.response_capture", "fail")]}
    assert overall_status(outcomes, rules) == "fail"


def test_non_blocking_failure_only_warns_the_run() -> None:
    """Covers: DQ-002 — WARN severities persist evidence without failing."""
    rules = {"DQ-A-001": _rule("DQ-A-001", "WARN")}
    outcomes = {"DQ-A-001": [RuleOutcome("raw_capture.response_capture", "fail")]}
    assert overall_status(outcomes, rules) == "warn"


def test_warn_outcomes_and_clean_passes_aggregate_correctly() -> None:
    """Covers: DQ-002 — pass stays pass; any warn outcome lifts to warn."""
    rules = {
        "DQ-A-001": _rule("DQ-A-001", "BLOCK"),
        "DQ-A-002": _rule("DQ-A-002", "INFO"),
    }
    passing = {
        "DQ-A-001": [RuleOutcome("raw_capture.response_capture", "pass")],
        "DQ-A-002": [RuleOutcome("raw_capture.response_capture", "not_applicable")],
    }
    assert overall_status(passing, rules) == "pass"

    warned = {
        "DQ-A-001": [RuleOutcome("raw_capture.response_capture", "pass")],
        "DQ-A-002": [RuleOutcome("raw_capture.response_capture", "warn")],
    }
    assert overall_status(warned, rules) == "warn"


def test_an_errored_run_is_never_a_pass() -> None:
    """Covers: DQ-002 — an execution error is evidence, not silence."""
    assert overall_status({}, {}, errored=True) == "error"


def test_unknown_result_vocabulary_is_rejected() -> None:
    """Covers: DQ-002 — only declared result values reach the evidence store."""
    with pytest.raises(QualityRunError, match="Unknown result"):
        RuleOutcome("raw_capture.response_capture", "skipped")


def test_summaries_are_bounded_and_secret_safe() -> None:
    """Covers: DQ-002 — persisted summaries stay bounded and sanitized."""
    assert len(sanitize_summary("x" * 5000)) == 2000
    assert sanitize_summary("db password=hunter2 leaked") == (
        "[summary withheld: possibly sensitive content]"
    )
    assert sanitize_summary("plain   failure\ntext") == "plain failure text"


def test_setup_rejects_unknown_rules_and_bad_shas() -> None:
    """Covers: DQ-002 — a run must name real rules and a full commit sha."""
    executor = {"DQ-NOPE-001": lambda cursor, scope: []}
    with pytest.raises(QualityRunError, match="unknown rule"):
        _validate_setup(ALL_RULES, executor, "a" * 40)
    with pytest.raises(QualityRunError, match="commit sha"):
        _validate_setup(ALL_RULES, {}, "short")
    with pytest.raises(QualityRunError, match="at least one executor"):
        _validate_setup(ALL_RULES, {}, "a" * 40)
