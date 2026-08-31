"""Deterministic assessment, plausibility, and certification contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.quality.assessment import (
    DAILY_LEDGER_RULES,
    PLAUSIBILITY_EXECUTORS,
    AssessmentError,
    resolve_commit_sha,
    select_executors,
)
from data_ingestion_toolbox.quality.plausibility import robust_change_verdict
from data_ingestion_toolbox.quality.reconciliation import (
    SHARED_RECONCILIATION_EXECUTORS,
)
from data_ingestion_toolbox.quality.sources import SOURCE_EXECUTORS

pytestmark = pytest.mark.unit


def test_daily_cadence_runs_the_bounded_control_sweep() -> None:
    """Covers: DQ-005 — daily checks ledgers and lineage, not full silver/gold."""
    selected = select_executors("daily")
    assert set(SHARED_RECONCILIATION_EXECUTORS) <= set(selected)
    assert set(selected) - set(SHARED_RECONCILIATION_EXECUTORS) == set(
        DAILY_LEDGER_RULES
    )


def test_weekly_cadence_runs_every_deterministic_executor() -> None:
    """Covers: DQ-005 — weekly is the full configured-scope reconciliation."""
    selected = select_executors("weekly")
    assert set(selected) == (
        set(SHARED_RECONCILIATION_EXECUTORS) | set(SOURCE_EXECUTORS)
    )


def test_monthly_cadence_adds_warn_only_plausibility() -> None:
    """Covers: DQ-005, DQ-006 — plausibility joins only the monthly sweep."""
    monthly = select_executors("monthly")
    weekly = select_executors("weekly")
    assert set(monthly) - set(weekly) == set(PLAUSIBILITY_EXECUTORS)


def test_targeting_narrows_to_one_rule_or_source() -> None:
    """Covers: DQ-005 — repair verification can target one rule or source."""
    assert set(select_executors("weekly", rule_id="DQ-CDC-002")) == {"DQ-CDC-002"}

    narrowed = set(select_executors("weekly", source_code="USDA_NASS"))
    assert "DQ-NASS-002" in narrowed
    assert "DQ-NASS-003" in narrowed
    assert all(key.startswith(("DQ-NASS-", "DQ-SHARED-")) for key in narrowed)

    with pytest.raises(AssessmentError, match="No executor is registered"):
        select_executors("weekly", rule_id="DQ-NOPE-001")
    with pytest.raises(AssessmentError, match="Unknown source code"):
        select_executors("weekly", source_code="NOT_A_SOURCE")
    with pytest.raises(AssessmentError, match="Unknown cadence"):
        select_executors("hourly")


def test_commit_sha_resolves_explicitly_or_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: DQ-007 — evidence is always tied to one full commit sha."""
    sha = "c" * 40
    assert resolve_commit_sha(sha) == sha

    monkeypatch.setenv("DATA_QUALITY_COMMIT_SHA", "d" * 40)
    assert resolve_commit_sha() == "d" * 40

    monkeypatch.delenv("DATA_QUALITY_COMMIT_SHA")
    monkeypatch.delenv("GIT_COMMIT_SHA", raising=False)
    with pytest.raises(AssessmentError, match="No commit SHA"):
        resolve_commit_sha("not-a-sha")


def test_short_history_supports_no_plausibility_verdict() -> None:
    """Covers: DQ-006 — a minimum history is required before any verdict."""
    assert robust_change_verdict([1.0, 2.0, 3.0], 100.0) is None


def test_extreme_change_is_flagged_and_normal_change_is_not() -> None:
    """Covers: DQ-006 — robust scores flag outliers without a global threshold."""
    history = [100.0, 101.0, 99.0, 100.5, 100.2, 99.8, 100.1, 99.9]
    normal = robust_change_verdict(history, 100.4)
    assert normal is not None and not normal.flagged

    extreme = robust_change_verdict(history, 250.0)
    assert extreme is not None and extreme.flagged
    assert extreme.score > 6.0


def test_a_constant_series_flags_only_an_actual_change() -> None:
    """Covers: DQ-006 — zero spread means any change is notable, WARN-only."""
    history = [5.0] * 10
    unchanged = robust_change_verdict(history, 5.0)
    assert unchanged is not None and not unchanged.flagged

    changed = robust_change_verdict(history, 5.1)
    assert changed is not None and changed.flagged
