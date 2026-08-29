"""Plausibility baselines and the warning review lifecycle (DQ-006).

Everything here is WARN-only by contract: an anomaly is reviewable evidence,
never automatic invalidation, and no rule in this module may mutate,
winsorize, interpolate, or delete a provider value. Baselines are robust
(median and MAD) and segmented per series, with a minimum-history requirement
so a short series cannot alarm; there is no global percentage threshold.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .reconciliation import EVIDENCE_LIMIT
from .runner import RuleOutcome

#: A series shorter than this publishes no plausibility verdict at all.
DEFAULT_MIN_HISTORY = 8

#: Robust z-score above which a change is flagged for review.
DEFAULT_SCORE_THRESHOLD = 6.0

#: Scale factor that makes the MAD a consistent sigma estimate.
_MAD_SIGMA = 1.4826


@dataclass(frozen=True, slots=True)
class PlausibilityVerdict:
    """One robust-change measurement for a single series."""

    score: float
    flagged: bool
    baseline_median: float
    baseline_size: int


def robust_change_verdict(
    history: Sequence[float],
    latest: float,
    *,
    min_history: int = DEFAULT_MIN_HISTORY,
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
) -> PlausibilityVerdict | None:
    """Score the latest value against its own robust history.

    Returns ``None`` when the history is too short to support a verdict.
    A zero MAD (a constant series) flags only an actual change of value,
    scored as infinite, because any deviation from a constant is notable
    while remaining WARN-only.
    """
    if len(history) < min_history:
        return None
    baseline = sorted(history)
    median = statistics.median(baseline)
    mad = statistics.median(abs(value - median) for value in baseline)
    if mad == 0:
        changed = latest != median
        return PlausibilityVerdict(
            score=float("inf") if changed else 0.0,
            flagged=changed,
            baseline_median=median,
            baseline_size=len(baseline),
        )
    score = abs(latest - median) / (mad * _MAD_SIGMA)
    return PlausibilityVerdict(
        score=score,
        flagged=score > score_threshold,
        baseline_median=median,
        baseline_size=len(baseline),
    )


def fred_change_plausibility(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FRED-006 — unusually large FRED changes warn, never invalidate."""
    min_history = int(scope.get("min_history", DEFAULT_MIN_HISTORY))
    threshold = float(scope.get("score_threshold", DEFAULT_SCORE_THRESHOLD))

    cursor.execute(
        """
        SELECT series.series_id, fact.observation_date, fact.value
          FROM gold_fred.fact_fred_observation AS fact
          JOIN gold_fred.dim_fred_series AS series
            ON series.fred_series_sk = fact.fred_series_sk
         WHERE fact.value IS NOT NULL
         ORDER BY series.series_id, fact.observation_date
        """
    )
    by_series: dict[str, list[float]] = {}
    for series_id, _observation_date, value in cursor.fetchall():
        by_series.setdefault(series_id, []).append(float(value))

    if not by_series:
        return [RuleOutcome("gold_fred.fact_fred_observation", "not_applicable")]

    outcomes: list[RuleOutcome] = []
    clean = 0
    for series_id, values in sorted(by_series.items()):
        verdict = robust_change_verdict(
            values[:-1],
            values[-1],
            min_history=min_history,
            score_threshold=threshold,
        )
        if verdict is None or not verdict.flagged:
            clean += 1
            continue
        outcomes.append(
            RuleOutcome(
                "gold_fred.fact_fred_observation",
                "warn",
                partition_key=series_id,
                observed_measure=(
                    None if verdict.score == float("inf") else verdict.score
                ),
                observed_count=verdict.baseline_size,
                evidence=[
                    f"latest={values[-1]}",
                    f"baseline_median={verdict.baseline_median}",
                ][:EVIDENCE_LIMIT],
            )
        )
    if not outcomes:
        return [
            RuleOutcome(
                "gold_fred.fact_fred_observation",
                "pass",
                observed_count=clean,
                expected_count=clean,
            )
        ]
    return outcomes


#: Review transitions an operator may record on a warning.
REVIEW_STATUSES: tuple[str, ...] = ("open", "acknowledged", "accepted", "escalated")


def record_warning_review(connection: Any, result_id: int, review_status: str) -> None:
    """Advance one warning's review state — the sole permitted mutation.

    The database trigger rejects any other change, so review history can be
    trusted: a reviewed warning was a warning, with its original evidence.
    """
    if review_status not in REVIEW_STATUSES:
        raise ValueError(
            f"Unknown review status '{review_status}'; expected one of "
            f"{', '.join(REVIEW_STATUSES)}."
        )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE control.data_quality_result
               SET review_status = %s
             WHERE result_id = %s AND result = 'warn'
            """,
            (review_status, result_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"Result {result_id} is not a reviewable warning.")
