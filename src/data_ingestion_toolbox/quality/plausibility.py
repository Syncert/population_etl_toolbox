"""Plausibility baselines and the warning review lifecycle (DQ-006).

Everything here is WARN-only by contract: an anomaly is reviewable evidence,
never automatic invalidation, and no rule in this module may mutate,
winsorize, interpolate, or delete a provider value. Baselines are robust
(median and MAD) and segmented per series, with a minimum-history requirement
so a short series cannot alarm; there is no global percentage threshold.

A baseline is only as trustworthy as the history it learns from. Learning from
whatever happens to be retained lets material the deterministic rules currently
reject teach the baseline what "normal" means, which fails in the direction
that matters: a bad value pulls the median toward itself and the next genuine
anomaly scores lower. Since DQ-007 gave the warehouse a release certification,
the baseline is restricted to the history that certification covered -- and
when nothing is certified, or the object is currently failing a blocking rule,
the honest verdict is that plausibility cannot be judged at all.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

from .reconciliation import EVIDENCE_LIMIT
from .runner import RuleOutcome

#: A series shorter than this publishes no plausibility verdict at all.
DEFAULT_MIN_HISTORY = 8

#: Robust z-score above which a change is flagged for review.
DEFAULT_SCORE_THRESHOLD = 6.0

#: Scale factor that makes the MAD a consistent sigma estimate.
_MAD_SIGMA = 1.4826

#: Severities whose current failure disqualifies an object from being a
#: plausibility baseline: the deterministic suite says the material is wrong.
_BLOCKING_SEVERITIES: tuple[str, ...] = ("BLOCK", "QUARANTINE")


@dataclass(frozen=True, slots=True)
class CertifiedScope:
    """The warehouse state a plausibility baseline is allowed to learn from."""

    #: When the newest promotable release certification finished. ``None``
    #: means nothing has been certified and no baseline may be built.
    certified_at: datetime | None = None
    #: The commit that certification was tied to, for the evidence trail.
    code_commit_sha: str | None = None
    #: Objects a blocking deterministic rule currently reports as failing.
    failing_objects: frozenset[str] = frozenset()

    @property
    def is_certified(self) -> bool:
        return self.certified_at is not None

    def admits(self, object_name: str) -> bool:
        """Whether this object may contribute a plausibility baseline."""
        return self.is_certified and object_name not in self.failing_objects

    def reason(self, object_name: str) -> str:
        """Why an object was refused, for bounded evidence."""
        if not self.is_certified:
            return "no promotable release certification exists"
        if object_name in self.failing_objects:
            return f"{object_name} is failing a blocking deterministic rule"
        return ""


def load_certified_scope(cursor: Any) -> CertifiedScope:
    """Read the certified baseline boundary from the quality evidence.

    A release run counts as certification only when it is promotable on the
    same terms ``certify_release`` applies: it finished, and no BLOCK or
    QUARANTINE rule failed inside it.
    """
    cursor.execute(
        """
        SELECT run.finished_at, run.code_commit_sha
          FROM control.data_quality_run AS run
         WHERE run.assessment_type = 'release'
           AND run.overall_status IN ('pass', 'warn')
           AND run.finished_at IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM control.data_quality_result AS result
                WHERE result.quality_run_id = run.quality_run_id
                  AND result.result = 'fail'
                  AND result.severity = ANY(%s)
           )
         ORDER BY run.finished_at DESC
         LIMIT 1
        """,
        (list(_BLOCKING_SEVERITIES),),
    )
    certification = cursor.fetchone()

    cursor.execute(
        """
        SELECT DISTINCT object_name
          FROM control.data_quality_latest_result
         WHERE result = 'fail' AND severity = ANY(%s)
        """,
        (list(_BLOCKING_SEVERITIES),),
    )
    failing = frozenset(row[0] for row in cursor.fetchall())

    if certification is None:
        return CertifiedScope(failing_objects=failing)
    return CertifiedScope(
        certified_at=certification[0],
        code_commit_sha=certification[1],
        failing_objects=failing,
    )


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
    """DQ-FRED-006 — unusually large FRED changes warn, never invalidate.

    The baseline is built only from observations a promotable release
    certification already covered. A value ingested after that certification is
    scored *against* the certified history rather than joining it, so material
    the warehouse has not certified can never teach the baseline what normal
    looks like.
    """
    min_history = int(scope.get("min_history", DEFAULT_MIN_HISTORY))
    threshold = float(scope.get("score_threshold", DEFAULT_SCORE_THRESHOLD))
    object_name = "gold_fred.fact_fred_observation"

    certified = load_certified_scope(cursor)
    if not certified.admits(object_name):
        return [
            RuleOutcome(
                object_name,
                "not_applicable",
                evidence=[certified.reason(object_name)],
            )
        ]

    cursor.execute(
        """
        SELECT series.series_id, fact.observation_date, fact.value,
               fact.updated_at <= %s AS is_certified
          FROM gold_fred.fact_fred_observation AS fact
          JOIN gold_fred.dim_fred_series AS series
            ON series.fred_series_sk = fact.fred_series_sk
         WHERE fact.value IS NOT NULL
         ORDER BY series.series_id, fact.observation_date
        """,
        (certified.certified_at,),
    )
    by_series: dict[str, list[tuple[float, bool]]] = {}
    for series_id, _observation_date, value, is_certified in cursor.fetchall():
        by_series.setdefault(series_id, []).append((float(value), bool(is_certified)))

    if not by_series:
        return [RuleOutcome(object_name, "not_applicable")]

    outcomes: list[RuleOutcome] = []
    clean = 0
    for series_id, observations in sorted(by_series.items()):
        values = [value for value, _certified in observations]
        history = [
            value
            for index, (value, is_certified) in enumerate(observations)
            if is_certified and index != len(observations) - 1
        ]
        verdict = robust_change_verdict(
            history,
            values[-1],
            min_history=min_history,
            score_threshold=threshold,
        )
        if verdict is None or not verdict.flagged:
            clean += 1
            continue
        outcomes.append(
            RuleOutcome(
                object_name,
                "warn",
                partition_key=series_id,
                observed_measure=(
                    None if verdict.score == float("inf") else verdict.score
                ),
                observed_count=verdict.baseline_size,
                evidence=[
                    f"latest={values[-1]}",
                    f"baseline_median={verdict.baseline_median}",
                    f"certified_commit={certified.code_commit_sha}",
                ][:EVIDENCE_LIMIT],
            )
        )
    if not outcomes:
        return [
            RuleOutcome(
                object_name,
                "pass",
                observed_count=clean,
                expected_count=clean,
                evidence=[f"certified_commit={certified.code_commit_sha}"],
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
