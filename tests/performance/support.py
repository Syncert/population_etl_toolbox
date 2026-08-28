"""Small deterministic helpers for checking versioned performance budgets."""

from __future__ import annotations

import json
from pathlib import Path

BASELINES = json.loads(
    (Path(__file__).with_name("baselines.json")).read_text(encoding="utf-8")
)


def percentile(values: list[float], fraction: float) -> float:
    """Return the nearest-rank percentile from a non-empty sample."""
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, int(len(ordered) * fraction) - 1))]


def locust_exit_code(request_count: int, fail_ratio: float) -> int:
    """Return failure when a Locust run has no evidence or reaches 1% errors."""
    return int(request_count <= 0 or fail_ratio >= 0.01)
