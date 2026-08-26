"""Deterministic performance-runner policy tests."""

from __future__ import annotations

import pytest

from tests.performance.support import locust_exit_code

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("request_count", "fail_ratio", "expected"),
    [
        (1000, 0.0, 0),
        (1000, 0.009, 0),
        (1000, 0.01, 1),
        (1000, 0.02, 1),
        (0, 0.0, 1),
    ],
)
def test_locust_exit_code_enforces_the_error_budget(
    request_count: int,
    fail_ratio: float,
    expected: int,
) -> None:
    """Covers: PERF-003 — controlled load requires evidence below 1% errors."""
    assert locust_exit_code(request_count, fail_ratio) == expected
