from __future__ import annotations

import pytest

from data_ingestion_toolbox.fred.domain_coverage import (
    assert_configured_domain_coverage,
)

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, results: list[list[tuple[str]]]) -> None:
        self._results = iter(results)
        self._current: list[tuple[str]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args) -> None:
        return None

    def execute(self, sql: str, params: tuple) -> None:
        self._current = next(self._results)

    def fetchall(self) -> list[tuple[str]]:
        return self._current


class _Connection:
    def __init__(self, results: list[list[tuple[str]]]) -> None:
        self._cursor = _Cursor(results)

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *args) -> None:
        return None

    def cursor(self) -> _Cursor:
        return self._cursor


class _Hook:
    def __init__(self, results: list[list[tuple[str]]]) -> None:
        self._results = results

    def get_conn(self) -> _Connection:
        return _Connection(self._results)


def test_domain_coverage_accepts_every_series_in_both_layers() -> None:
    """Covers: ETL-015 — configured series exist in raw and gold layers."""
    series_by_domain = {
        "labor": ["PAYEMS"],
        "prices": ["CPIAUCSL"],
    }
    hook = _Hook(
        [
            [("PAYEMS",)],
            [("PAYEMS",)],
            [("CPIAUCSL",)],
            [("CPIAUCSL",)],
        ]
    )

    assert_configured_domain_coverage(hook, series_by_domain)


def test_domain_coverage_reports_missing_series_by_layer() -> None:
    """Covers: ETL-015 — missing series are reported by warehouse layer."""
    series_by_domain = {"labor": ["PAYEMS", "UNRATE"]}
    hook = _Hook(
        [
            [("PAYEMS",)],
            [("UNRATE",)],
        ]
    )

    with pytest.raises(ValueError) as exc_info:
        assert_configured_domain_coverage(hook, series_by_domain)

    assert "'silver': ['UNRATE']" in str(exc_info.value)
    assert "'gold': ['PAYEMS']" in str(exc_info.value)
