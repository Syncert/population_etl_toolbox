"""The chunk driver skips a grain step silver did not change (skip-unchanged-grains).

Covers: DB-065
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from data_ingestion_toolbox.utility.gold_schema import (
    refresh_serving_layer_in_year_chunks,
)
from data_ingestion_toolbox.utility.serving_reserve import ACS_CHUNK_CONFIG

pytestmark = pytest.mark.unit

SERVED = datetime(2026, 10, 27, tzinfo=timezone.utc)


class _Cursor:
    """Answers the driver's bookkeeping as a warehouse with one served year would."""

    def __init__(self, calls: list[tuple[str, object]], changed: set[str]) -> None:
        self.calls = calls
        self.changed = changed
        self._one: tuple | None = None
        self._all: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.calls.append((sql, params))
        self._one, self._all = (None,), []
        if "INSERT INTO control.serving_refresh_state" in sql:
            self._one = (SERVED,)
        elif "RETURNING" in sql and "serving_refresh_chunk_state" in sql:
            # A year served before, with newer silver rows to serve.
            last = SERVED.replace(year=2025)
            self._one = (SERVED, last, "PENDING", last)
        elif sql in (
            ACS_CHUNK_CONFIG.changed_chunks_sql,
            ACS_CHUNK_CONFIG.all_chunks_sql,
        ):
            self._all = [(date(2021, 1, 1), date(2021, 12, 31), SERVED)]
        elif sql is ACS_CHUNK_CONFIG.step_changed_sql:
            levels = set(params["geo_levels"])
            self._one = (bool(levels & self.changed),)
        elif "UNNEST(%s::DATE[]" in sql:
            # The one planned chunk completed.
            self._one = (1,)
        elif "SELECT COUNT(*)" in sql:
            self._one = (0,)
        elif "clock_timestamp()" in sql or "now()" in sql.lower():
            self._one = (SERVED,)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _Hook:
    def __init__(self, changed: set[str]) -> None:
        self.calls: list[tuple[str, object]] = []
        self.changed = changed

    def get_conn(self):
        hook = self

        class _Conn:
            notices: list[str] = []

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

            def cursor(self):
                return _Cursor(hook.calls, hook.changed)

            def commit(self):
                return None

        return _Conn()


def _served_grains(hook: _Hook) -> list[list[str]]:
    return [
        list(params[2])
        for sql, params in hook.calls
        if sql.startswith(f"CALL {ACS_CHUNK_CONFIG.report_procedure}")
    ]


def test_a_places_only_year_serves_the_place_step_alone() -> None:
    """Covers: DB-065 -- area and tract are skipped; place is served."""
    hook = _Hook(changed={"PLACE"})
    refresh_serving_layer_in_year_chunks(hook=hook, config=ACS_CHUNK_CONFIG)
    assert _served_grains(hook) == [["PLACE"]]


def test_a_forced_re_serve_never_asks_and_serves_every_step() -> None:
    """Covers: DB-065 -- a forced run exists because nothing moved."""
    hook = _Hook(changed=set())
    refresh_serving_layer_in_year_chunks(
        hook=hook, config=ACS_CHUNK_CONFIG, force_full=True
    )
    assert not any(sql is ACS_CHUNK_CONFIG.step_changed_sql for sql, _ in hook.calls)
    assert len(_served_grains(hook)) == len(ACS_CHUNK_CONFIG.grain_steps)
