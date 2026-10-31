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

    def __init__(
        self,
        calls: list[tuple[str, object]],
        changed: set[str],
        silver_rows: bool = False,
    ) -> None:
        self.calls = calls
        self.changed = changed
        self.silver_rows = silver_rows
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
        elif sql is ACS_CHUNK_CONFIG.stateless_rows_sql:
            self._one = (0,)
        elif sql is ACS_CHUNK_CONFIG.state_list_sql:
            self._all = [("10",), ("55",)]
        elif sql is ACS_CHUNK_CONFIG.silver_rows_sql:
            self._one = (self.silver_rows,)
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
    def __init__(self, changed: set[str], silver_rows: bool = False) -> None:
        self.calls: list[tuple[str, object]] = []
        self.changed = changed
        self.silver_rows = silver_rows

    def get_conn(self):
        hook = self

        class _Conn:
            notices: list[str] = []

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

            def cursor(self):
                return _Cursor(hook.calls, hook.changed, hook.silver_rows)

            def commit(self):
                return None

        return _Conn()


def _served_grains(hook: _Hook) -> list[list[str]]:
    """Each grain step served, once, however many states it was split into."""
    grains: list[list[str]] = []
    for sql, params in hook.calls:
        if sql.startswith(f"CALL {ACS_CHUNK_CONFIG.report_procedure}"):
            if list(params[2]) not in grains:
                grains.append(list(params[2]))
    return grains


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


def test_a_year_that_serves_nothing_from_publishable_silver_fails() -> None:
    """Covers: DB-065 -- zero report rows over nonzero silver is a failure, not a checkpoint."""
    hook = _Hook(changed={"PLACE"}, silver_rows=True)
    with pytest.raises(RuntimeError, match="served no report rows"):
        refresh_serving_layer_in_year_chunks(hook=hook, config=ACS_CHUNK_CONFIG)
    assert any("status = 'FAILED'" in sql for sql, _ in hook.calls)
    assert not any("status = 'COMPLETE'" in sql for sql, _ in hook.calls)


def test_a_tract_or_place_step_is_published_state_by_state() -> None:
    """Covers: DB-065 -- one session per state, each naming its state; the area step stays whole."""
    hook = _Hook(changed={"PLACE", "TRACT", "COUNTY"})
    refresh_serving_layer_in_year_chunks(hook=hook, config=ACS_CHUNK_CONFIG)
    calls = [
        params
        for sql, params in hook.calls
        if sql.startswith(f"CALL {ACS_CHUNK_CONFIG.report_procedure}")
    ]
    area = [params for params in calls if "COUNTY" in params[2]]
    assert len(area) == 1 and len(area[0]) == 3
    for grain in ("TRACT", "PLACE"):
        states = sorted(params[3] for params in calls if list(params[2]) == [grain])
        assert states == ["10", "55"]
