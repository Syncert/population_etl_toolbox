"""The forced full re-serve plan and its operator entry point.

Covers: ETL-049 — a change to what a served row *says* rather than what it is
worth moves no silver watermark, so the changed-year plan skips exactly the
years still carrying the old meaning. Before this, the only supported full
re-serve was a single unchunked procedure call under a 60-minute statement
timeout, which cannot finish a 68-million-row relation at all; operators were
left hand-driving a year loop with no checkpoint and no progress record.
"""

from __future__ import annotations

from datetime import date, datetime

import re

import pytest

from data_ingestion_toolbox.utility.gold_schema import (
    ServingRefreshChunkConfig,
    refresh_serving_layer_in_year_chunks,
)
from data_ingestion_toolbox.utility.serving_reserve import (
    ACS_CHUNK_CONFIG,
    BLS_CHUNK_CONFIG,
    FRED_CHUNK_CONFIG,
    FULL_RESERVE_CONFIGS,
)

pytestmark = pytest.mark.unit

CONFIGS = (BLS_CHUNK_CONFIG, ACS_CHUNK_CONFIG, FRED_CHUNK_CONFIG)


def test_every_union_served_source_declares_both_plans() -> None:
    """Covers: ETL-049 — a source that cannot be re-served in full is a gap."""
    assert set(FULL_RESERVE_CONFIGS) == {"BLS", "CENSUS_ACS", "FRED"}
    for config in CONFIGS:
        assert config.changed_chunks_sql.strip()
        assert config.all_chunks_sql.strip()


def test_the_forced_plan_carries_no_watermark_predicate() -> None:
    """Covers: ETL-049 — the whole point is to ignore the watermark.

    A stray ``ingested_at > %s`` would silently turn a full re-serve back into
    an incremental one, and the symptom — years keeping their old meaning — is
    exactly the bug this exists to fix.
    """
    for config in CONFIGS:
        assert "ingested_at > %s" in config.changed_chunks_sql
        assert "ingested_at > %s" not in config.all_chunks_sql
        # No bound parameters at all: the driver passes none.
        assert "%s" not in config.all_chunks_sql


def test_the_forced_plan_spans_the_served_relation_too() -> None:
    """Covers: ETL-049 — a year silver dropped still has served rows to delete."""
    for config in CONFIGS:
        assert config.report_table in config.all_chunks_sql
        assert "generate_series" in config.all_chunks_sql


#: The silver fact table each source's forced plan aggregates.
SILVER_FACTS = {
    "BLS": "silver_bls.fact_labor_statistics",
    "CENSUS_ACS": "silver_census.fact_demographics",
    "FRED": "silver_fred.fact_economic_indicators",
}


def test_the_forced_plan_reads_the_silver_fact_table_exactly_once() -> None:
    """Covers: ETL-049 — the per-year watermark is one aggregate, not N.

    The first implementation took each year's watermark from a correlated
    subquery, so the planner read the whole fact table once per calendar year.
    On Census ACS that is twenty passes over tens of millions of rows: the
    planning step alone had not returned after ten minutes, before a single row
    was re-served. The fix aggregates once and joins the result to the year
    series, and naming the table exactly once is what makes the difference
    visible to a test -- runtime is not assertable here, because the fixtures
    these tests can build are far too small to show it.
    """
    for source_code, config in FULL_RESERVE_CONFIGS.items():
        table = SILVER_FACTS[source_code]
        occurrences = config.all_chunks_sql.count(table)
        assert occurrences == 1, (
            f"{source_code}'s forced plan names {table} {occurrences} times; "
            "more than one means it scans the fact table per year rather than "
            "aggregating it once"
        )
        assert "GROUP BY" in config.all_chunks_sql


def test_a_forced_chunk_may_take_longer_than_an_incremental_one() -> None:
    """Covers: ETL-049 — a forced chunk rewrites the year, not the delta."""
    for config in CONFIGS:
        assert config.full_statement_timeout
        forced = int(re.match(r"(\d+)min", config.full_statement_timeout).group(1))
        incremental = int(re.match(r"(\d+)min", config.statement_timeout).group(1))
        assert forced >= incremental


def test_the_shared_configs_are_the_ones_the_ingest_dags_use() -> None:
    """Covers: ETL-049 — one definition, so the two callers cannot drift."""
    for source_code, config in FULL_RESERVE_CONFIGS.items():
        assert config.source_code == source_code
        assert config.report_procedure.startswith(config.report_table.split(".")[0])


# --- the driver's plan selection --------------------------------------------


class _Cursor:
    def __init__(self, recorder: list[str]) -> None:
        self.recorder = recorder

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, statement: str, parameters: object = None) -> None:
        self.recorder.append(statement)

    def fetchone(self) -> tuple[object, ...]:
        return (None,)

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _Conn:
    def __init__(self, recorder: list[str]) -> None:
        self.recorder = recorder
        #: What psycopg2 puts every `RAISE NOTICE` into. The chunk driver reads
        #: and clears it per chunk so the procedures' own reporting -- row
        #: counts, and the `cleared_partitions=` marker (DB-056, DB-058) --
        #: reaches the run's log instead of a list nobody reads. A stub without
        #: it is a stub that does not stand in for the thing.
        self.notices: list[str] = []

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self) -> _Cursor:
        return _Cursor(self.recorder)

    def commit(self) -> None:
        return None


class _Hook:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def get_conn(self) -> _Conn:
        return _Conn(self.statements)


def _config(**overrides: str) -> ServingRefreshChunkConfig:
    values = {
        "source_code": "FIXTURE",
        "log_label": "FIXTURE",
        "report_table": "gold_fixture.rpt",
        "report_date_column": "observation_date",
        "changed_chunks_sql": "SELECT 'changed-plan' WHERE ingested_at > %s",
        "report_procedure": "gold_fixture.refresh_rpt",
        "latest_procedure": "gold_fixture.refresh_latest",
        "statement_timeout": "10min",
        "all_chunks_sql": "SELECT 'full-plan'",
        "full_statement_timeout": "20min",
    }
    values.update(overrides)
    return ServingRefreshChunkConfig(**values)


def test_the_default_run_uses_the_changed_year_plan() -> None:
    """Covers: ETL-049 — an expensive re-serve never happens by accident."""
    hook = _Hook()
    refresh_serving_layer_in_year_chunks(hook=hook, config=_config())

    assert any("changed-plan" in statement for statement in hook.statements)
    assert not any("full-plan" in statement for statement in hook.statements)


def test_a_forced_run_uses_the_every_year_plan() -> None:
    """Covers: ETL-049 — the operator's request selects the other plan."""
    hook = _Hook()
    refresh_serving_layer_in_year_chunks(hook=hook, config=_config(), force_full=True)

    assert any("full-plan" in statement for statement in hook.statements)
    assert not any("changed-plan" in statement for statement in hook.statements)


def test_a_source_without_a_forced_plan_refuses_rather_than_silently_degrading() -> (
    None
):
    """Covers: ETL-049 — falling back to the changed plan would do nothing."""
    hook = _Hook()
    with pytest.raises(ValueError, match="cannot be re-served in full"):
        refresh_serving_layer_in_year_chunks(
            hook=hook, config=_config(all_chunks_sql=""), force_full=True
        )


# --- DB-048: statistics stay current across chunks ---------------------------


class _PlanningCursor(_Cursor):
    """A cursor that plans one chunk, so the chunk loop actually runs.

    The `_Cursor` above answers everything with `None` and no rows, which is
    enough for the plan-selection tests: the driver finds no chunks and
    returns. Proving what happens *inside* a chunk needs one.
    """

    def __init__(self, recorder: list[str]) -> None:
        super().__init__(recorder)
        self._last = ""

    def execute(self, statement: str, parameters: object = None) -> None:
        self._last = statement
        self.recorder.append(statement)

    def fetchall(self) -> list[tuple[object, ...]]:
        if "changed-plan" in self._last or "full-plan" in self._last:
            return [(date(2023, 1, 1), date(2023, 12, 31), datetime(2026, 3, 1))]
        return []

    def fetchone(self) -> tuple[object, ...]:
        if "RETURNING" in self._last:
            # target, completed, status, completed_at: a chunk that has never
            # run, so the loop does the work rather than skipping it.
            return (datetime(2026, 3, 1), None, "PENDING", None)
        if "COUNT(*)" in self._last:
            # Two different counts share this shape: the rows the chunk wrote,
            # and how many planned chunks are durably complete. The second one
            # guards the watermark, so answering both with the same number
            # makes the driver refuse.
            return (1,) if "serving_refresh_chunk_state" in self._last else (7,)
        return (None,)


class _PlanningConn(_Conn):
    def cursor(self) -> _PlanningCursor:
        return _PlanningCursor(self.recorder)


class _PlanningHook(_Hook):
    def get_conn(self) -> _PlanningConn:
        return _PlanningConn(self.statements)


def test_each_chunk_analyzes_what_it_rewrote() -> None:
    """Covers: DB-048 — the next chunk plans against current statistics.

    A chunk is delete-then-reinsert for one year. Nothing in that updates the
    planner's idea of the table, so by the second chunk of a twenty-year
    re-serve every plan is built from statistics describing rows that are gone.
    """
    hook = _PlanningHook()
    refresh_serving_layer_in_year_chunks(
        hook=hook, config=_config(latest_table="gold_fixture.latest")
    )

    analyzed = [statement for statement in hook.statements if "ANALYZE" in statement]
    assert "ANALYZE gold_fixture.rpt" in analyzed
    # The latest relation is rewritten by the same chunk and is what the
    # explorer reads, so it is analysed too.
    assert "ANALYZE gold_fixture.latest" in analyzed


def test_the_analyze_happens_after_the_chunk_is_checkpointed() -> None:
    """Covers: DB-048 — the durable checkpoint is never behind an optimisation."""
    hook = _PlanningHook()
    refresh_serving_layer_in_year_chunks(
        hook=hook, config=_config(latest_table="gold_fixture.latest")
    )

    checkpoint = next(
        index
        for index, statement in enumerate(hook.statements)
        if "status = 'COMPLETE'" in statement
    )
    first_analyze = next(
        index
        for index, statement in enumerate(hook.statements)
        if "ANALYZE" in statement
    )
    assert checkpoint < first_analyze


def test_a_source_with_no_latest_relation_analyzes_only_its_report() -> None:
    """Covers: DB-048 — PEP serves views, which have no statistics to refresh."""
    hook = _PlanningHook()
    refresh_serving_layer_in_year_chunks(hook=hook, config=_config(latest_table=""))

    analyzed = [statement for statement in hook.statements if "ANALYZE" in statement]
    assert analyzed == ["ANALYZE gold_fixture.rpt"]


def test_a_failed_analyze_does_not_undo_a_completed_chunk() -> None:
    """Covers: DB-048 — stale statistics are a slower plan, not a wrong answer.

    Failing the chunk over them would turn an optimisation into an outage, and
    the retry would redo work that was already committed.
    """

    class _RefusingCursor(_PlanningCursor):
        def execute(self, statement: str, parameters: object = None) -> None:
            super().execute(statement, parameters)
            if statement.startswith("ANALYZE"):
                raise RuntimeError("could not obtain a lock on the relation")

    class _RefusingConn(_Conn):
        def cursor(self) -> _RefusingCursor:
            return _RefusingCursor(self.recorder)

    class _RefusingHook(_Hook):
        def get_conn(self) -> _RefusingConn:
            return _RefusingConn(self.statements)

    hook = _RefusingHook()
    outcome = refresh_serving_layer_in_year_chunks(
        hook=hook, config=_config(latest_table="gold_fixture.latest")
    )

    assert outcome["completed"] == 1
    assert any("status = 'COMPLETE'" in statement for statement in hook.statements)
    # And nothing marked the chunk failed on the way out.
    assert not any("status = 'FAILED'" in statement for statement in hook.statements)
