"""The forced full re-serve plan and its operator entry point.

Covers: ETL-049 — a change to what a served row *says* rather than what it is
worth moves no silver watermark, so the changed-year plan skips exactly the
years still carrying the old meaning. Before this, the only supported full
re-serve was a single unchunked procedure call under a 60-minute statement
timeout, which cannot finish a 68-million-row relation at all; operators were
left hand-driving a year loop with no checkpoint and no progress record.
"""

from __future__ import annotations

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
