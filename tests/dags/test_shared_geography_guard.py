"""Every source DAG waits on the same predicate.

Covers: DAG-020 -- each of the six ingestion DAGs' guard task calls the shared
        helper, and no DAG file carries a geography predicate of its own.

The three sources added later asked `to_regclass('silver_ref.dim_geo_entity')`
-- whether the table exists -- and the bootstrap manifest creates it, empty,
before any source runs. This tier is where "the DAG calls the helper" can
actually be checked: the callables are reached through the DagBag.
"""

from __future__ import annotations

import io
import re
import tokenize
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.dag

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "dags"

#: Each ingestion DAG and the task that guards it.
GUARDED = {
    "acs_ingest": "require_shared_geography",
    "bls_ingest": "require_shared_geography",
    "census_pep_ingest": "validate_geography_prerequisites",
    "cdc_ingest": "require_shared_geography",
    "fbi_ucr_ingest": "require_shared_geography",
    "usda_nass_crop_ingest": "require_shared_geography",
}


class _Connection:
    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_exception: object) -> None:
        return None

    def cursor(self) -> Any:  # pragma: no cover - the helper is patched
        raise AssertionError("the guard queried the database instead of the helper")


class _Hook:
    def get_conn(self) -> _Connection:
        return _Connection()


@pytest.mark.dag
@pytest.mark.parametrize("dag_id", sorted(GUARDED))
def test_each_ingestion_dag_guards_through_the_shared_helper(
    dagbag: Any, monkeypatch: pytest.MonkeyPatch, dag_id: str
) -> None:
    """Covers: DAG-020 — six DAGs, one predicate."""
    dag = dagbag.dags[dag_id]
    task = dag.get_task(GUARDED[dag_id])

    calls: list[dict[str, Any]] = []

    def _record(connection: Any, **keywords: Any) -> dict[str, int]:
        calls.append(keywords)
        return {"nation": 1, "state": 56, "county": 3144, "place": 19500}

    # The callable's own globals, not a module re-imported by name. Airflow's
    # DagBag loads each DAG file under a synthetic module name, so
    # `__import__(callable.__module__)` can hand back a *different* module
    # object than the one the function will actually read from -- which showed
    # up as six tests passing alone and failing in the full tier, against a
    # database connection that was never meant to be reached.
    scope = task.python_callable.__globals__
    monkeypatch.setitem(scope, "require_shared_geography_loaded", _record)
    monkeypatch.setitem(scope, "_get_postgres_hook", lambda *_args, **_kw: _Hook())

    task.python_callable()

    assert len(calls) == 1, f"{dag_id} did not reach the shared helper"
    # Census PEP serves place-level estimates and is the one source that adds
    # a grain. Everything else asks the shared question unchanged.
    if dag_id == "census_pep_ingest":
        assert calls[0]["additional_minimums"] == {"place": 18000}
    else:
        assert calls[0] == {}


@pytest.mark.dag
def test_no_dag_file_carries_a_geography_predicate_of_its_own() -> None:
    """Covers: DAG-020 — one copy of the thresholds, not seven.

    Comments and docstrings are tokenized away rather than filtered by eye:
    three of these files explain in a docstring what the old `to_regclass`
    guard did and why it was wrong, and deleting that explanation to satisfy a
    grep would delete the record of a shipped defect. So this reads the code
    and only the code.
    """
    offences: list[str] = []
    for path in sorted(DAGS_FOLDER.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        code_only: list[str] = []
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type in (tokenize.COMMENT, tokenize.STRING):
                continue
            code_only.append(f"{token.start[0]}:{token.string}")
        code = " ".join(code_only)
        # The SQL these guards used lived in string literals, which are gone
        # with the docstrings above -- so what is left to catch is the call
        # and the comparisons.
        for pattern, what in (
            (r"to_regclass", "a to_regclass existence check"),
            (r"\bcounties\b\s*:?\s*<\s*3000", "its own county threshold"),
            (r"\bstates\b\s*:?\s*<\s*50", "its own state threshold"),
            (r"\bnation\b\s*:?\s*!=\s*1", "its own nation threshold"),
        ):
            if re.search(pattern, code):
                offences.append(f"{path.name}: {what}")

    assert not offences, (
        "a DAG carries its own geography predicate; the thresholds live in "
        f"silver_ref.geography_guard: {offences}"
    )


@pytest.mark.dag
def test_the_guard_gates_the_work_that_needs_a_geography(dagbag: Any) -> None:
    """Covers: DAG-020 — a guard downstream of the ingest guards nothing.

    Not "the guard has no upstream task": `acs_ingest` syncs its dataset list
    first, and that reads the provider's catalog rather than resolving a
    geography. What matters is that nothing which *resolves* a geography runs
    before it.
    """
    resolving = ("ingest", "capture", "replay", "publish", "transform", "load")
    for dag_id, task_id in sorted(GUARDED.items()):
        dag = dagbag.dags[dag_id]
        guard = dag.get_task(task_id)

        assert guard.downstream_task_ids, f"{dag_id}.{task_id} gates nothing"
        for upstream in sorted(guard.upstream_task_ids):
            assert not any(word in upstream.lower() for word in resolving), (
                f"{dag_id} runs {upstream} before its geography guard, so rows "
                "are resolved against a reference nobody has checked"
            )
