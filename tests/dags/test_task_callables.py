"""Controlled production DAG callable and ledger-boundary tests."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any

import pytest

pytestmark = pytest.mark.dag


@dataclass
class RecordingDatabase:
    """Minimal PostgresHook/connection/cursor surface with scripted reads."""

    fetches: list[list[tuple[Any, ...]]] = field(default_factory=list)
    executions: list[tuple[str, tuple[Any, ...] | None]] = field(default_factory=list)
    commits: int = 0

    def get_conn(self) -> "RecordingDatabase":
        return self

    def cursor(self) -> "RecordingDatabase":
        return self

    def __enter__(self) -> "RecordingDatabase":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.executions.append((sql, params))
        normalized = " ".join(sql.lower().split())
        self.rowcount = 0 if normalized.startswith("update") else 1

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.fetches.pop(0) if self.fetches else []

    def fetchone(self) -> tuple[Any, ...] | None:
        rows = self.fetchall()
        return rows[0] if rows else None

    def commit(self) -> None:
        self.commits += 1


def _callable(dagbag, dag_id: str, task_id: str):
    # Access the already-parsed DAG directly. ``DagBag.get_dag`` consults
    # Airflow's metadata database to decide whether a serialized DAG should be
    # refreshed, which is unrelated to callable-boundary tests and requires a
    # migrated metadata schema.
    return dagbag.dags[dag_id].get_task(task_id).python_callable


def _patch_callable_global(
    monkeypatch: pytest.MonkeyPatch, callable_, name: str, value: Any
) -> None:
    """Patch the globals used by a DagBag-loaded callable.

    Airflow imports DAG files under generated module names, so importing the
    repository module by its package name creates a second module object. The
    callable's own globals are the authoritative production boundary.
    """
    monkeypatch.setitem(callable_.__globals__, name, value)


@pytest.mark.parametrize(
    ("module_name", "work_unit", "expected_kwargs"),
    [
        (
            "dags.acs_ingest_dag",
            {
                "dataset": "acs5",
                "year": 2024,
                "geo_level": "county",
                "state_fips": "55",
                "variables_hash": "acs-hash",
                "variables_count": 3,
            },
            {
                "year": 2024,
                "dataset": "acs5",
                "geo_level": "county",
                "state_fips": "55",
            },
        ),
        (
            "dags.bls_ingest_dag",
            {
                "program": "la",
                "start_year": 2023,
                "end_year": 2024,
                "geo_level": "county",
                "state_fips": "55",
                "series_hash": "bls-hash",
                "series_count": 7,
            },
            {
                "program": "la",
                "start_year": 2023,
                "end_year": 2024,
                "geo_level": "county",
                "state_fips": "55",
            },
        ),
        (
            "dags.fred_ingest_dag",
            {
                "domain": "macro",
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "series_hash": "fred-hash",
                "series_count": 5,
            },
            {
                "domain": "macro",
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
            },
        ),
    ],
)
@pytest.mark.parametrize(
    ("rows_loaded", "final_status"), [(4, "success"), (0, "empty")]
)
def test_source_work_unit_forwards_runtime_parameters_and_commits_ledger_state(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    work_unit: dict[str, Any],
    expected_kwargs: dict[str, Any],
    rows_loaded: int,
    final_status: str,
) -> None:
    """Covers: DAG-010, DAG-014 — source boundaries persist running/final states."""
    module = importlib.import_module(module_name)
    database = RecordingDatabase()
    forwarded: list[dict[str, Any]] = []
    monkeypatch.setattr(module, "_get_postgres_hook", lambda: database)

    def fake_ingest_slice(**kwargs: Any) -> int:
        forwarded.append(kwargs)
        return rows_loaded

    monkeypatch.setattr(module, "ingest_slice", fake_ingest_slice)

    assert module._run_one_work_unit(work_unit) == rows_loaded
    assert forwarded == [expected_kwargs]
    assert database.commits == 2
    sql = "\n".join(statement for statement, _ in database.executions).lower()
    assert "status = 'running'" in sql
    final_params = database.executions[-1][1]
    assert final_params is not None
    assert final_params[0] == final_status
    assert final_params[1] == rows_loaded


@pytest.mark.parametrize(
    ("module_name", "work_unit", "error_factory", "expected_status"),
    [
        (
            "dags.acs_ingest_dag",
            {"dataset": "acs5", "year": 2024, "geo_level": "state"},
            lambda module: RuntimeError(
                "postgresql://user:db-secret@db/test api_key=source-secret"
            ),
            "failed",
        ),
        (
            "dags.bls_ingest_dag",
            {"program": "ce", "start_year": 2024, "end_year": 2024},
            lambda module: module.BlsRetryableHTTP(
                "registrationkey=source-secret upstream 503"
            ),
            "planned",
        ),
        (
            "dags.fred_ingest_dag",
            {
                "domain": "macro",
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
            },
            lambda module: RuntimeError("token=source-secret database unavailable"),
            "failed",
        ),
    ],
)
def test_source_work_unit_failure_is_retried_or_failed_with_sanitized_ledger(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    work_unit: dict[str, Any],
    error_factory,
    expected_status: str,
) -> None:
    """Covers: DAG-014, RES-001 — task-visible failures keep safe ledger context."""
    module = importlib.import_module(module_name)
    database = RecordingDatabase()
    error = error_factory(module)
    monkeypatch.setattr(module, "_get_postgres_hook", lambda: database)
    monkeypatch.setattr(
        module,
        "ingest_slice",
        lambda **kwargs: (_ for _ in ()).throw(error),
    )

    with pytest.raises(type(error)):
        module._run_one_work_unit(work_unit)

    final_sql, final_params = database.executions[-1]
    assert f"status = '{expected_status}'" in final_sql.lower()
    persisted = " ".join(str(value) for value in final_params or ())
    assert "source-secret" not in persisted
    assert "db-secret" not in persisted
    assert "postgresql://" not in persisted
    assert "***" in persisted or "database unavailable" in persisted


@pytest.mark.parametrize(
    ("dag_id", "module_name"),
    [
        ("acs_ingest", "dags.acs_ingest_dag"),
        ("bls_ingest", "dags.bls_ingest_dag"),
        ("fred_ingest", "dags.fred_ingest_dag"),
    ],
)
def test_mapped_ingest_task_calls_each_production_work_unit_once(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    dag_id: str,
    module_name: str,
) -> None:
    """Covers: DAG-010 — mapped runtime forwards every planned source slice."""
    work_units = [{"id": 1}, {"id": 2}, {"id": 3}]
    seen: list[dict[str, int]] = []
    callable_ = _callable(dagbag, dag_id, "ingest_batch")
    _patch_callable_global(
        monkeypatch,
        callable_,
        "_run_one_work_unit",
        lambda work_unit: seen.append(work_unit) or work_unit["id"],
    )
    if dag_id == "bls_ingest":
        _patch_callable_global(
            monkeypatch, callable_, "get_current_context", lambda: {}
        )

    result = callable_(work_units)

    assert result == 6
    assert seen == work_units


@pytest.mark.parametrize(
    ("dag_id", "module_name", "task_id", "target_name", "arguments"),
    [
        (
            "acs_ingest",
            "dags.acs_ingest_dag",
            "transform_to_silver",
            "transform_census_to_silver",
            (),
        ),
        (
            "bls_ingest",
            "dags.bls_ingest_dag",
            "transform_to_silver_by_program",
            "transform_bls_to_silver",
            ("la",),
        ),
        (
            "fred_ingest",
            "dags.fred_ingest_dag",
            "transform_to_silver_by_domain",
            "transform_fred_to_silver",
            ("macro",),
        ),
    ],
)
def test_silver_task_callable_forwards_declared_source_scope(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    dag_id: str,
    module_name: str,
    task_id: str,
    target_name: str,
    arguments: tuple[str, ...],
) -> None:
    """Covers: DAG-010 — silver task callables invoke production transforms."""
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def target(*args: Any, **kwargs: Any) -> int:
        calls.append((args, kwargs))
        return 17

    callable_ = _callable(dagbag, dag_id, task_id)
    _patch_callable_global(monkeypatch, callable_, target_name, target)

    assert callable_(*arguments) == 17
    if dag_id == "acs_ingest":
        assert calls == [((), {})]
    elif dag_id == "bls_ingest":
        assert calls == [((), {"program": "la"})]
    else:
        assert calls == [((), {"domain": "macro"})]


@pytest.mark.parametrize(
    (
        "dag_id",
        "module_name",
        "task_id",
        "expected_source",
        "report_proc",
        "latest_proc",
    ),
    [
        (
            "acs_ingest",
            "dags.acs_ingest_dag",
            "refresh_gold_census_serving_layer",
            "CENSUS_ACS",
            "gold_census.refresh_rpt_acs_observations",
            "gold_census.refresh_mv_acs_latest",
        ),
        (
            "bls_ingest",
            "dags.bls_ingest_dag",
            "refresh_gold_bls_serving_layer",
            "BLS",
            "gold_bls.refresh_rpt_bls_observations",
            "gold_bls.refresh_mv_bls_latest",
        ),
        (
            "fred_ingest",
            "dags.fred_ingest_dag",
            "refresh_gold_fred_serving_layer",
            "FRED",
            "gold_fred.refresh_rpt_fred_observations",
            "gold_fred.refresh_mv_fred_latest",
        ),
    ],
)
def test_gold_serving_task_forwards_production_chunk_configuration(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    dag_id: str,
    module_name: str,
    task_id: str,
    expected_source: str,
    report_proc: str,
    latest_proc: str,
) -> None:
    """Covers: DAG-010, ETL-037 — gold tasks pass exact production checkpoints."""
    database = RecordingDatabase()
    captured: list[Any] = []
    callable_ = _callable(dagbag, dag_id, task_id)
    _patch_callable_global(
        monkeypatch, callable_, "_get_postgres_hook", lambda: database
    )

    def refresh(**kwargs: Any) -> dict[str, int]:
        captured.append(kwargs["config"])
        assert kwargs["hook"] is database
        return {"chunks": 2, "rows": 11}

    _patch_callable_global(
        monkeypatch, callable_, "refresh_serving_layer_in_year_chunks", refresh
    )

    assert callable_() == {"chunks": 2, "rows": 11}
    assert len(captured) == 1
    assert captured[0].source_code == expected_source
    assert captured[0].report_procedure == report_proc
    assert captured[0].latest_procedure == latest_proc


def test_reference_dimension_task_callables_forward_declared_windows(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-009 — reference task runtime uses production dimension loaders."""
    calls: list[tuple[str, dict[str, Any]]] = []
    geo_callable = _callable(dagbag, "silver_ref", "load_dim_geo")
    time_callable = _callable(dagbag, "silver_ref", "load_dim_time")
    _patch_callable_global(
        monkeypatch,
        geo_callable,
        "sync_geo_dim",
        lambda **kwargs: calls.append(("geo", kwargs)) or 4,
    )
    _patch_callable_global(
        monkeypatch,
        time_callable,
        "sync_time_dim",
        lambda **kwargs: calls.append(("time", kwargs)) or 5,
    )

    assert geo_callable() == 4
    assert time_callable() == 5
    assert calls[0] == ("geo", {"source_year": None, "min_year": 2010})
    assert calls[1][0] == "time"
    assert calls[1][1]["start_date"].isoformat() == "1970-01-01"
    assert calls[1][1]["end_date"] is None


@pytest.mark.parametrize(
    ("dag_id", "module_name", "programs", "fingerprint_name"),
    [
        ("acs_ingest", "dags.acs_ingest_dag", ["acs5"], "_variables_fingerprint"),
        ("bls_ingest", "dags.bls_ingest_dag", ["ce"], "_series_fingerprint"),
        ("fred_ingest", "dags.fred_ingest_dag", ["macro"], "_series_fingerprint"),
    ],
)
def test_planning_task_builds_historical_and_rolling_or_geography_scopes(
    dagbag,
    monkeypatch: pytest.MonkeyPatch,
    dag_id: str,
    module_name: str,
    programs: list[str],
    fingerprint_name: str,
) -> None:
    """Covers: DAG-010 — production planners build source-specific work scopes."""
    database = RecordingDatabase(fetches=[[], []])
    callable_ = _callable(dagbag, dag_id, "build_ingestion_plan")
    _patch_callable_global(
        monkeypatch, callable_, "_get_postgres_hook", lambda: database
    )
    _patch_callable_global(
        monkeypatch, callable_, fingerprint_name, lambda *args: ("stable-hash", 3)
    )

    planning_input: Any
    if dag_id == "acs_ingest":
        _patch_callable_global(
            monkeypatch,
            callable_,
            "sync_variable_metadata_for_year",
            lambda year, dataset: None,
        )
        planning_input = [{"dataset": "acs5", "year": 2024}]
    elif dag_id == "fred_ingest":
        _patch_callable_global(
            monkeypatch,
            callable_,
            "_configured_series_by_domain",
            lambda: {"macro": []},
        )
        planning_input = 1
    else:
        planning_input = programs

    batches = callable_(planning_input)
    work_units = [work_unit for batch in batches for work_unit in batch]

    assert work_units
    assert all(
        work_unit.get("series_hash", work_unit.get("variables_hash")) == "stable-hash"
        for work_unit in work_units
    )
    if dag_id == "acs_ingest":
        assert {work_unit["geo_level"] for work_unit in work_units} == {
            "us",
            "state",
            "county",
        }
        assert len(work_units) == 54
    else:
        starts = {
            work_unit.get("start_year", work_unit.get("date_start"))
            for work_unit in work_units
        }
        assert len(starts) == 2
