"""A BLS daily-quota stop resumes at the next 1:00am US/Eastern.

Covers: DAG-022

The pause used to write a computed delay onto ``ti.task.retry_delay`` inside
the worker and re-raise for an ordinary retry. Airflow's scheduler decides when
a retry runs from its own copy of the task (``TaskInstance.next_retry_datetime``
reads ``self.task.retry_delay`` from the serialized DAG), so the worker's write
never reached it and every pause waited the static 23-hour fallback: on
OFFICE_LAB the 2026-11-05 LAUS backfill batch stopped at 00:32 UTC and was due
back at 23:32 UTC instead of 06:00 UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.dag

airflow_exceptions = pytest.importorskip("airflow.exceptions")
AirflowRescheduleException = airflow_exceptions.AirflowRescheduleException

_EASTERN = ZoneInfo("America/New_York")


def _callable(dagbag, task_id: str):
    return dagbag.dags["bls_ingest"].get_task(task_id).python_callable


@pytest.mark.parametrize(
    ("stopped_utc", "resume_utc"),
    [
        # The live case: stopped 00:32 UTC (19:32 EST on 11-04), resume 06:00 UTC.
        (
            datetime(2026, 11, 5, 0, 32, tzinfo=timezone.utc),
            datetime(2026, 11, 5, 6, 0, tzinfo=timezone.utc),
        ),
        # Stopped after 01:00 ET: the same Eastern day's 01:00 has passed, so tomorrow.
        (
            datetime(2026, 11, 5, 14, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc),
        ),
        # Summer time (EDT, UTC-4).
        (
            datetime(2026, 7, 1, 16, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 2, 5, 0, tzinfo=timezone.utc),
        ),
        # Across the autumn change (EDT on 10-31, EST from 02:00 on 11-01).
        (
            datetime(2026, 10, 31, 20, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 1, 5, 0, tzinfo=timezone.utc),
        ),
        # Across the spring change (EST on 03-07, EDT from 02:00 on 03-08).
        (
            datetime(2026, 3, 7, 20, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 8, 6, 0, tzinfo=timezone.utc),
        ),
    ],
)
def test_resume_time_is_the_next_one_am_eastern(
    dagbag, stopped_utc: datetime, resume_utc: datetime
) -> None:
    """Covers: DAG-022 — the resume is 01:00 Eastern, in real elapsed time."""
    module_globals = _callable(dagbag, "ingest_batch").__globals__
    resume = module_globals["_next_quota_resume_utc"](stopped_utc)
    assert resume == resume_utc
    assert resume.astimezone(_EASTERN).hour == 1
    assert (
        module_globals["_compute_delay_until_1am_est"](stopped_utc)
        == resume_utc - stopped_utc
    )


def test_quota_stop_reschedules_the_batch_for_one_am_eastern(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-022 — a quota stop raises a reschedule carrying the resume time."""
    ingest_batch = _callable(dagbag, "ingest_batch")
    module_globals = ingest_batch.__globals__
    threshold = module_globals["BlsDailyThresholdExceeded"]
    ran: list[dict[str, Any]] = []

    def stop_on_second(work_unit: dict[str, Any]) -> int:
        ran.append(work_unit)
        if len(ran) == 2:
            raise threshold("REQUEST_NOT_PROCESSED")
        return 5

    monkeypatch.setitem(module_globals, "_run_one_work_unit", stop_on_second)
    batch = [
        {"program": "la", "start_year": 1990 + i, "end_year": 1990 + i}
        for i in range(3)
    ]

    before = datetime.now(timezone.utc)
    with pytest.raises(AirflowRescheduleException) as raised:
        ingest_batch(batch)
    after = datetime.now(timezone.utc)

    assert len(ran) == 2, "the batch stops at the slice the quota refused"
    resume = raised.value.reschedule_date
    assert resume.tzinfo is not None
    assert resume.astimezone(_EASTERN).hour == 1
    assert resume.astimezone(_EASTERN).minute == 0
    assert before < resume <= after + timedelta(hours=25)
    assert isinstance(raised.value.__cause__, threshold)


def test_resumed_batch_skips_only_slices_finished_in_this_run(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-022 — a resume skips this run's finished slices, not older ones.

    A reschedule keeps the try number, so the old ``try_number > 1`` guard never
    fired after a pause and a resumed batch spent quota re-fetching slices it had
    already loaded. A forced rolling-window slice that is ``success`` from an
    earlier run must still be fetched.
    """
    ingest_batch = _callable(dagbag, "ingest_batch")
    module_globals = ingest_batch.__globals__
    run_started = datetime(2026, 11, 5, 0, 21, tzinfo=timezone.utc)

    class _DagRun:
        start_date = run_started

    monkeypatch.setitem(
        module_globals, "get_current_context", lambda: {"dag_run": _DagRun()}
    )
    finished_at = {
        1990: run_started + timedelta(minutes=5),  # done earlier in this run: skip
        2024: run_started - timedelta(days=30),  # done last month, forced now: fetch
        2025: None,  # still planned: fetch
    }
    seen_since: list[datetime | None] = []

    def done(work_unit: dict[str, Any], since: datetime | None = None) -> bool:
        seen_since.append(since)
        when = finished_at[work_unit["start_year"]]
        return when is not None and since is not None and when >= since

    fetched: list[int] = []
    monkeypatch.setitem(module_globals, "_is_work_unit_done_for_current_hash", done)
    monkeypatch.setitem(
        module_globals,
        "_run_one_work_unit",
        lambda w: fetched.append(w["start_year"]) or 1,
    )

    total = ingest_batch(
        [{"program": "la", "start_year": y, "end_year": y} for y in (1990, 2024, 2025)]
    )

    assert fetched == [2024, 2025]
    assert total == 2
    assert seen_since == [run_started] * 3


def test_done_check_counts_only_slices_finished_since_the_run_started(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: DAG-022 — the ledger read compares ``finished_at`` with the run start."""
    module_globals = _callable(dagbag, "ingest_batch").__globals__
    check = module_globals["_is_work_unit_done_for_current_hash"]
    since = datetime(2026, 11, 5, 0, 21, tzinfo=timezone.utc)
    rows: list[tuple[Any, ...]] = []

    class _Db:
        def get_conn(self):
            return self

        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, sql: str, params: Any = None) -> None:
            assert "finished_at" in sql

        def fetchone(self):
            return rows.pop(0) if rows else None

    monkeypatch.setitem(module_globals, "_get_postgres_hook", lambda: _Db())
    unit = {"program": "la", "start_year": 1990, "end_year": 1990, "series_hash": "h"}

    rows.append(("success", "h", since + timedelta(seconds=1)))
    assert check(unit, since=since) is True
    rows.append(("success", "h", since - timedelta(days=1)))
    assert check(unit, since=since) is False
    rows.append(("success", "h", since - timedelta(days=1)))
    assert check(unit) is True, (
        "without a run start, any success under this hash counts"
    )
    rows.append(("success", "other", since + timedelta(seconds=1)))
    assert check(unit, since=since) is False
    rows.append(("planned", "h", None))
    assert check(unit, since=since) is False


def test_ingest_batch_is_mapped_so_the_scheduler_honours_the_reschedule(dagbag) -> None:
    """Covers: DAG-022 — Airflow 2.9 enforces a non-sensor's reschedule only when mapped.

    ``ReadyToRescheduleDep`` passes an unmapped task that is not a sensor in
    reschedule mode straight through, which would rerun a paused batch at once.
    ``ingest_batch`` is expanded over the plan, so the dependency reads the
    ``task_reschedule`` row and waits.
    """
    from airflow.models.mappedoperator import MappedOperator

    task = dagbag.dags["bls_ingest"].get_task("ingest_batch")
    assert isinstance(task, MappedOperator)
    assert task.partial_kwargs.get("retry_delay", task.retry_delay) <= timedelta(
        hours=1
    ), "real failures retry promptly; quota pauses no longer ride on retry_delay"
