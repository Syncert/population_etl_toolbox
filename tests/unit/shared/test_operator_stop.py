"""An operator's stop fails a task outright; any other error keeps its retries."""

from __future__ import annotations

import sys
import types

import psycopg2.errors
import pytest

from data_ingestion_toolbox.utility.operator_stop import (
    fail_without_retry_when_stopped,
    stopped_by_operator,
)

pytestmark = pytest.mark.unit


class _FailNow(Exception):
    """Stands in for AirflowFailException, so this tier needs no Airflow."""


@pytest.fixture
def airflow_exceptions(monkeypatch: pytest.MonkeyPatch) -> type[Exception]:
    module = types.ModuleType("airflow.exceptions")
    module.AirflowFailException = _FailNow  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.exceptions", module)
    return _FailNow


@pytest.mark.parametrize(
    "stop",
    [
        psycopg2.errors.QueryCanceled("canceling statement due to user request"),
        psycopg2.errors.AdminShutdown(
            "terminating connection due to administrator command"
        ),
    ],
)
def test_an_operator_stop_becomes_a_failure_without_retry(
    airflow_exceptions: type[Exception], stop: Exception
) -> None:
    """Covers: DAG-008 — the stop is re-raised as AirflowFailException with its cause."""
    with (
        pytest.raises(airflow_exceptions) as raised,
        fail_without_retry_when_stopped(
            "the BLS serving refresh", "Clear this task to resume."
        ),
    ):
        raise RuntimeError("refresh failed") from stop
    assert str(raised.value) == (
        "An operator stopped the BLS serving refresh; not retrying. "
        "Clear this task to resume."
    )
    assert stopped_by_operator(raised.value.__cause__)


@pytest.mark.parametrize(
    "error",
    [
        psycopg2.errors.QueryCanceled("canceling statement due to statement timeout"),
        RuntimeError("Docker froze"),
    ],
)
def test_any_other_error_passes_through_and_keeps_its_retries(
    airflow_exceptions: type[Exception], error: Exception
) -> None:
    """Covers: DAG-008 — a statement timeout or a fault is not an operator's stop."""
    with (
        pytest.raises(type(error)) as raised,
        fail_without_retry_when_stopped("the ACS serving refresh", "Clear it."),
    ):
        raise error
    assert raised.value is error


def test_a_block_that_finishes_returns_its_value() -> None:
    """Covers: DAG-008 — the guard changes nothing when nothing is stopped."""

    def refresh() -> dict[str, int]:
        with fail_without_retry_when_stopped("the FRED serving refresh", "Clear it."):
            return {"chunks": 2}

    assert refresh() == {"chunks": 2}
