"""What one request costs in statements, against a real warehouse.

Covers: API-147 -- within one request each relation is probed at most once and
        a composition resolves its measures in one statement.

The unit tier counts statements against a stand-in session, which proves the
memo. What only a real session can answer is whether the memo lives where this
code thinks it does: `Session.info` is SQLAlchemy's own per-session dictionary,
and a stub that happens to have an `info` attribute proves nothing about the
real one's lifetime.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from psycopg2.extensions import connection
from sqlalchemy import event

from apps.api import database as api_database
from data_ingestion_toolbox.config import Settings
from apps.api.services.contracts import require_relation
from apps.api.services.neutral_observations_service import (
    resolve_metric,
    resolve_metrics,
)
from data_ingestion_toolbox.sql.catalog_queries import METRIC_RELATION

pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.api]


@pytest.fixture
def recorded_session(
    bootstrapped_postgres: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[Any, list[str]]]:
    """A real API session, and every statement it sends.

    Recorded through SQLAlchemy's own `before_cursor_execute` rather than by
    wrapping `execute`: the point is to count what reaches the database, and a
    wrapper counts what reached the wrapper.
    """
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg2://"
        f"{bootstrapped_postgres.user}:{bootstrapped_postgres.password}"
        f"@{bootstrapped_postgres.host}:{bootstrapped_postgres.port}"
        f"/{bootstrapped_postgres.database}",
    )
    api_database.dispose_engine()
    engine = api_database.get_api_engine(Settings())
    statements: list[str] = []

    def _record(_conn, _cursor, statement, _parameters, _context, _many):
        statements.append(" ".join(str(statement).split()))

    event.listen(engine, "before_cursor_execute", _record)
    session = next(api_database.get_db_session())
    try:
        yield session, statements
    finally:
        event.remove(engine, "before_cursor_execute", _record)
        session.close()
        api_database.dispose_engine()


def _probes(statements: list[str]) -> list[str]:
    return [statement for statement in statements if "to_regclass" in statement]


def test_a_relation_is_probed_once_within_one_session(
    recorded_session: tuple[Any, list[str]],
) -> None:
    """Covers: API-147 — the memo lives on the real Session.info."""
    session, statements = recorded_session

    for _ in range(12):
        require_relation(session, METRIC_RELATION)

    assert len(_probes(statements)) == 1, _probes(statements)


def test_a_new_session_probes_again(
    recorded_session: tuple[Any, list[str]],
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: API-147 — the memo dies with the session it was made for.

    A relation that appears between requests has to be seen by the next one;
    this is the assertion that keeps the memo from becoming a cache.
    """
    session, statements = recorded_session
    require_relation(session, METRIC_RELATION)
    session.close()

    second = next(api_database.get_db_session())
    try:
        require_relation(second, METRIC_RELATION)
    finally:
        second.close()

    assert len(_probes(statements)) == 2


def test_a_composition_costs_one_metric_statement(
    recorded_session: tuple[Any, list[str]],
) -> None:
    """Covers: API-147 — eight measures, one read, whatever asks afterwards."""
    session, statements = recorded_session
    codes = [f"FRED:BUDGET_{index}" for index in range(8)]

    resolve_metrics(session, codes)
    for code in codes:
        resolve_metric(session, code)

    metric_reads = [
        statement
        for statement in statements
        if METRIC_RELATION in statement and "to_regclass" not in statement
    ]
    assert len(metric_reads) == 1, metric_reads
    assert "= ANY" in metric_reads[0]
    assert len(_probes(statements)) == 1
