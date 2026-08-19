"""Unit contracts for provider-neutral glossary orchestration helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from data_ingestion_toolbox.glossary import harvest

pytestmark = pytest.mark.unit


class Cursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed: list[tuple[Any, Any]] = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, statement: Any, parameters: Any = None) -> None:
        self.executed.append((statement, parameters))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Connection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.cursor_instance = Cursor(rows)
        self.closed = False

    def cursor(self) -> Cursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_discover_publishers_requires_the_complete_contract() -> None:
    """Covers: ARC-001 — discovery accepts only complete shared contracts."""
    complete = tuple(harvest.REQUIRED_COLUMNS)
    connection = Connection(
        [("gold_complete", complete), ("gold_partial", complete[:-1])]
    )

    assert harvest.discover_publishers(connection) == [
        harvest.Publisher("gold_complete")
    ]
    assert connection.cursor_instance.executed[0][1] == (harvest.PUBLISHER_VIEW,)


def test_harvest_all_isolates_publishers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: ARC-001 — a failed publisher cannot roll back its peers."""
    discovery = Connection([])
    publishers = [harvest.Publisher("gold_good"), harvest.Publisher("gold_bad")]
    monkeypatch.setattr(harvest, "discover_publishers", lambda _: publishers)

    def harvest_one(_: Any, publisher: harvest.Publisher) -> int:
        if publisher.schema == "gold_bad":
            raise RuntimeError("credentials=secret\nprovider unavailable")
        return 3

    monkeypatch.setattr(harvest, "harvest_publisher", harvest_one)

    assert harvest.harvest_all_publishers(lambda: discovery) == {
        "gold_good": 3,
        "gold_bad": "credentials=secret=*** unavailable",
    }
    assert discovery.closed


def test_emit_latest_returns_none_for_empty_publisher() -> None:
    """Covers: ARC-001 — empty publishers emit no shared event."""
    connection = Connection([])

    assert (
        harvest.emit_latest_publisher_ready(
            lambda: connection, publisher_schema="gold_empty"
        )
        is None
    )
    assert connection.closed


def test_orchestration_limits_must_be_positive() -> None:
    """Covers: ARC-001 — shared orchestration rejects unsafe limits."""
    with pytest.raises(ValueError, match="retirement grace"):
        harvest.harvest_publisher(
            lambda: Connection([]),
            harvest.Publisher("gold"),
            retirement_grace_harvests=0,
        )
    with pytest.raises(ValueError, match="event limit"):
        harvest.process_pending_events(lambda: Connection([]), limit=0)


def test_emit_latest_forwards_publisher_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ARC-001 — publisher events retain provider-neutral identity."""
    published_at = datetime(2026, 8, 19, tzinfo=timezone.utc)
    connection = Connection([("FRED", "1.0", "42", None, published_at)])
    received: dict[str, Any] = {}

    def emit(_: Any, **values: Any) -> str:
        received.update(values)
        return "event-id"

    monkeypatch.setattr(harvest, "emit_publisher_ready", emit)

    assert (
        harvest.emit_latest_publisher_ready(
            lambda: connection, publisher_schema="gold_fred"
        )
        == "event-id"
    )
    assert received == {
        "source_code": "FRED",
        "publisher_contract_version": "1.0",
        "source_watermark": "42",
        "source_run_id": None,
        "publication_time": published_at,
    }
