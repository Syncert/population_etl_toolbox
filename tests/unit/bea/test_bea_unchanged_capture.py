"""Offline contract: an unchanged BEA file is recorded, never replayed again.

Covers: ETL-058
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from data_ingestion_toolbox.bea.capture import (
    PARSER_CONTRACT_VERSION,
    capture_table,
    is_unchanged,
)
from data_ingestion_toolbox.bea.config import BeaConfig
from data_ingestion_toolbox.bea.registry import get_table
from data_ingestion_toolbox.bea.silver_bea.load import replay_run
from data_ingestion_toolbox.capture import CaptureReceipt

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bea"
PUBLISHED_AT = datetime(2026, 10, 28, 14, 5, tzinfo=UTC)


class _Cursor:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self._result: tuple | None = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, parameters: tuple = ()) -> None:
        self.database.statements.append((sql, parameters))
        if "capture.in_scope_row_count" in sql:
            # The published capture's count already matches its replayed rows.
            self._result = (9471, 9471)
        elif "SELECT capture.capture_id::TEXT" in sql:
            self._result = (str(uuid4()),)
        elif "FROM control.bea_table_capture AS capture" in sql:
            self._result = self.database.published
        elif "FROM silver_bea.fact_observation" in sql:
            # Every registered line already has facts.
            self._result = (True,)
        elif "bridge_geo_relationship_version" in sql:
            self._result = (self.database.delineation_since,)
        elif sql.lstrip().startswith("INSERT INTO control.bea_table_capture"):
            self.database.capture_rows.append(parameters)
            self._result = None
        elif "FROM control.bea_table_capture WHERE run_id" in sql:
            run_id = parameters[0]
            row = next(r for r in self.database.capture_rows if r[0] == run_id)
            self._result = (row[1], row[2], row[3])
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def fetchone(self) -> tuple | None:
        return self._result


class _Database:
    def __init__(
        self,
        published: tuple | None = None,
        *,
        delineation_since: bool = False,
    ) -> None:
        self.published = published
        self.delineation_since = delineation_since
        self.statements: list[tuple[str, tuple]] = []
        self.capture_rows: list[tuple] = []

    def connect(self) -> _Database:
        return self

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _Control:
    def __init__(self) -> None:
        self.runs: list[str] = []

    def start_run(self, *, watermark: dict[str, Any]) -> UUID:
        return uuid4()

    def start_request(self, **_: Any) -> Any:
        return type("Request", (), {"request_id": uuid4()})()

    def record_request_retry(self, *_: Any, **__: Any) -> None:
        return None

    def finish_request(self, *_: Any, **__: Any) -> None:
        return None

    def finish_run(self, _run_id: UUID, *, status: str, **__: Any) -> None:
        self.runs.append(status)


class _Client:
    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        return httpx.Response(
            200,
            content=(FIXTURES / "CAINC1.zip").read_bytes(),
            headers={"content-type": "application/zip"},
            request=httpx.Request("GET", url),
        )

    def close(self) -> None:
        return None


def _capture(database: _Database, checksum: str) -> tuple[UUID, str]:
    control = _Control()
    run_id, _capture_id = capture_table(
        database.connect,
        get_table("CAINC1"),
        config=BeaConfig(min_spacing_seconds=0, max_attempts=1),
        client=_Client(),
        control=control,
        persist_capture=lambda _factory, capture: CaptureReceipt(
            capture.capture_id, checksum
        ),
    )
    assert control.runs == ["success"]
    (row,) = [r for r in database.capture_rows if r[0] == str(run_id)]
    return run_id, row[3]


def test_a_never_published_table_is_captured_for_replay() -> None:
    """Covers: ETL-058 — a table with no publication is loaded (the loader rule)."""
    database = _Database(published=None)
    _run, status = _capture(database, "a" * 64)
    assert status == "captured"


def test_the_same_bytes_as_the_last_publication_are_unchanged_and_replay_nothing() -> (
    None
):
    """Covers: ETL-058 — a weekly read of an unchanged file adds no observation."""
    database = _Database(published=("a" * 64, PARSER_CONTRACT_VERSION, PUBLISHED_AT))
    run_id, status = _capture(database, "a" * 64)
    assert status == "unchanged"
    before = len(database.statements)
    assert replay_run(database.connect, run_id=run_id) == 0
    # Replay read the run's status, found every registered line loaded and
    # the published capture's count matching its replayed rows, and stopped:
    # no parse, no insert, no update.
    replayed = database.statements[before:]
    assert "WHERE run_id" in replayed[0][0]
    assert all(
        "fact_observation" in sql or "control.bea_table_capture AS capture" in sql
        for sql, _p in replayed[1:]
    )
    assert any("in_scope_row_count" in sql for sql, _p in replayed)
    assert not any(
        "INSERT" in sql or "UPDATE" in sql or "load_captured" in sql
        for sql, _p in replayed
    )


@pytest.mark.parametrize(
    ("published", "delineation_since"),
    [
        # BEA posted a new release: different bytes.
        (("b" * 64, PARSER_CONTRACT_VERSION, PUBLISHED_AT), False),
        # The parser contract changed since the last publication.
        (("a" * 64, "bea_regional_bulk_csv:v0", PUBLISHED_AT), False),
        # A newer CBSA delineation changes the metro areas a county table sums.
        (("a" * 64, PARSER_CONTRACT_VERSION, PUBLISHED_AT), True),
    ],
)
def test_new_bytes_a_new_parser_or_a_new_delineation_replay(
    published: tuple, delineation_since: bool
) -> None:
    """Covers: ETL-058 — anything that would change the replayed rows replays."""
    database = _Database(published=published, delineation_since=delineation_since)
    _run, status = _capture(database, "a" * 64)
    assert status == "captured"


def test_a_state_table_ignores_the_metro_delineation() -> None:
    """Covers: ETL-058 — only county tables derive metro areas from members."""
    database = _Database(
        published=("a" * 64, PARSER_CONTRACT_VERSION, PUBLISHED_AT),
        delineation_since=True,
    )
    assert is_unchanged(database.connect, get_table("SARPP"), "a" * 64)
    assert not any(
        "bridge_geo_relationship_version" in sql for sql, _ in database.statements
    )
