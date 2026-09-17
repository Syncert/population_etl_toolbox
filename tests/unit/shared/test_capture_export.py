"""The export path that lets capture history survive a reset.

Covers: DB-046 -- an export carries every capture and the control rows it
        points at, verifies each payload against its own checksum, and
        restores by inserting only, so the append-only triggers never have to
        be disabled.

The database half of the round trip belongs to the integration tier; this is
the half that can be checked without one. The connection here is a stand-in
DB-API object rather than a mock of one method: the module is written against
the DB-API on purpose, because the DAG resolves a real driver at task runtime
and a test that mocked the driver would prove what the mock does.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from data_ingestion_toolbox.capture_export import (
    EXPORT_FORMAT_VERSION,
    EXPORT_ROOT_SETTING,
    ExportError,
    export_directory_for,
    export_captures,
    payload_path,
    read_manifest,
    restore_captures,
    resolve_export_root,
    restore_statements,
    verify_export,
)

pytestmark = [pytest.mark.unit]

RUN_ID = "11111111-1111-1111-1111-111111111111"
REQUEST_ID = "22222222-2222-2222-2222-222222222222"
CAPTURE_ID = "33333333-3333-3333-3333-333333333333"
SECOND_CAPTURE_ID = "44444444-4444-4444-4444-444444444444"

PAYLOAD = b'[{"NAME":"Dane County, Wisconsin","B01003_001E":"561504"}]'
SECOND_PAYLOAD = b'[{"NAME":"Dane County, Wisconsin","B01003_001E":"565000"}]'
CHECKSUM = hashlib.sha256(PAYLOAD).hexdigest()
SECOND_CHECKSUM = hashlib.sha256(SECOND_PAYLOAD).hexdigest()


class _Cursor:
    """A DB-API cursor over a fixed set of answers, recording what it ran."""

    def __init__(self, tables, executed):
        self._tables = tables
        self._executed = executed
        self._rows = []
        self.description = ()

    def __enter__(self):
        return self

    def __exit__(self, *_exception):
        return False

    def execute(self, sql, parameters=None):
        self._executed.append((" ".join(sql.split()), parameters))
        for name, (columns, rows) in self._tables.items():
            if f"FROM {name}" in sql:
                selected = columns
                if "SELECT payload_checksum, payload" in sql:
                    selected = ["payload_checksum", "payload"]
                self.description = tuple((column,) for column in selected)
                indexes = [columns.index(column) for column in selected]
                self._rows = [tuple(row[index] for index in indexes) for row in rows]
                return
        self._rows = []

    def fetchmany(self, size):
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def close(self):
        return None


class _Connection:
    def __init__(self, tables):
        self._tables = tables
        self.executed = []

    def cursor(self):
        return _Cursor(self._tables, self.executed)

    def close(self):
        return None


def _warehouse(captures=1):
    run = (RUN_ID, "CENSUS_ACS", "success", datetime(2026, 3, 1, tzinfo=timezone.utc))
    request = (REQUEST_ID, RUN_ID, "CENSUS_ACS", "/data/2023/acs/acs5", "captured")
    capture_rows = [
        (
            CAPTURE_ID,
            REQUEST_ID,
            RUN_ID,
            "CENSUS_ACS",
            "/data/2023/acs/acs5",
            datetime(2026, 3, 1, 12, tzinfo=timezone.utc),
            200,
            CHECKSUM,
        )
    ]
    blob_rows = [(CHECKSUM, PAYLOAD)]
    if captures > 1:
        capture_rows.append(
            (
                SECOND_CAPTURE_ID,
                REQUEST_ID,
                RUN_ID,
                "CENSUS_ACS",
                "/data/2023/acs/acs5",
                datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
                200,
                SECOND_CHECKSUM,
            )
        )
        blob_rows.append((SECOND_CHECKSUM, SECOND_PAYLOAD))
    return {
        "control.ingestion_run": (
            ["run_id", "source_code", "status", "created_at"],
            [run],
        ),
        "control.ingestion_request": (
            ["request_id", "run_id", "source_code", "endpoint", "status"],
            [request],
        ),
        "raw_capture.response_capture": (
            [
                "capture_id",
                "request_id",
                "run_id",
                "source_code",
                "endpoint",
                "retrieved_at",
                "http_status",
                "payload_checksum",
            ],
            capture_rows,
        ),
        "raw_capture.payload_blob": (
            ["payload_checksum", "payload"],
            blob_rows,
        ),
    }


def test_an_export_carries_the_captures_and_what_they_point_at(tmp_path: Path) -> None:
    """Covers: DB-046 — a capture with no request is unprovenanced evidence."""
    connection = _Connection(_warehouse(captures=2))
    summary = export_captures(connection, tmp_path)

    assert summary.row_counts == {
        "control.ingestion_run": 1,
        "control.ingestion_request": 1,
        "raw_capture.response_capture": 2,
    }
    assert summary.payload_count == 2
    assert summary.payload_bytes == len(PAYLOAD) + len(SECOND_PAYLOAD)

    manifest = read_manifest(tmp_path)
    assert manifest["format_version"] == EXPORT_FORMAT_VERSION
    # The range an operator quotes when asking whether this is the export with
    # the March captures in it.
    assert manifest["capture_id_range"] == {
        "first": CAPTURE_ID,
        "last": SECOND_CAPTURE_ID,
    }

    # The payload is on disk under its own checksum, byte for byte.
    assert payload_path(tmp_path, CHECKSUM).read_bytes() == PAYLOAD


def test_an_export_reads_and_never_writes(tmp_path: Path) -> None:
    """Covers: DB-046 — a read that could change what it reads is not evidence."""
    connection = _Connection(_warehouse())
    export_captures(connection, tmp_path)

    statements = [sql for sql, _ in connection.executed]
    assert statements, "the export ran no statements at all"
    for statement in statements:
        assert statement.upper().startswith("SELECT"), statement


def test_a_timestamp_keeps_its_offset(tmp_path: Path) -> None:
    """Covers: DB-046 — when a provider answered is a fact about the answer."""
    export_captures(_Connection(_warehouse()), tmp_path)
    [row] = [
        json.loads(line)
        for line in (tmp_path / "response_capture.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert row["retrieved_at"] == "2026-03-01T12:00:00+00:00"


def test_a_payload_whose_stored_checksum_is_wrong_fails_the_export(
    tmp_path: Path,
) -> None:
    """Covers: DB-046 — corruption is not carried forward under a good name."""
    tables = _warehouse()
    tables["raw_capture.payload_blob"] = (
        ["payload_checksum", "payload"],
        [(CHECKSUM, b"not the bytes this checksum describes")],
    )
    with pytest.raises(ExportError, match="does not match its stored bytes"):
        export_captures(_Connection(tables), tmp_path)


def test_verification_reads_the_bytes_rather_than_the_manifest(tmp_path: Path) -> None:
    """Covers: DB-046 — a payload file's name is its verification."""
    export_captures(_Connection(_warehouse()), tmp_path)
    assert verify_export(tmp_path) == 1

    payload_path(tmp_path, CHECKSUM).write_bytes(b"substituted")
    with pytest.raises(ExportError, match="sha256"):
        verify_export(tmp_path)


def test_a_missing_payload_fails_verification(tmp_path: Path) -> None:
    """Covers: DB-046 — an export the manifest overstates is not an export."""
    export_captures(_Connection(_warehouse(captures=2)), tmp_path)
    payload_path(tmp_path, SECOND_CHECKSUM).unlink()
    with pytest.raises(ExportError, match="declares 2 payloads and 1"):
        verify_export(tmp_path)


def test_a_directory_that_is_not_an_export_is_refused(tmp_path: Path) -> None:
    """Covers: DB-046 — a restore says so rather than loading nothing."""
    with pytest.raises(ExportError, match="not an export"):
        read_manifest(tmp_path)

    (tmp_path / "manifest.json").write_text(json.dumps({"format_version": 99}))
    with pytest.raises(ExportError, match="is not 1"):
        read_manifest(tmp_path)


def test_a_restore_inserts_and_never_mutates(tmp_path: Path) -> None:
    """Covers: DB-046 — the append-only triggers stay in place through it."""
    export_captures(_Connection(_warehouse(captures=2)), tmp_path)

    target = _Connection({})
    loaded = restore_captures(target, tmp_path)
    assert loaded == {
        "control.ingestion_run": 1,
        "control.ingestion_request": 1,
        "raw_capture.payload_blob": 2,
        "raw_capture.response_capture": 2,
    }

    statements = [sql for sql, _ in target.executed]
    for statement in statements:
        assert statement.upper().startswith("INSERT INTO"), statement
        # Not an upsert: an upsert is an update, and an update on these
        # relations is exactly what the triggers refuse.
        assert "ON CONFLICT DO NOTHING" in statement
        assert "DO UPDATE" not in statement.upper()
    for forbidden in ("DROP ", "ALTER ", "DELETE ", "TRUNCATE", "DISABLE TRIGGER"):
        assert not any(forbidden in statement.upper() for statement in statements), (
            forbidden
        )


def test_a_restore_loads_a_row_only_after_what_it_references(tmp_path: Path) -> None:
    """Covers: DB-046 — foreign-key order is the only order that works."""
    export_captures(_Connection(_warehouse()), tmp_path)
    target = _Connection({})
    restore_captures(target, tmp_path)

    order = [sql for sql, _ in target.executed]
    position = {
        "run": next(i for i, sql in enumerate(order) if "control.ingestion_run" in sql),
        "request": next(
            i for i, sql in enumerate(order) if "control.ingestion_request" in sql
        ),
        "payload": next(i for i, sql in enumerate(order) if "payload_blob" in sql),
        "capture": next(i for i, sql in enumerate(order) if "response_capture" in sql),
    }
    assert position["run"] < position["request"] < position["capture"]
    # A capture references a payload as well as a request.
    assert position["payload"] < position["capture"]


def test_a_restore_recomputes_the_payload_size_from_the_bytes(tmp_path: Path) -> None:
    """Covers: DB-046 — the table's CHECK compares it to OCTET_LENGTH."""
    export_captures(_Connection(_warehouse()), tmp_path)
    target = _Connection({})
    restore_captures(target, tmp_path)

    [(_, parameters)] = [
        (sql, parameters)
        for sql, parameters in target.executed
        if "payload_blob" in sql
    ]
    checksum, payload, size = parameters
    assert checksum == CHECKSUM
    assert payload == PAYLOAD
    assert size == len(PAYLOAD)


def test_a_restore_refuses_an_export_it_cannot_verify(tmp_path: Path) -> None:
    """Covers: DB-046 — corruption fails before the database, not inside it."""
    export_captures(_Connection(_warehouse()), tmp_path)
    payload_path(tmp_path, CHECKSUM).write_bytes(b"substituted")

    target = _Connection({})
    with pytest.raises(ExportError):
        restore_captures(target, tmp_path)
    assert target.executed == [], "a statement ran against an unverified export"


def test_one_row_is_one_statement_naming_its_own_columns() -> None:
    """Covers: DB-046 — a positional insert breaks when a column is added."""
    statement, parameters = restore_statements(
        {"run_id": RUN_ID, "source_code": "CENSUS_ACS"}, "control.ingestion_run"
    )
    assert statement.startswith(
        "INSERT INTO control.ingestion_run (run_id, source_code)"
    )
    assert "VALUES (%s, %s)" in statement
    assert parameters == [RUN_ID, "CENSUS_ACS"]


def test_a_payload_path_is_refused_for_anything_but_a_sha256() -> None:
    """Covers: DB-046 — the filename is the checksum, so it is checked."""
    for bad in ("../../etc/passwd", "", "ZZ" * 32, CHECKSUM[:-1]):
        with pytest.raises(ExportError, match="not a sha256"):
            payload_path(Path("/tmp"), bad)


def test_the_export_root_is_never_guessed() -> None:
    """Covers: DB-046 — a default path would be inside the volume it protects."""
    with pytest.raises(ExportError, match="does not survive the reset"):
        resolve_export_root(environment={})

    assert resolve_export_root(
        environment={EXPORT_ROOT_SETTING: "/srv/exports"}
    ) == Path("/srv/exports")
    # Whitespace is not configuration.
    with pytest.raises(ExportError):
        resolve_export_root(environment={EXPORT_ROOT_SETTING: "   "})


def test_the_environment_wins_over_the_variable_and_either_will_do() -> None:
    """Covers: DB-046 — a deployment sets whichever of the two it already uses."""
    read = resolve_export_root(
        variable_reader=lambda _name: "/from/variable",
        environment={EXPORT_ROOT_SETTING: "/from/environment"},
    )
    assert read == Path("/from/environment")

    assert resolve_export_root(
        variable_reader=lambda _name: "/from/variable", environment={}
    ) == Path("/from/variable")

    # A reader that answers nothing is the same as no reader at all.
    with pytest.raises(ExportError):
        resolve_export_root(variable_reader=lambda _name: "", environment={})


def test_one_directory_per_run_named_for_its_date() -> None:
    """Covers: DB-046 — an operator names the pre-reset export without reading it."""
    directory = export_directory_for(
        Path("/srv/exports"), datetime(2026, 3, 1, 3, 0, tzinfo=timezone.utc)
    )
    assert directory == Path("/srv/exports/capture-export-20260301T030000Z")
    # A run in another zone lands under the same UTC name, so two schedulers
    # cannot write two differently named directories for one logical date.
    from datetime import timedelta as _timedelta

    other_zone = datetime(2026, 2, 28, 22, 0, tzinfo=timezone(-_timedelta(hours=5)))
    assert export_directory_for(Path("/srv/exports"), other_zone).name.endswith("Z")
    assert export_directory_for(Path("/srv/exports"), other_zone) == Path(
        "/srv/exports/capture-export-20260301T030000Z"
    )
