"""Export and restore the one layer a warehouse reset cannot reproduce.

ADR-0001 makes captures append-only and protects them with statement triggers,
and it also accepts destroying a beta environment and re-ingesting. Those two
statements are only compatible if a reset re-captures the same evidence, and it
does not: ``BETA_RESET_REINGESTION.md`` re-ingests *current* provider data.
FRED vintages, CDC releases the provider has since superseded, NASS revisions
and FBI refreshes captured earlier are gone after a reset, and with them the
revision history the repository's invariants ask to preserve. ADR-0006 records
the decision that capture history survives a reset; this module is the path
that makes it so.

What is exported, and why exactly this much:

- ``raw_capture.payload_blob`` and ``raw_capture.response_capture`` -- the
  evidence itself.
- ``control.ingestion_run`` and ``control.ingestion_request`` -- the rows a
  capture's foreign keys point at. Without them a restore cannot insert a
  capture at all, and a capture with no request is evidence nobody can say the
  provenance of.

What is not, and why:

- ``control.capture_quarantine`` references captures rather than being
  referenced by them, and it records a *parser's* failure against a capture.
  Replaying the restored captures through the current parser produces current
  quarantine state, which is the state worth having; carrying the old rows
  forward would restore a claim about a parser version that is no longer
  running.
- The slice ledgers (``control.*_ingestion_slices``) are watermarks for
  planning work, not evidence of a response. A reset intends to re-plan.
- Silver and gold are reproducible by design and are not this module's
  business.

The on-disk shape is deliberately boring: newline-delimited JSON for the rows
and one file per payload, named by its own checksum. A payload file's name is
its verification -- a restore recomputes the digest of the bytes it is about
to load and refuses a file whose name does not match its contents, so a
corrupted or substituted export fails before it reaches the database rather
than after.

The restore inserts and never updates. The append-only triggers from migration
001 reject ``UPDATE``, ``DELETE`` and ``TRUNCATE`` on both raw relations, and
they stay in place for the whole restore: a load that has to disable them is a
load that could have rewritten history, which is the property being restored.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: The export layout's own version. A restore refuses a layout it does not
#: know rather than guessing at it.
EXPORT_FORMAT_VERSION = 1

MANIFEST_NAME = "manifest.json"
PAYLOAD_DIRECTORY = "payload"

#: The tables an export carries, in the order a restore must insert them.
#: Reversing this list is not a valid delete order and is not offered: this
#: module never deletes anything.
EXPORTED_TABLES: tuple[tuple[str, str], ...] = (
    ("control.ingestion_run", "ingestion_run.jsonl"),
    ("control.ingestion_request", "ingestion_request.jsonl"),
    ("raw_capture.response_capture", "response_capture.jsonl"),
)

#: Ordered so a row always lands after the rows it references.
_TABLE_KEYS: Mapping[str, tuple[str, ...]] = {
    "control.ingestion_run": ("run_id",),
    "control.ingestion_request": ("request_id",),
    "raw_capture.response_capture": ("capture_id",),
}


class ExportError(RuntimeError):
    """An export that cannot be written, or one that cannot be trusted."""


@dataclass(frozen=True)
class ExportSummary:
    """What one export run produced, and enough to check it later."""

    directory: Path
    exported_at: datetime
    row_counts: dict[str, int] = field(default_factory=dict)
    payload_count: int = 0
    payload_bytes: int = 0
    first_capture_id: str | None = None
    last_capture_id: str | None = None

    def manifest(self) -> dict[str, Any]:
        return {
            "format_version": EXPORT_FORMAT_VERSION,
            "exported_at": self.exported_at.astimezone(timezone.utc).isoformat(),
            "row_counts": dict(self.row_counts),
            "payload_count": self.payload_count,
            "payload_bytes": self.payload_bytes,
            # The range an operator quotes when asking "is this export the one
            # that has the March captures in it?".
            "capture_id_range": {
                "first": self.first_capture_id,
                "last": self.last_capture_id,
            },
            "tables": [table for table, _ in EXPORTED_TABLES],
            "payload_checksum_algorithm": "sha256",
        }


def _json_ready(value: Any) -> Any:
    """A row value in a form JSON can hold without losing what it was."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        # Timestamps keep their offset: a capture's `retrieved_at` is a fact
        # about when a provider answered, and a naive copy of it is not.
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ExportError(
            "binary column values belong in the payload directory, not in a row file"
        )
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return str(value)


def _write_rows(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    written = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps({key: _json_ready(value) for key, value in row.items()})
            )
            handle.write("\n")
            written += 1
    return written


def _read_rows(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as error:
                raise ExportError(
                    f"{path.name}:{number} is not a JSON row: {error}"
                ) from error


def payload_path(directory: Path, checksum: str) -> Path:
    """Where one payload lives. The name is the checksum, on purpose."""
    if (
        not isinstance(checksum, str)
        or len(checksum) != 64
        or not all(character in "0123456789abcdef" for character in checksum)
    ):
        raise ExportError(f"not a sha256 checksum: {checksum!r}")
    return directory / PAYLOAD_DIRECTORY / f"{checksum}.bin"


def export_captures(connection: Any, directory: Path | str) -> ExportSummary:
    """Write every capture and the control rows it points at to ``directory``.

    ``connection`` is any DB-API connection; the module is deliberately
    ignorant of which driver, because the DAG resolves that at task runtime
    and the tests supply a stand-in.

    Nothing here writes to the database. An export is a read, and a read that
    could change what it is reading is not evidence.
    """
    target = Path(directory)
    (target / PAYLOAD_DIRECTORY).mkdir(parents=True, exist_ok=True)
    exported_at = datetime.now(timezone.utc)

    row_counts: dict[str, int] = {}
    for table, filename in EXPORTED_TABLES:
        order = ", ".join(_TABLE_KEYS[table])
        rows = _query(connection, f"SELECT * FROM {table} ORDER BY {order}")
        row_counts[table] = _write_rows(target / filename, rows)

    # The payloads last, and streamed one at a time: `payload_blob.payload` is
    # BYTEA and is the largest thing in the database. Holding the whole set in
    # memory to write it out would make the export fail on exactly the
    # warehouse that most needs one.
    payload_count = 0
    payload_bytes = 0
    for row in _query(
        connection, "SELECT payload_checksum, payload FROM raw_capture.payload_blob"
    ):
        checksum = str(row["payload_checksum"])
        payload = bytes(row["payload"])
        digest = hashlib.sha256(payload).hexdigest()
        if digest != checksum:
            # The database's own stored checksum disagrees with its bytes.
            # Exporting it would carry the corruption forward under a name
            # that claims it is fine.
            raise ExportError(
                f"payload {checksum} does not match its stored bytes (sha256 {digest})"
            )
        payload_path(target, checksum).write_bytes(payload)
        payload_count += 1
        payload_bytes += len(payload)

    captures = list(_read_rows(target / "response_capture.jsonl"))
    summary = ExportSummary(
        directory=target,
        exported_at=exported_at,
        row_counts=row_counts,
        payload_count=payload_count,
        payload_bytes=payload_bytes,
        first_capture_id=str(captures[0]["capture_id"]) if captures else None,
        last_capture_id=str(captures[-1]["capture_id"]) if captures else None,
    )
    (target / MANIFEST_NAME).write_text(
        json.dumps(summary.manifest(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "exported %d captures and %d payloads (%d bytes) to %s",
        row_counts.get("raw_capture.response_capture", 0),
        payload_count,
        payload_bytes,
        target,
    )
    return summary


def read_manifest(directory: Path | str) -> dict[str, Any]:
    """The export's own account of itself, or a refusal to read one."""
    path = Path(directory) / MANIFEST_NAME
    if not path.is_file():
        raise ExportError(f"no {MANIFEST_NAME} in {directory}: this is not an export")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    version = manifest.get("format_version")
    if version != EXPORT_FORMAT_VERSION:
        raise ExportError(
            f"export format {version!r} is not {EXPORT_FORMAT_VERSION}; "
            "restore it with the version of this tool that wrote it"
        )
    return manifest


def verify_export(directory: Path | str) -> int:
    """Check every payload against the name it is filed under.

    Returns the number of payloads verified. A payload file's name is its
    sha256, so this is the whole verification: a corrupted or substituted file
    fails here, before a restore, rather than inside one.
    """
    target = Path(directory)
    manifest = read_manifest(target)
    verified = 0
    for path in sorted((target / PAYLOAD_DIRECTORY).glob("*.bin")):
        expected = path.stem
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected:
            raise ExportError(f"{path.name} contains bytes whose sha256 is {digest}")
        verified += 1
    declared = int(manifest.get("payload_count", 0))
    if verified != declared:
        raise ExportError(
            f"the manifest declares {declared} payloads and {verified} are present"
        )
    return verified


def _parameter_ready(value: Any) -> Any:
    """A row value in a form a DB-API driver will send.

    `_json_ready` keeps a `jsonb` column as the object it was, which is what
    the row files should hold: a capture's `request_parameters` is a mapping,
    and flattening it to a string on the way out would make the export a worse
    record than the table. Read back, that value is a `dict`, and a DB-API
    driver has no adapter for one -- psycopg2 raises `can't adapt type 'dict'`
    and the restore stops on the first capture that carries any.

    Serialising it back to JSON text is enough. The parameter reaches
    PostgreSQL untyped, so the target column decides what it becomes, and every
    structured column in the four exported tables is `jsonb`. That is worth
    stating because it is the reason this is safe: a `text[]` column would also
    arrive here as a `list` and would need array literal syntax rather than
    JSON, so if one is ever added to these tables this helper has to learn
    about it.
    """
    if isinstance(value, (Mapping, list)):
        return json.dumps(value)
    return value


def restore_statements(row: Mapping[str, Any], table: str) -> tuple[str, list[Any]]:
    """The one statement that loads one row, and its parameters.

    ``ON CONFLICT DO NOTHING`` rather than an upsert: a restore into a
    warehouse that already holds some of this history must add what is missing
    and change nothing that is there. An upsert would be an update, and an
    update on these relations is what the append-only triggers exist to
    refuse.
    """
    columns = list(row.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    statement = (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )
    return statement, [_parameter_ready(row[column]) for column in columns]


def restore_captures(connection: Any, directory: Path | str) -> dict[str, int]:
    """Load an export back, in foreign-key order, inserting only.

    The order is the only order that works: a run, then the requests that
    reference it, then the payloads, then the captures -- which reference both
    a request and a payload. Nothing here deletes, and nothing updates, so the
    append-only triggers from migration 001 stay in place for the whole
    restore. A load that had to disable them would be a load that could
    rewrite history, which is the property being restored.
    """
    target = Path(directory)
    verify_export(target)
    loaded: dict[str, int] = {}

    with connection.cursor() as cursor:
        for table, filename in (
            ("control.ingestion_run", "ingestion_run.jsonl"),
            ("control.ingestion_request", "ingestion_request.jsonl"),
        ):
            count = 0
            for row in _read_rows(target / filename):
                statement, parameters = restore_statements(row, table)
                cursor.execute(statement, parameters)
                count += 1
            loaded[table] = count

        # The payloads come from their own files rather than from a row file,
        # so the bytes never pass through JSON. `payload_size` is recomputed
        # from what was actually read: the table's own CHECK constraint
        # compares it to `OCTET_LENGTH(payload)`, and a restore that carried a
        # size forward from the manifest would be trusting a number over the
        # bytes beside it.
        payloads = 0
        for path in sorted((target / PAYLOAD_DIRECTORY).glob("*.bin")):
            payload = path.read_bytes()
            cursor.execute(
                "INSERT INTO raw_capture.payload_blob "
                "(payload_checksum, checksum_algorithm, payload, payload_size) "
                "VALUES (%s, 'sha256', %s, %s) ON CONFLICT DO NOTHING",
                [path.stem, payload, len(payload)],
            )
            payloads += 1
        loaded["raw_capture.payload_blob"] = payloads

        # And the captures last, because each one references a request and a
        # payload that must already be there.
        captures = 0
        for row in _read_rows(target / "response_capture.jsonl"):
            statement, parameters = restore_statements(
                row, "raw_capture.response_capture"
            )
            cursor.execute(statement, parameters)
            captures += 1
        loaded["raw_capture.response_capture"] = captures

    logger.info("restored %s from %s", loaded, target)
    return loaded


def _query(connection: Any, sql: str) -> Iterator[dict[str, Any]]:
    """Every row of one statement, as dictionaries, streamed.

    Written against the DB-API rather than a driver: the DAG resolves a
    connection at task runtime, and the tests hand this a stand-in. Rows come
    back as dictionaries whichever cursor is used, because a positional row is
    a thing that silently means something different when a column is added.
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [description[0] for description in cursor.description]
        while True:
            batch = cursor.fetchmany(256)
            if not batch:
                return
            for row in batch:
                yield dict(row) if isinstance(row, Mapping) else dict(zip(columns, row))


#: Where exports are written. Named the same in the environment and as an
#: Airflow Variable, so a deployment can set whichever it already uses.
EXPORT_ROOT_SETTING = "CAPTURE_EXPORT_ROOT"


def resolve_export_root(
    variable_reader: Callable[[str], str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Where this deployment keeps its exports, or a refusal to guess.

    There is no default, deliberately. A default would be a path inside the
    container, and an export inside the container is one a reset destroys
    along with everything else -- the failure this whole path exists to
    prevent, dressed up as a success.
    """
    source = os.environ if environment is None else environment
    configured = str(source.get(EXPORT_ROOT_SETTING, "")).strip()
    if not configured and variable_reader is not None:
        configured = str(variable_reader(EXPORT_ROOT_SETTING) or "").strip()
    if not configured:
        raise ExportError(
            f"{EXPORT_ROOT_SETTING} is not set. Point it at a writable path outside "
            "the database volume; an export that shares a volume with the database "
            "it protects does not survive the reset it exists for (ADR-0006)."
        )
    return Path(configured)


def export_directory_for(root: Path, logical_date: datetime) -> Path:
    """One directory per run, named so an operator can pick one by date."""
    stamp = logical_date.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return root / f"capture-export-{stamp}"
