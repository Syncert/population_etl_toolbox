#!/usr/bin/env python3
"""Keep a deletion promise across a database restore (ADR-0005 §5).

ADR-0005 §5 commits to more than clearing the live database:

    "The contract: the deployment declares a backup retention window, deleted
    data is gone from production immediately and from every retained backup
    once that window has passed, and a restore performed inside the window
    re-applies the deletion log before the database serves traffic."

A hard ``DELETE`` does the first part and nothing at all about the second. A
point-in-time snapshot taken an hour before a deletion still contains the
account, so restoring from it brings back a person who asked to be forgotten --
silently, and with every saved analysis and evidence packet they owned.

This script is the third clause. Three operations, deliberately separate:

``--export PATH``
    Write the deletion log out. **This is the one that has to happen before a
    restore, not after**, and it is why the log cannot live only in the
    database. A restore brings the log back as it stood at the restore point,
    which by definition excludes the deletions that happened afterwards --
    exactly the ones that need re-applying. Run this on a schedule beside the
    backup itself, and keep the output somewhere a restore does not overwrite.

``--apply PATH``
    Re-apply an exported log against a restored database: every account id it
    names is deleted, cascading exactly as the original deletion did. Run it
    **before the database serves traffic**. Idempotent -- an id that is already
    absent is the normal case and is not an error.

``--purge-expired DAYS``
    Drop log entries older than the declared retention window. By then no
    retained backup contains the account, so the entry is the last remaining
    trace of it, and ADR-0005 §5's "what it keeps after deletion is nothing"
    is only true once it is gone too.

What the log holds is an account id and a timestamp. Not an address, not a
provider subject, not a name, and nothing the account created -- so the file
this writes is not a file of personal data, and the mechanism that keeps the
promise does not itself become the thing the promise was about.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import psycopg2

#: The export's columns, in order. A CSV rather than JSON because the only
#: consumer is this script and an operator reading it to check a count, and
#: because it appends cleanly if a deployment ever wants to.
FIELDS = ("user_account_id", "deleted_at")


def connect(dsn: str):
    connection = psycopg2.connect(dsn)
    connection.autocommit = False
    return connection


def export_log(connection, destination: Path) -> int:
    """Write every log entry to ``destination``; returns how many."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT user_account_id, deleted_at FROM app_api.account_deletion_log"
            " ORDER BY deleted_at, user_account_id"
        )
        rows = cursor.fetchall()

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(FIELDS)
        for user_account_id, deleted_at in rows:
            writer.writerow([user_account_id, deleted_at.isoformat()])
    return len(rows)


def read_export(source: Path) -> list[tuple[int, str]]:
    """Parse an exported log, refusing one that is not this script's output.

    A wrong file here deletes accounts. So the header is checked rather than
    skipped: a CSV of something else entirely whose first column happens to
    hold integers would otherwise be read as a list of people to destroy.
    """
    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"{source} is empty; it is not a deletion log") from None
        if tuple(field.strip() for field in header) != FIELDS:
            raise ValueError(
                f"{source} does not look like a deletion log: expected a header of "
                f"{','.join(FIELDS)}, found {','.join(header)}"
            )
        entries: list[tuple[int, str]] = []
        for line, row in enumerate(reader, start=2):
            if not row:
                continue
            if len(row) != len(FIELDS):
                raise ValueError(f"{source}:{line} has {len(row)} fields, expected 2")
            try:
                entries.append((int(row[0]), row[1]))
            except ValueError:
                raise ValueError(
                    f"{source}:{line} does not begin with an account id: {row[0]!r}"
                ) from None
    return entries


def apply_log(connection, entries: Sequence[tuple[int, str]]) -> tuple[int, int]:
    """Re-delete every account the log names; returns ``(deleted, already_gone)``.

    The log is also restored into the database, so a second restore from an
    older snapshot is covered by the same file and the deployment does not
    quietly lose the record of what it has promised.
    """
    if not entries:
        return (0, 0)
    identifiers = [identifier for identifier, _ in entries]
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO app_api.account_deletion_log (user_account_id, deleted_at)"
            " VALUES (%s, %s) ON CONFLICT (user_account_id) DO NOTHING",
            entries,
        )
        cursor.execute(
            "DELETE FROM app_api.user_account WHERE user_account_id = ANY(%s)",
            (identifiers,),
        )
        deleted = cursor.rowcount
    connection.commit()
    # An id already absent is the normal case: most of the log describes
    # accounts this database never had back, and re-applying is a no-op for
    # them. Counted rather than silent, because "0 deleted" from a log of
    # thousands is how an operator learns they pointed it at the wrong
    # database.
    return (deleted, len(identifiers) - deleted)


def purge_expired(connection, retention_days: int, now: datetime | None = None) -> int:
    """Drop entries older than the retention window; returns how many."""
    if retention_days <= 0:
        raise ValueError(
            "a retention window of zero days would purge the log immediately, "
            "which is the opposite of what it is for; declare the deployment's "
            "real window"
        )
    moment = now or datetime.now(timezone.utc)
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM app_api.account_deletion_log"
            " WHERE deleted_at < %s - make_interval(days => %s)",
            (moment, retention_days),
        )
        purged = cursor.rowcount
    connection.commit()
    return purged


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True, help="libpq connection string")
    parser.add_argument("--export", metavar="PATH", default="")
    parser.add_argument("--apply", metavar="PATH", default="")
    parser.add_argument("--purge-expired", metavar="DAYS", type=int, default=0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if not (args.export or args.apply or args.purge_expired):
        print(
            "Nothing to do: pass --export, --apply, or --purge-expired.",
            file=sys.stderr,
        )
        return 2

    connection = connect(args.dsn)
    try:
        if args.export:
            count = export_log(connection, Path(args.export))
            print(f"Exported {count} deletion log entries to {args.export}.")
            print(
                "Keep this somewhere a restore does not overwrite; a restored "
                "log does not contain the deletions made after the restore point."
            )
        if args.apply:
            entries = read_export(Path(args.apply))
            deleted, already_gone = apply_log(connection, entries)
            print(
                f"Re-applied {len(entries)} deletion log entries: {deleted} "
                f"account(s) deleted, {already_gone} already absent."
            )
            if entries and deleted == 0:
                print(
                    "Every id was already absent. That is the expected result "
                    "for a database that was never restored -- and also what a "
                    "wrong --dsn looks like.",
                    file=sys.stderr,
                )
        if args.purge_expired:
            purged = purge_expired(connection, args.purge_expired)
            print(f"Purged {purged} log entries older than {args.purge_expired} days.")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
