"""Load a table added to the curated list without asking again for the rest.

A slice's ledger row carries the fingerprint of the variables it was asked
for (`dags/acs_ingest_dag.py`), so adding a table to `AcsConfig.curated_tables`
changes the fingerprint of every year that publishes it and the scheduled run
plans every such slice again: the loader rule (a period never loaded is loaded
on a scheduled run) holds with no manual run. Asking each slice for its whole
list again would be wasteful, though. Revisions are append-only
(`silver_census.observation_revision`), so a full re-request writes a second
copy of every cell already held -- for the tract and place years, terabytes --
when only the added table's cells are new.

`AcsConfig.added_tables` names the tables added after years were loaded
without them, oldest first. A slice finished under the list as it stood
before one of them was added is asked for the added tables only; the silver
transform reads the newest revision of each cell across every capture, so the
slice's earlier cells and the new ones are read together. A slice whose
fingerprint matches no earlier list (never loaded, or changed some other way)
is asked for its whole list, as before.

While the request is outstanding the ledger holds ``PENDING_PREFIX`` and the
earlier fingerprint, never the current one, so a failed or interrupted
request is asked for the added tables again, and a slice that failed under
the earlier list (whose cells may be partial) is never mistaken for one that
finished.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

#: Marks a ledger hash as "finished under this earlier fingerprint, added
#: tables requested". It is never a SHA-256 digest, so it never equals one.
PENDING_PREFIX = "added-tables-pending:"


@dataclass(frozen=True)
class AddedTablesRequest:
    """The added tables one slice is asked for, and what the ledger holds meanwhile."""

    tables: list[str]
    ledger_hash: str


def pending_hash(earlier_hash: str) -> str:
    return f"{PENDING_PREFIX}{earlier_hash}"


def earlier_lists(
    tables: Sequence[str], added_tables: Sequence[str]
) -> list[tuple[list[str], list[str]]]:
    """Each earlier form of ``tables`` with the tables added since, newest first.

    ``added_tables`` is oldest first. The first pair is the list before the
    newest addition, the last the list before the oldest. An addition the
    level does not request (a table outside the tract list, say) leaves no
    earlier form of its own.
    """
    pairs: list[tuple[list[str], list[str]]] = []
    for start in range(len(added_tables) - 1, -1, -1):
        since = [table for table in added_tables[start:] if table in tables]
        if not since or (pairs and pairs[-1][1] == since):
            continue
        pairs.append(([table for table in tables if table not in since], since))
    return pairs


def added_tables_request(
    stored_hash: str | None,
    stored_status: str | None,
    tables: Sequence[str],
    added_tables: Sequence[str],
    fingerprint: Callable[[list[str]], str],
) -> AddedTablesRequest | None:
    """The added tables to ask a stale slice for, or ``None`` to ask for all of ``tables``.

    ``stored_hash`` and ``stored_status`` are the slice's ledger row;
    ``fingerprint`` digests a table list the way the planner does for the
    slice's dataset and year. Only a slice that *finished* (``success`` or
    ``empty``) under an earlier list, or one already waiting on its added
    tables, is narrowed.
    """
    if not stored_hash:
        return None
    if stored_hash.startswith(PENDING_PREFIX):
        earlier = stored_hash[len(PENDING_PREFIX) :]
    elif stored_status in ("success", "empty"):
        earlier = stored_hash
    else:
        return None
    for before, since in earlier_lists(tables, added_tables):
        if fingerprint(before) == earlier:
            return AddedTablesRequest(tables=since, ledger_hash=pending_hash(earlier))
    return None
