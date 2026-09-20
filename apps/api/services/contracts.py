"""The shared guard for declared serving contracts.

API-002 established the rule for observation reads: a relation the API declares
a dependency on but the warehouse does not have is a deployment fault, not a
client error, and it fails before any query runs. API-003 extends the same rule
to catalog discovery, so the guard lives here rather than being copied into a
second service module.

The exception is handled at the application level (``apps/api/main.py``), which
answers the same sanitized 503 as a database outage. A caller cannot use the
response to probe which warehouse objects exist; the relation name goes to the
server log where an operator can act on it.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


class ServingContractUnavailable(RuntimeError):
    """A relation the API declares a dependency on is absent from the warehouse.

    This is a deployment fault, not a client error: the bootstrap manifest did
    not run, ran partially, or ran against a different database than the one the
    API is pointed at. It is raised rather than absorbed so the failure names the
    missing relation in the server log instead of surfacing as an empty page that
    looks like "this metric has no data".
    """


#: Where one session records the relations it has already probed.
#:
#: ``Session.info`` is SQLAlchemy's own per-session dictionary: it is created
#: with the session and discarded with it, which is the property that makes
#: this safe. The warehouse session is ``REPEATABLE READ`` (``apps.api.database``),
#: so a relation's existence cannot change inside one request -- and the memo
#: cannot outlive the snapshot it was true under, because it cannot outlive the
#: session.
_PROBED_RELATIONS_KEY = "apps.api.probed_relations"


def session_memo(db: Session, key: str) -> dict | None:
    """A dictionary that lives and dies with one session, or ``None``.

    ``None`` for anything without a usable ``info`` mapping -- a stub in a
    deterministic unit test. Such a session is not memoised rather than being
    given a memo of its own, so a test double behaves exactly as it did before.

    What may be memoised here is anything the request's snapshot fixes.
    ``REPEATABLE READ`` is what makes that a short list and a safe one: a
    relation's existence and a published row's contents cannot change inside
    one request, and the memo cannot outlive the snapshot because it cannot
    outlive the session.
    """
    info = getattr(db, "info", None)
    if not isinstance(info, dict):
        return None
    memo = info.get(key)
    if memo is None:
        memo = {}
        info[key] = memo
    return memo if isinstance(memo, dict) else None


def _probe_memo(db: Session) -> dict | None:
    """This session's record of the relations it has already probed."""
    return session_memo(db, _PROBED_RELATIONS_KEY)


def relation_is_absent(db: Session, relation_name: str) -> bool:
    """True only when the database positively reports the relation missing.

    A session that cannot answer the question -- a stub in a deterministic unit
    test, or a driver that raises -- is not evidence of absence, so the check
    stays silent rather than inventing a deployment fault from a test double.

    Probed at most once per relation per session (API-147). Fifteen call sites
    outside this module guard every serving read, so one `/observations` page
    cost five round trips and an evidence packet at the declared 50-block cap
    cost on the order of a hundred statements, most of them this probe asking
    the same question about the same relation.
    """
    if not hasattr(db, "bind"):
        return False
    memo = _probe_memo(db)
    if memo is not None and relation_name in memo:
        return memo[relation_name]
    try:
        exists = db.execute(
            text("SELECT to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": relation_name},
        ).scalar()
    except SQLAlchemyError:
        # Not recorded: a driver that raised answered nothing, and a later
        # call in the same request may be made against a working connection.
        return False
    if exists is None:
        return False
    absent = not bool(exists)
    if memo is not None:
        memo[relation_name] = absent
    return absent


def require_relation(db: Session, relation_name: str) -> None:
    """Stop the read before any query when a declared relation is absent."""
    if relation_is_absent(db, relation_name):
        raise ServingContractUnavailable(
            f"required serving relation is not present: {relation_name}"
        )
