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


def relation_is_absent(db: Session, relation_name: str) -> bool:
    """True only when the database positively reports the relation missing.

    A session that cannot answer the question -- a stub in a deterministic unit
    test, or a driver that raises -- is not evidence of absence, so the check
    stays silent rather than inventing a deployment fault from a test double.
    """
    if not hasattr(db, "bind"):
        return False
    try:
        exists = db.execute(
            text("SELECT to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": relation_name},
        ).scalar()
    except SQLAlchemyError:
        return False
    if exists is None:
        return False
    return not bool(exists)


def require_relation(db: Session, relation_name: str) -> None:
    """Stop the read before any query when a declared relation is absent."""
    if relation_is_absent(db, relation_name):
        raise ServingContractUnavailable(
            f"required serving relation is not present: {relation_name}"
        )
