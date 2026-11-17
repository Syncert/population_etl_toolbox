"""Create or update one Airflow connection in place, in one transaction.

``airflow-init`` used to run ``airflow connections delete public_data`` and then
``airflow connections add public_data``. Each CLI call boots Airflow for several
seconds, so on every deploy the connection did not exist for that long, and any
task that opened it then failed with "The conn_id `public_data` isn't defined"
(OFFICE_LAB, 2026-11-05 15:45 UTC: one ``bls_ingest`` batch spent a retry on it).

This writes the row with the existing primary key when the connection exists,
so a running task always reads either the old settings or the new ones.

Usage (inside an Airflow container)::

    python scripts/airflow_upsert_connection.py public_data --conn-type postgres \\
        --host HOST --port 5432 --login USER --password PASS --schema DB
"""

from __future__ import annotations

import argparse
from typing import Any, Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("conn_id")
    parser.add_argument("--conn-type", required=True)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--login", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--schema", default=None)
    return parser.parse_args(argv)


def upsert_connection(session: Any, connection: Any) -> str:
    """Merge ``connection`` over any row with the same ``conn_id``; return the verb."""
    from airflow.models import Connection
    from sqlalchemy import select

    existing_id = session.scalar(
        select(Connection.id).where(Connection.conn_id == connection.conn_id)
    )
    if existing_id is not None:
        connection.id = existing_id
    session.merge(connection)
    session.commit()
    return "updated" if existing_id is not None else "created"


def main(argv: Sequence[str] | None = None) -> int:
    from airflow.models import Connection
    from airflow.utils.session import create_session

    args = parse_args(argv)
    connection = Connection(
        conn_id=args.conn_id,
        conn_type=args.conn_type,
        host=args.host,
        port=args.port,
        login=args.login,
        password=args.password,
        schema=args.schema,
    )
    with create_session() as session:
        verb = upsert_connection(session, connection)
    print(f"Connection {args.conn_id} {verb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
