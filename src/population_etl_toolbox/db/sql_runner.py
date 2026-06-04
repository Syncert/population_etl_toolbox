from __future__ import annotations

from pathlib import Path

import psycopg2

from .connection import PostgresConnectionFactory


def run_sql_file(path: str | Path, *, env_prefix: str = "POSTGRES_") -> None:
    """Execute a SQL file against the configured Postgres database."""
    sql_path = Path(path)
    sql = sql_path.read_text(encoding="utf-8")
    conn_details = PostgresConnectionFactory.from_env(prefix=env_prefix)
    with psycopg2.connect(**conn_details.psycopg_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
