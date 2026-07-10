"""Shared helpers for subject-scoped gold schema bootstrap."""
from __future__ import annotations

import hashlib
import logging
import pathlib
from datetime import date
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _compute_ddl_hash(ddl_files: list[pathlib.Path]) -> str:
    digest = hashlib.sha256()
    for ddl_file in ddl_files:
        digest.update(str(ddl_file.name).encode("utf-8"))
        digest.update(b"\0")
        digest.update(ddl_file.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _ensure_schema_state_table(cur: Any) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.schema_migration_state (
            component_name TEXT PRIMARY KEY,
            ddl_hash       TEXT NOT NULL,
            applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _get_recorded_hash(cur: Any, component_name: str) -> str | None:
    cur.execute(
        """
        SELECT ddl_hash
        FROM gold.schema_migration_state
        WHERE component_name = %s
        """,
        (component_name,),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def _record_hash(cur: Any, component_name: str, ddl_hash: str) -> None:
    cur.execute(
        """
        INSERT INTO gold.schema_migration_state (component_name, ddl_hash, applied_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (component_name) DO UPDATE
        SET ddl_hash = EXCLUDED.ddl_hash,
            applied_at = EXCLUDED.applied_at
        """,
        (component_name, ddl_hash),
    )


def _is_bootstrapped(cur: Any, required_relations: tuple[str, ...], required_procedures: tuple[str, ...]) -> bool:
    relation_checks = ",\n                ".join(
        f"to_regclass('{relation_name}') IS NOT NULL" for relation_name in required_relations
    )
    procedure_checks = ",\n                ".join(
        f"to_regprocedure('{procedure_name}') IS NOT NULL" for procedure_name in required_procedures
    )
    sql = f"""
        SELECT
            {relation_checks},
            {procedure_checks}
    """
    cur.execute(sql)
    checks = cur.fetchone()
    return bool(checks and all(checks))


def ensure_gold_schema_from_files(
    ddl_files: list[pathlib.Path],
    component_name: str,
    required_relations: tuple[str, ...],
    required_procedures: tuple[str, ...],
    hook: PostgresHook,
) -> None:
    """Apply subject-scoped gold DDL files when required objects/hash are stale."""
    if not ddl_files:
        raise FileNotFoundError("No DDL files were provided for gold bootstrap")

    ordered_files = sorted(ddl_files)
    current_hash = _compute_ddl_hash(ordered_files)

    with hook.get_conn() as conn, conn.cursor() as cur:
        _ensure_schema_state_table(cur)
        recorded_hash = _get_recorded_hash(cur, component_name)
        bootstrapped = _is_bootstrapped(cur, required_relations, required_procedures)

        if bootstrapped and recorded_hash == current_hash:
            logger.info(
                "Gold schema component %s already bootstrapped with matching hash; skipping",
                component_name,
            )
            conn.commit()
            return

        for ddl_file in ordered_files:
            sql = ddl_file.read_text(encoding="utf-8")
            cur.execute("SET LOCAL statement_timeout = 0")
            cur.execute(sql)
            logger.info("Applied gold DDL (%s): %s", component_name, ddl_file)

        _record_hash(cur, component_name, current_hash)
        conn.commit()

    logger.info("Gold schema component %s ensured from %d DDL file(s)", component_name, len(ordered_files))


def build_shard_list(
    window_start: date,
    window_end: date,
    hook: PostgresHook,
) -> list[str]:
        """Return ISO month_start strings from silver_ref.dim_time within the window."""
    sql = """
        SELECT DISTINCT date_trunc('month', date_key)::date AS month_start
                from silver_ref.dim_time
        WHERE date_key >= %s
          AND date_key <= %s
          AND is_month_start = TRUE
        ORDER BY month_start
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (window_start, window_end))
        rows = cur.fetchall()
    return [r[0].isoformat() for r in rows]
