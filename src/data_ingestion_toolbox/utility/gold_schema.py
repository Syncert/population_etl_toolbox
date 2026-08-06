"""Shared helpers for subject-scoped gold schema bootstrap."""

from __future__ import annotations

import hashlib
import logging
import pathlib
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServingRefreshChunkConfig:
    """Trusted SQL configuration for a source-specific annual serving refresh."""

    source_code: str
    log_label: str
    report_table: str
    report_date_column: str
    changed_chunks_sql: str
    report_procedure: str
    latest_procedure: str
    statement_timeout: str


def _compute_ddl_hash(ddl_files: list[pathlib.Path]) -> str:
    digest = hashlib.sha256()
    for ddl_file in ddl_files:
        digest.update(str(ddl_file.name).encode("utf-8"))
        digest.update(b"\0")
        digest.update(ddl_file.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _ensure_schema_state_table(cur: Any) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold_glossary")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gold_glossary.schema_migration_state (
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
        FROM gold_glossary.schema_migration_state
        WHERE component_name = %s
        """,
        (component_name,),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def _record_hash(cur: Any, component_name: str, ddl_hash: str) -> None:
    cur.execute(
        """
        INSERT INTO gold_glossary.schema_migration_state (component_name, ddl_hash, applied_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (component_name) DO UPDATE
        SET ddl_hash = EXCLUDED.ddl_hash,
            applied_at = EXCLUDED.applied_at
        """,
        (component_name, ddl_hash),
    )


def _is_bootstrapped(
    cur: Any, required_relations: tuple[str, ...], required_procedures: tuple[str, ...]
) -> bool:
    relation_checks = ",\n                ".join(
        f"to_regclass('{relation_name}') IS NOT NULL"
        for relation_name in required_relations
    )
    procedure_checks = ",\n                ".join(
        f"to_regprocedure('{procedure_name}') IS NOT NULL"
        for procedure_name in required_procedures
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
            cur.execute("SET LOCAL lock_timeout = '30s'")
            cur.execute("SET LOCAL statement_timeout = '120min'")
            cur.execute(sql)
            logger.info("Applied gold DDL (%s): %s", component_name, ddl_file)

        _record_hash(cur, component_name, current_hash)
        conn.commit()

    logger.info(
        "Gold schema component %s ensured from %d DDL file(s)",
        component_name,
        len(ordered_files),
    )


def build_shard_list(
    window_start: date,
    window_end: date,
    hook: PostgresHook,
) -> list[str]:
    """Return ISO month_start strings from silver_ref.dim_time within the window."""
    sql = """
        SELECT DISTINCT date_trunc('month', date_key)::date AS month_start
        FROM silver_ref.dim_time
        WHERE date_key >= %s
          AND date_key <= %s
          AND is_month_start = TRUE
        ORDER BY month_start
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (window_start, window_end))
        rows = cur.fetchall()
    return [r[0].isoformat() for r in rows]


def refresh_serving_layer_in_year_chunks(
    *,
    hook: PostgresHook,
    config: ServingRefreshChunkConfig,
    task_logger: logging.Logger | None = None,
) -> dict[str, int]:
    """Refresh changed calendar years with a durable checkpoint per year.

    Each report/latest pair is committed independently. If a later chunk fails,
    an Airflow retry replans from the unchanged source watermark and skips chunks
    whose completed watermark already covers the planned target.
    """
    log = task_logger or logger
    refresh_started = time.monotonic()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '30s'")
        cur.execute("SET LOCAL statement_timeout = '10min'")
        cur.execute(
            f"""
            INSERT INTO gold_glossary.serving_refresh_state (
                source_code,
                last_silver_ingested_at,
                last_refresh_completed_at
            )
            SELECT
                %s,
                COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
                CASE WHEN COUNT(*) > 0 THEN NOW() ELSE NULL END
            FROM {config.report_table} r
            ON CONFLICT (source_code) DO NOTHING
            """,
            (config.source_code,),
        )
        cur.execute(
            """
            SELECT last_silver_ingested_at
            FROM gold_glossary.serving_refresh_state
            WHERE source_code = %s
            FOR UPDATE
            """,
            (config.source_code,),
        )
        watermark = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE gold_glossary.serving_refresh_state
            SET last_refresh_started_at = clock_timestamp(),
                updated_at = NOW()
            WHERE source_code = %s
            """,
            (config.source_code,),
        )
        cur.execute(config.changed_chunks_sql, (watermark,))
        changed_chunks = cur.fetchall()

        planned_chunks: list[dict[str, Any]] = []
        for chunk_start, chunk_end, target_watermark in changed_chunks:
            cur.execute(
                """
                INSERT INTO gold_glossary.serving_refresh_chunk_state (
                    source_code,
                    chunk_start,
                    chunk_end,
                    target_silver_ingested_at,
                    status,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, 'PENDING', NOW())
                ON CONFLICT (source_code, chunk_start, chunk_end) DO UPDATE
                SET target_silver_ingested_at = GREATEST(
                        gold_glossary.serving_refresh_chunk_state.target_silver_ingested_at,
                        EXCLUDED.target_silver_ingested_at
                    ),
                    status = CASE
                        WHEN COALESCE(
                            gold_glossary.serving_refresh_chunk_state.completed_silver_ingested_at,
                            '-infinity'::TIMESTAMPTZ
                        ) >= GREATEST(
                            gold_glossary.serving_refresh_chunk_state.target_silver_ingested_at,
                            EXCLUDED.target_silver_ingested_at
                        ) THEN 'COMPLETE'
                        ELSE 'PENDING'
                    END,
                    last_error = CASE
                        WHEN COALESCE(
                            gold_glossary.serving_refresh_chunk_state.completed_silver_ingested_at,
                            '-infinity'::TIMESTAMPTZ
                        ) >= EXCLUDED.target_silver_ingested_at
                        THEN gold_glossary.serving_refresh_chunk_state.last_error
                        ELSE NULL
                    END,
                    updated_at = NOW()
                RETURNING
                    target_silver_ingested_at,
                    completed_silver_ingested_at,
                    status
                """,
                (
                    config.source_code,
                    chunk_start,
                    chunk_end,
                    target_watermark,
                ),
            )
            target, completed, status = cur.fetchone()
            planned_chunks.append(
                {
                    "start": chunk_start,
                    "end": chunk_end,
                    "target": target,
                    "completed": completed,
                    "status": status,
                }
            )
        conn.commit()

    if not planned_chunks:
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE gold_glossary.serving_refresh_state
                SET last_refresh_completed_at = clock_timestamp(),
                    updated_at = NOW()
                WHERE source_code = %s
                """,
                (config.source_code,),
            )
            conn.commit()
        log.info(
            "[%s SERVING REFRESH] no changed Silver rows after watermark=%s",
            config.log_label,
            watermark,
        )
        return {"planned": 0, "completed": 0, "skipped": 0}

    log.info(
        "[%s SERVING REFRESH] planned_chunks=%d watermark=%s window_start=%s window_end=%s",
        config.log_label,
        len(planned_chunks),
        watermark,
        planned_chunks[0]["start"],
        planned_chunks[-1]["end"],
    )

    completed_count = 0
    skipped_count = 0
    for position, chunk in enumerate(planned_chunks, start=1):
        completed_watermark = chunk["completed"]
        if completed_watermark is not None and completed_watermark >= chunk["target"]:
            skipped_count += 1
            log.info(
                "[%s SERVING REFRESH] chunk=%d/%d start=%s end=%s status=SKIPPED checkpoint=%s target=%s",
                config.log_label,
                position,
                len(planned_chunks),
                chunk["start"],
                chunk["end"],
                completed_watermark,
                chunk["target"],
            )
            continue

        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE gold_glossary.serving_refresh_chunk_state
                SET status = 'RUNNING',
                    attempt_count = attempt_count + 1,
                    last_refresh_started_at = clock_timestamp(),
                    last_error = NULL,
                    updated_at = NOW()
                WHERE source_code = %s
                  AND chunk_start = %s
                  AND chunk_end = %s
                """,
                (config.source_code, chunk["start"], chunk["end"]),
            )
            conn.commit()

        chunk_started = time.monotonic()
        log.info(
            "[%s SERVING REFRESH] chunk=%d/%d start=%s end=%s status=STARTED target=%s",
            config.log_label,
            position,
            len(planned_chunks),
            chunk["start"],
            chunk["end"],
            chunk["target"],
        )
        try:
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '30s'")
                cur.execute(
                    f"SET LOCAL statement_timeout = '{config.statement_timeout}'"
                )
                cur.execute(
                    f"CALL {config.report_procedure}(%s, %s)",
                    (chunk["start"], chunk["end"]),
                )
                cur.execute(
                    f"CALL {config.latest_procedure}(%s, %s)",
                    (chunk["start"], chunk["end"]),
                )
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {config.report_table}
                    WHERE {config.report_date_column} >= %s
                      AND {config.report_date_column} <= %s
                    """,
                    (chunk["start"], chunk["end"]),
                )
                report_rows = int(cur.fetchone()[0])
                cur.execute(
                    """
                    UPDATE gold_glossary.serving_refresh_chunk_state
                    SET completed_silver_ingested_at = %s,
                        status = 'COMPLETE',
                        last_refresh_completed_at = clock_timestamp(),
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE source_code = %s
                      AND chunk_start = %s
                      AND chunk_end = %s
                    """,
                    (
                        chunk["target"],
                        config.source_code,
                        chunk["start"],
                        chunk["end"],
                    ),
                )
                conn.commit()
        except Exception as exc:
            error_text = str(exc)[:4000]
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gold_glossary.serving_refresh_chunk_state
                    SET status = 'FAILED',
                        last_error = %s,
                        updated_at = NOW()
                    WHERE source_code = %s
                      AND chunk_start = %s
                      AND chunk_end = %s
                    """,
                    (
                        error_text,
                        config.source_code,
                        chunk["start"],
                        chunk["end"],
                    ),
                )
                conn.commit()
            log.exception(
                "[%s SERVING REFRESH] chunk=%d/%d start=%s end=%s status=FAILED duration_seconds=%.2f",
                config.log_label,
                position,
                len(planned_chunks),
                chunk["start"],
                chunk["end"],
                time.monotonic() - chunk_started,
            )
            raise

        completed_count += 1
        log.info(
            "[%s SERVING REFRESH] chunk=%d/%d start=%s end=%s status=COMPLETE report_rows=%d duration_seconds=%.2f",
            config.log_label,
            position,
            len(planned_chunks),
            chunk["start"],
            chunk["end"],
            report_rows,
            time.monotonic() - chunk_started,
        )

    final_watermark = max(chunk["target"] for chunk in planned_chunks)
    window_start = min(chunk["start"] for chunk in planned_chunks)
    window_end = max(chunk["end"] for chunk in planned_chunks)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM gold_glossary.serving_refresh_chunk_state c
            WHERE c.source_code = %s
              AND (c.chunk_start, c.chunk_end) IN (
                  SELECT * FROM UNNEST(%s::DATE[], %s::DATE[])
              )
              AND COALESCE(
                    c.completed_silver_ingested_at,
                    '-infinity'::TIMESTAMPTZ
                  ) >= c.target_silver_ingested_at
            """,
            (
                config.source_code,
                [chunk["start"] for chunk in planned_chunks],
                [chunk["end"] for chunk in planned_chunks],
            ),
        )
        durable_complete_count = int(cur.fetchone()[0])
        if durable_complete_count != len(planned_chunks):
            raise RuntimeError(
                f"{config.log_label} serving refresh cannot advance its watermark: "
                f"{durable_complete_count}/{len(planned_chunks)} chunks are complete"
            )
        cur.execute(
            """
            UPDATE gold_glossary.serving_refresh_state
            SET last_silver_ingested_at = GREATEST(
                    last_silver_ingested_at,
                    %s
                ),
                last_refresh_completed_at = clock_timestamp(),
                last_window_start = %s,
                last_window_end = %s,
                updated_at = NOW()
            WHERE source_code = %s
            """,
            (final_watermark, window_start, window_end, config.source_code),
        )
        conn.commit()

    log.info(
        "[%s SERVING REFRESH] status=COMPLETE planned=%d refreshed=%d skipped=%d watermark=%s duration_seconds=%.2f",
        config.log_label,
        len(planned_chunks),
        completed_count,
        skipped_count,
        final_watermark,
        time.monotonic() - refresh_started,
    )
    return {
        "planned": len(planned_chunks),
        "completed": completed_count,
        "skipped": skipped_count,
    }
