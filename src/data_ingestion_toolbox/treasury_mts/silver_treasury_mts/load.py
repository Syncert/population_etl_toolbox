"""Replay a captured Table 9 read into silver, reconcile it, and publish it.

Everything here reads the committed captures, never the network. One
transaction per run: the quarantine, the lines the read changed or added,
the superseded rows and the page counts land together or not at all.

Only lines whose published row changed are written (the churn rule): the
read is staged in a temporary table and compared with the current rows by
(record date, line); a line whose published text is the same is counted,
not rewritten. Treasury's classification ids are kept as read but are not
part of the comparison, so a re-numbering alone rewrites nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from .parse import Quarantined, StatementRow, parse_page, record_id


class TreasuryMtsReconciliationError(RuntimeError):
    """A replayed read does not account for every parsed line."""


class TreasuryMtsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


_COLUMNS = (
    "record_date, line_code, classification_id, parent_id, classification_desc, "
    "sequence_level, sequence_number_cd, src_line_nbr, print_order_nbr, data_type_cd, "
    "record_type_cd, fiscal_year, fiscal_quarter, calendar_month, current_month_source, "
    "current_month_amount, value_source, current_fytd_amount, prior_fytd_source, "
    "prior_fytd_amount, value_status, run_id, capture_id, source_row_index, source_record_id"
)

_STAGE = """
CREATE TEMPORARY TABLE treasury_mts_incoming (
    record_date DATE NOT NULL,
    line_code TEXT NOT NULL,
    classification_id TEXT NOT NULL,
    parent_id TEXT,
    classification_desc TEXT NOT NULL,
    sequence_level INTEGER,
    sequence_number_cd TEXT,
    src_line_nbr INTEGER,
    print_order_nbr TEXT,
    data_type_cd TEXT,
    record_type_cd TEXT,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,
    calendar_month INTEGER NOT NULL,
    current_month_source TEXT NOT NULL,
    current_month_amount NUMERIC,
    value_source TEXT NOT NULL,
    current_fytd_amount NUMERIC,
    prior_fytd_source TEXT NOT NULL,
    prior_fytd_amount NUMERIC,
    value_status TEXT NOT NULL,
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    source_row_index INTEGER NOT NULL,
    source_record_id TEXT NOT NULL,
    PRIMARY KEY (record_date, line_code)
) ON COMMIT DROP
"""

#: The published row, compared field by field (classification ids aside).
_PUBLISHED = (
    "classification_desc, sequence_level, sequence_number_cd, src_line_nbr, print_order_nbr, "
    "data_type_cd, record_type_cd, fiscal_year, fiscal_quarter, calendar_month, "
    "current_month_source, value_source, prior_fytd_source"
)


def _published(alias: str) -> str:
    return (
        "("
        + ", ".join(f"{alias}.{column.strip()}" for column in _PUBLISHED.split(","))
        + ")"
    )


def _row(item: StatementRow, run_id: UUID, capture_id: str) -> tuple[Any, ...]:
    return (
        item.record_date,
        item.line_code,
        item.classification_id,
        item.parent_id,
        item.classification_desc,
        item.sequence_level,
        item.sequence_number_cd,
        item.src_line_nbr,
        item.print_order_nbr,
        item.data_type_cd,
        item.record_type_cd,
        item.fiscal_year,
        item.fiscal_quarter,
        item.calendar_month,
        item.current_month_source,
        item.current_month_amount,
        item.value_source,
        item.current_fytd_amount,
        item.prior_fytd_source,
        item.prior_fytd_amount,
        item.value_status,
        str(run_id),
        capture_id,
        item.source_row_index,
        record_id(item.record_date, item.line_code, capture_id),
    )


def _quarantine(
    cursor: Any, run_id: UUID, capture_id: str, items: list[Quarantined]
) -> None:
    if items:
        execute_values(
            cursor,
            """
            INSERT INTO silver_treasury_mts.quarantine (
                run_id, capture_id, source_row_index, error_code, error_summary
            ) VALUES %s
            ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
            """,
            [
                (
                    str(run_id),
                    capture_id,
                    item.source_row_index,
                    item.error_code,
                    item.error_summary,
                )
                for item in items
            ],
        )


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Replay every captured page of a run; return the statement lines written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT page_number, capture_id::TEXT
                FROM control.treasury_mts_page
                WHERE run_id = %s AND status = 'captured'
                ORDER BY page_number
                """,
                (str(run_id),),
            )
            pages = cursor.fetchall()
            if not pages:
                connection.commit()
                return 0
            cursor.execute(_STAGE)
            staged = 0
            refused_pages: set[int] = set()
            for page_number, capture_id in pages:
                parsed = parse_page(
                    load_captured_payload(connection_factory, UUID(capture_id))
                )
                rows = [_row(item, run_id, capture_id) for item in parsed.rows]
                repeated: list[Quarantined] = []
                if rows:
                    execute_values(
                        cursor,
                        f"INSERT INTO treasury_mts_incoming ({_COLUMNS}) VALUES %s "
                        "ON CONFLICT (record_date, line_code) DO NOTHING",
                        rows,
                    )
                    cursor.execute(
                        "SELECT COUNT(*) FROM treasury_mts_incoming WHERE capture_id = %s",
                        (capture_id,),
                    )
                    landed = int(cursor.fetchone()[0])
                    if landed != len(rows):
                        cursor.execute(
                            "SELECT record_date, line_code FROM treasury_mts_incoming WHERE capture_id = %s",
                            (capture_id,),
                        )
                        landed_keys = {(row[0], row[1]) for row in cursor.fetchall()}
                        repeated = [
                            Quarantined(
                                item.source_row_index,
                                "repeated_line",
                                f"line {item.line_code} on {item.record_date} read from an earlier page",
                            )
                            for item in parsed.rows
                            if (item.record_date, item.line_code) not in landed_keys
                        ]
                    staged += landed
                _quarantine(cursor, run_id, capture_id, parsed.quarantined + repeated)
                if any(item.source_row_index == 0 for item in parsed.quarantined):
                    refused_pages.add(page_number)
            cursor.execute("ANALYZE treasury_mts_incoming")
            cursor.execute(
                f"""
                INSERT INTO silver_treasury_mts.statement_line_revision (
                    {_COLUMNS}, created_at, superseded_by_run_id
                )
                SELECT {", ".join("current." + column.strip() for column in _COLUMNS.split(","))},
                       current.created_at, %(run_id)s
                FROM silver_treasury_mts.statement_line AS current
                JOIN treasury_mts_incoming AS incoming USING (record_date, line_code)
                WHERE {_published("current")} IS DISTINCT FROM {_published("incoming")}
                ON CONFLICT DO NOTHING
                """,
                {"run_id": str(run_id)},
            )
            cursor.execute(
                f"""
                INSERT INTO silver_treasury_mts.statement_line ({_COLUMNS})
                SELECT {", ".join("incoming." + column.strip() for column in _COLUMNS.split(","))}
                FROM treasury_mts_incoming AS incoming
                LEFT JOIN silver_treasury_mts.statement_line AS current USING (record_date, line_code)
                WHERE current.record_date IS NULL
                   OR {_published("current")} IS DISTINCT FROM {_published("incoming")}
                ON CONFLICT (record_date, line_code) DO UPDATE SET
                    {", ".join(f"{column.strip()} = EXCLUDED.{column.strip()}" for column in _COLUMNS.split(",")[2:])},
                    created_at = NOW()
                RETURNING capture_id::TEXT
                """
            )
            written_by_capture: dict[str, int] = {}
            for (capture_id,) in cursor.fetchall():
                written_by_capture[capture_id] = (
                    written_by_capture.get(capture_id, 0) + 1
                )
            # Every staged line is now current with its published text.
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM treasury_mts_incoming AS incoming
                JOIN silver_treasury_mts.statement_line AS current USING (record_date, line_code)
                WHERE {_published("current")} IS NOT DISTINCT FROM {_published("incoming")}
                """
            )
            present = int(cursor.fetchone()[0])
            if present != staged:
                raise TreasuryMtsReconciliationError(
                    f"run {run_id}: {present} of {staged} staged lines are current after the replay"
                )
            for page_number, capture_id in pages:
                cursor.execute(
                    """
                    UPDATE control.treasury_mts_page
                       SET written_count = %s, status = %s, updated_at = NOW()
                     WHERE run_id = %s AND page_number = %s AND status = 'captured'
                    """,
                    (
                        written_by_capture.get(capture_id, 0),
                        "quarantined"
                        if page_number in refused_pages
                        else "silver_ready",
                        str(run_id),
                        page_number,
                    ),
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return sum(written_by_capture.values())


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a run's reconciled pages through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.treasury_mts_page
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING page_number
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT COUNT(*) FROM control.treasury_mts_page WHERE run_id = %s AND status = 'captured'",
                (str(run_id),),
            )
            if int(cursor.fetchone()[0]):
                raise TreasuryMtsPublicationError(
                    f"run {run_id} has pages never replayed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_treasury_mts"
        )
    return published


def settle_unreplayed_runs(connection_factory: Callable[[], Any]) -> list[UUID]:
    """Finish, oldest first, every run an earlier attempt captured but never replayed."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id::TEXT, MIN(created_at)
                  FROM control.treasury_mts_page
                 WHERE status = 'captured'
                 GROUP BY run_id
                 ORDER BY MIN(created_at), run_id
                """
            )
            runs = [UUID(row[0]) for row in cursor.fetchall()]
        reader.commit()
    finally:
        reader.close()
    for run_id in runs:
        replay_run(connection_factory, run_id=run_id)
        publish_run(connection_factory, run_id=run_id)
    return runs
