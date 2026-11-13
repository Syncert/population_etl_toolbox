"""Rebuild the source rows of Return A years loaded before they were recorded.

Return A years loaded before ``silver_fbi.return_a_source_row`` existed have
facts naming ``(capture_id, source_row_index)`` with nothing behind them in
silver (2015-2025, loaded 2026-10-31: about 20 million facts). Their bytes are
still in ``raw_capture``, so each such year is re-read from its capture and
its lines recorded, exactly as a load records them now.

It runs at the start of every scheduled ``fbi_ucr_ingest`` run (LOADER RULE:
no manual history run). A year whose capture already has source rows is not
read at all, so once the gap is closed a run costs one index probe per loaded
year. Rows are only ever inserted, never rewritten, and no fact is touched
(CHURN RULE). A Return A load holding its advisory lock defers the work to
the next run rather than waiting on it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any
from uuid import UUID

from data_ingestion_toolbox.capture import load_captured_payload

from .fetch import unzip_text
from .layout import iter_records
from .load import LOCK_KEY, _agencies, _write_source_rows, line_checksums

logger = logging.getLogger(__name__)


def years_missing_source_rows(cursor: Any) -> list[tuple[UUID, str, int, UUID]]:
    """Loaded years whose facts exist but whose capture has no source row."""
    cursor.execute(
        """
        SELECT loaded.capture_id, loaded.release_key, loaded.data_year,
               capture.run_id
        FROM control.fbi_return_a_year AS loaded
        JOIN raw_capture.response_capture AS capture
          ON capture.capture_id = loaded.capture_id
        WHERE NOT EXISTS (
                  SELECT 1 FROM silver_fbi.return_a_source_row AS source_row
                  WHERE source_row.capture_id = loaded.capture_id)
          AND (EXISTS (
                  SELECT 1 FROM silver_fbi.fact_crime_observation AS fact
                  WHERE fact.capture_id = loaded.capture_id)
               OR EXISTS (
                  SELECT 1 FROM silver_fbi.fact_reporting_participation AS fact
                  WHERE fact.capture_id = loaded.capture_id))
        ORDER BY loaded.data_year DESC, loaded.release_key
        """
    )
    return [
        (UUID(str(capture_id)), release_key, int(year), UUID(str(run_id)))
        for capture_id, release_key, year, run_id in cursor.fetchall()
    ]


def backfill_source_rows(connection_factory: Callable[[], Any]) -> dict[str, Any]:
    """Record the source rows of every loaded year that lacks them."""
    summary: dict[str, Any] = {"status": "done", "years": [], "inserted": 0}
    database = connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
            if not cursor.fetchone()[0]:
                database.rollback()
                logger.info("a Return A load is running; source rows wait a run")
                return {**summary, "status": "deferred"}
        try:
            with database.cursor() as cursor:
                pending = years_missing_source_rows(cursor)
            database.commit()
            for capture_id, release_key, year, run_id in pending:
                text = unzip_text(load_captured_payload(connection_factory, capture_id))
                agencies = _agencies(iter_records(text))
                with database.cursor() as cursor:
                    inserted = _write_source_rows(
                        cursor,
                        run_id=run_id,
                        release_key=release_key,
                        year=year,
                        capture_id=capture_id,
                        agencies=agencies,
                        checksums=line_checksums(
                            text, (record.line_number for record in agencies)
                        ),
                    )
                database.commit()
                logger.info(
                    "Return A %s (%s): %s source rows recorded",
                    year,
                    capture_id,
                    inserted,
                )
                summary["years"].append(
                    {
                        "data_year": year,
                        "release_key": release_key,
                        "capture_id": str(capture_id),
                        "inserted": inserted,
                    }
                )
                summary["inserted"] += inserted
            if summary["inserted"]:
                # The planner must see the new rows before the next anti-join.
                with database.cursor() as cursor:
                    cursor.execute("ANALYZE silver_fbi.return_a_source_row")
                database.commit()
        finally:
            # Every year above committed or failed; the lock is held by the
            # session, so it outlives this rollback until the unlock.
            database.rollback()
            with database.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
            database.commit()
    except BaseException:
        database.rollback()
        raise
    finally:
        database.close()
    return summary
