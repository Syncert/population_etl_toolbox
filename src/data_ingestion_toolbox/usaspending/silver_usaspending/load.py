"""Replay captured USAspending answers into silver, reconcile, and publish.

Everything here reads the committed captures, never the network. One
transaction per request: an answer stands alone for its fiscal year, layer
and cut, so its quarantine, the rows it changed, added or withdrew and its
ledger status land together, and an interrupted replay resumes at the next
unreplayed request.

Only rows whose published text changed are written (the churn rule): the
answer is staged and compared with the current rows of the same key by
shape code. An unchanged row is counted, not rewritten; a changed one moves
to the revision table first; a place a later answer no longer lists is
moved there as ``withdrawn`` and removed from the current table.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..registry import geo_id_of
from .parse import ParsedAnswer, parse_agencies, parse_geography, record_id


class UsaspendingReconciliationError(RuntimeError):
    """A replayed answer does not account for every row it carried."""


class UsaspendingPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


_COLUMNS = (
    "fiscal_year, geo_layer, cut_kind, cut_key, shape_code, display_name, value_source, "
    "amount, value_status, population_source, geo_id, geography_status, run_id, capture_id, "
    "source_row_index, source_record_id"
)

_STAGE = """
CREATE TEMPORARY TABLE usaspending_incoming (
    fiscal_year INTEGER NOT NULL,
    geo_layer TEXT NOT NULL,
    cut_kind TEXT NOT NULL,
    cut_key TEXT NOT NULL,
    shape_code TEXT NOT NULL,
    display_name TEXT,
    value_source TEXT NOT NULL,
    amount NUMERIC,
    value_status TEXT NOT NULL,
    population_source TEXT,
    geo_id TEXT,
    geography_status TEXT NOT NULL,
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    source_row_index INTEGER NOT NULL,
    source_record_id TEXT NOT NULL,
    PRIMARY KEY (shape_code)
) ON COMMIT DROP
"""

_KEY = "fiscal_year = %(fy)s AND geo_layer = %(layer)s AND cut_kind = %(kind)s AND cut_key = %(cut)s"

#: The published row, compared field by field.
_CHANGED = (
    "current.display_name IS DISTINCT FROM incoming.display_name "
    "OR current.value_source IS DISTINCT FROM incoming.value_source "
    "OR current.population_source IS DISTINCT FROM incoming.population_source"
)

_REVISION_COLUMNS = (
    "fiscal_year, geo_layer, cut_kind, cut_key, shape_code, display_name, value_source, amount, "
    "value_status, population_source, geo_id, geography_status, run_id, capture_id, "
    "source_row_index, source_record_id, created_at"
)


def _quarantine(
    cursor: Any, run_id: UUID, capture_id: str, parsed: ParsedAnswer
) -> None:
    if not parsed.quarantined:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_usaspending.quarantine (
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
            for item in parsed.quarantined
        ],
    )


def _replay_agencies(
    cursor: Any, run_id: UUID, capture_id: str, fiscal_year: int, parsed: ParsedAnswer
) -> int:
    if not parsed.rows:
        return 0
    written = execute_values(
        cursor,
        """
        INSERT INTO silver_usaspending.agency_year AS current (
            fiscal_year, agency_code, agency_name, agency_id, value_source, amount,
            run_id, capture_id, source_row_index
        ) VALUES %s
        ON CONFLICT (fiscal_year, agency_code) DO UPDATE
           SET agency_name = EXCLUDED.agency_name, agency_id = EXCLUDED.agency_id,
               value_source = EXCLUDED.value_source, amount = EXCLUDED.amount,
               run_id = EXCLUDED.run_id, capture_id = EXCLUDED.capture_id,
               source_row_index = EXCLUDED.source_row_index, created_at = NOW()
         WHERE current.agency_name IS DISTINCT FROM EXCLUDED.agency_name
            OR current.value_source IS DISTINCT FROM EXCLUDED.value_source
        RETURNING 1
        """,
        [
            (
                fiscal_year,
                row.agency_code,
                row.agency_name,
                row.agency_id,
                row.value_source,
                row.amount,
                str(run_id),
                capture_id,
                row.source_row_index,
            )
            for row in parsed.rows
        ],
        fetch=True,
        page_size=1000,
    )
    return len(written)


def _replay_geography(
    cursor: Any,
    run_id: UUID,
    capture_id: str,
    request: dict[str, Any],
    parsed: ParsedAnswer,
) -> int:
    key = {
        "fy": request["fiscal_year"],
        "layer": request["geo_layer"],
        "kind": request["cut_kind"],
        "cut": request["cut_key"],
        "run": str(run_id),
    }
    cursor.execute(_STAGE)
    if parsed.rows:
        rows = []
        for row in parsed.rows:
            geo_id = geo_id_of(request["geo_layer"], row.shape_code)
            rows.append(
                (
                    request["fiscal_year"],
                    request["geo_layer"],
                    request["cut_kind"],
                    request["cut_key"],
                    row.shape_code,
                    row.display_name,
                    row.value_source,
                    row.amount,
                    row.value_status,
                    row.population_source,
                    geo_id,
                    "resolved" if geo_id else "unsupported",
                    str(run_id),
                    capture_id,
                    row.source_row_index,
                    record_id(
                        request["fiscal_year"],
                        request["geo_layer"],
                        request["cut_kind"],
                        request["cut_key"],
                        row.shape_code,
                        capture_id,
                    ),
                )
            )
        execute_values(
            cursor, f"INSERT INTO usaspending_incoming ({_COLUMNS}) VALUES %s", rows
        )
    # A refused answer (row 0) withdraws nothing: it says nothing about the key.
    refused = any(item.source_row_index == 0 for item in parsed.quarantined)
    if refused:
        return 0
    cursor.execute(
        f"""
        WITH moved AS (
            INSERT INTO silver_usaspending.geography_amount_revision (
                {_REVISION_COLUMNS}, change_kind, superseded_by_run_id
            )
            SELECT {", ".join("current." + column.strip() for column in _REVISION_COLUMNS.split(","))},
                   CASE WHEN incoming.shape_code IS NULL THEN 'withdrawn' ELSE 'replaced' END,
                   %(run)s
              FROM silver_usaspending.geography_amount AS current
              LEFT JOIN usaspending_incoming AS incoming ON incoming.shape_code = current.shape_code
             WHERE current.{_KEY.replace(" AND ", " AND current.")}
               AND current.run_id <> %(run)s
               AND (incoming.shape_code IS NULL OR {_CHANGED})
            ON CONFLICT DO NOTHING
            RETURNING shape_code, change_kind
        )
        DELETE FROM silver_usaspending.geography_amount AS current
         USING moved
         WHERE current.{_KEY.replace(" AND ", " AND current.")}
           AND current.shape_code = moved.shape_code
           AND moved.change_kind = 'withdrawn'
        RETURNING 1
        """,
        key,
    )
    withdrawn = cursor.rowcount
    cursor.execute(
        f"""
        INSERT INTO silver_usaspending.geography_amount AS current ({_COLUMNS})
        SELECT {_COLUMNS} FROM usaspending_incoming AS incoming
        ON CONFLICT (fiscal_year, geo_layer, cut_kind, cut_key, shape_code) DO UPDATE
           SET display_name = EXCLUDED.display_name, value_source = EXCLUDED.value_source,
               amount = EXCLUDED.amount, value_status = EXCLUDED.value_status,
               population_source = EXCLUDED.population_source, geo_id = EXCLUDED.geo_id,
               geography_status = EXCLUDED.geography_status, run_id = EXCLUDED.run_id,
               capture_id = EXCLUDED.capture_id, source_row_index = EXCLUDED.source_row_index,
               source_record_id = EXCLUDED.source_record_id, created_at = NOW()
         WHERE current.display_name IS DISTINCT FROM EXCLUDED.display_name
            OR current.value_source IS DISTINCT FROM EXCLUDED.value_source
            OR current.population_source IS DISTINCT FROM EXCLUDED.population_source
        RETURNING 1
        """
    )
    return withdrawn + cursor.rowcount


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Replay every captured request of a run; return rows written."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT fiscal_year, geo_layer, cut_kind, cut_key, capture_id::TEXT, row_count
                  FROM control.usaspending_request
                 WHERE run_id = %s AND status = 'captured'
                 ORDER BY fiscal_year, cut_kind = 'agency_list' DESC, geo_layer, cut_kind, cut_key
                """,
                (str(run_id),),
            )
            columns = [column[0] for column in cursor.description]
            requests = [
                dict(zip(columns, row, strict=True)) for row in cursor.fetchall()
            ]
        reader.commit()
    finally:
        reader.close()
    written_total = 0
    for request in requests:
        payload = load_captured_payload(connection_factory, UUID(request["capture_id"]))
        if request["cut_kind"] == "agency_list":
            parsed = parse_agencies(payload)
        else:
            parsed = parse_geography(payload, geo_layer=request["geo_layer"])
        accounted = len(parsed.rows) + sum(
            1 for item in parsed.quarantined if item.source_row_index > 0
        )
        refused = any(item.source_row_index == 0 for item in parsed.quarantined)
        if not refused and accounted != parsed.row_count:
            raise UsaspendingReconciliationError(
                f"capture {request['capture_id']} parsed {parsed.row_count} rows, "
                f"accounted for {accounted}"
            )
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                _quarantine(cursor, run_id, request["capture_id"], parsed)
                if request["cut_kind"] == "agency_list":
                    written = _replay_agencies(
                        cursor,
                        run_id,
                        request["capture_id"],
                        request["fiscal_year"],
                        parsed,
                    )
                else:
                    written = _replay_geography(
                        cursor, run_id, request["capture_id"], request, parsed
                    )
                cursor.execute(
                    """
                    UPDATE control.usaspending_request
                       SET status = %s, written_count = %s, updated_at = NOW()
                     WHERE run_id = %s AND fiscal_year = %s AND geo_layer = %s
                       AND cut_kind = %s AND cut_key = %s
                    """,
                    (
                        "quarantined" if refused else "silver_ready",
                        written,
                        str(run_id),
                        request["fiscal_year"],
                        request["geo_layer"],
                        request["cut_kind"],
                        request["cut_key"],
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        written_total += written
    return written_total


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a run's reconciled requests through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM control.usaspending_request WHERE run_id = %s AND status = 'captured'",
                (str(run_id),),
            )
            if int(cursor.fetchone()[0]):
                raise UsaspendingPublicationError(
                    f"run {run_id} has requests never replayed"
                )
            cursor.execute(
                """
                UPDATE control.usaspending_request
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING 1
                """,
                (str(run_id),),
            )
            published = cursor.rowcount
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_usaspending"
        )
    return published


def settle_unreplayed_runs(connection_factory: Callable[[], Any]) -> list[UUID]:
    """Finish, oldest first, every run an earlier attempt captured but never published."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id::TEXT, MIN(created_at)
                  FROM control.usaspending_request
                 WHERE status IN ('captured', 'silver_ready')
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
