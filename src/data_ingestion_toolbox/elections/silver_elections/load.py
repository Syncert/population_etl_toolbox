"""Replay one captured elections read into silver, reconcile it, and publish it.

Everything here reads the committed files, never the network. One
transaction per run: the parsed rows, the quarantine, the geography
resolution ledger and the facts land together or not at all. A read the
capture found ``unchanged`` replays nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..client import ElectionsPayloadError, FileRequest, check_body
from ..config import SOURCE_CODE
from ..registry import GOVERNORS, LEGISLATORS
from .parse import (
    CandidateResult,
    Quarantine,
    SeatTerm,
    parse_governors,
    parse_legislators,
    parse_results,
)


class ElectionsReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed row."""


class ElectionsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


_TERM_INSERT = """
INSERT INTO silver_elections.seat_term (
    run_id, term_key, office, state_code, state_fips, district, senate_class, holder_id,
    holder_name, party_text, party_code, caucus_code, start_date, end_date, how, geo_id,
    geo_sk, geography_status, source_record_id, capture_id, record_index
)
SELECT incoming.run_id::UUID, incoming.term_key, incoming.office, incoming.state_code,
       incoming.state_fips, incoming.district::INTEGER, incoming.senate_class::SMALLINT,
       incoming.holder_id, incoming.holder_name, incoming.party_text, incoming.party_code,
       incoming.caucus_code, incoming.start_date::DATE, incoming.end_date::DATE, incoming.how,
       incoming.geo_id, entity.geo_sk,
       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
       incoming.source_record_id, incoming.capture_id::UUID, incoming.record_index::INTEGER
FROM (VALUES %s) AS incoming (
    run_id, term_key, office, state_code, state_fips, district, senate_class, holder_id,
    holder_name, party_text, party_code, caucus_code, start_date, end_date, how, geo_id,
    source_record_id, capture_id, record_index
)
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
ON CONFLICT (run_id, term_key) DO NOTHING
"""

_RESULT_INSERT = """
INSERT INTO silver_elections.candidate_result (
    run_id, result_key, office, election_year, stage, special, state_code, state_fips,
    county_fips, district, candidate, party_text, party_code, writein, mode, candidate_votes,
    total_votes, geo_level, geo_id, geo_sk, geography_status, source_record_id, capture_id,
    record_index
)
SELECT incoming.run_id::UUID, incoming.result_key, incoming.office,
       incoming.election_year::SMALLINT, incoming.stage, incoming.special::BOOLEAN,
       incoming.state_code, incoming.state_fips, incoming.county_fips,
       incoming.district::INTEGER, incoming.candidate, incoming.party_text, incoming.party_code,
       incoming.writein::BOOLEAN, incoming.mode, incoming.candidate_votes::BIGINT,
       incoming.total_votes::BIGINT, incoming.geo_level, incoming.geo_id, entity.geo_sk,
       CASE
           WHEN incoming.geo_level = 'district' THEN 'district'
           WHEN entity.geo_sk IS NULL THEN 'unmapped'
           ELSE 'resolved'
       END,
       incoming.source_record_id, incoming.capture_id::UUID, incoming.record_index::INTEGER
FROM (VALUES %s) AS incoming (
    run_id, result_key, office, election_year, stage, special, state_code, state_fips,
    county_fips, district, candidate, party_text, party_code, writein, mode, candidate_votes,
    total_votes, geo_level, geo_id, source_record_id, capture_id, record_index
)
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
ON CONFLICT (run_id, result_key) DO NOTHING
"""


def _term_row(run_id: UUID, capture_id: str, term: SeatTerm) -> tuple:
    return (
        str(run_id), term.term_key, term.office, term.state_code, term.state_fips,
        term.district, term.senate_class, term.holder_id, term.holder_name,
        term.party_text, term.party_code, term.caucus_code, term.start_date,
        term.end_date, term.how, term.geo_id, term.source_record_id, capture_id,
        term.record_index,
    )  # fmt: skip


def _result_row(run_id: UUID, capture_id: str, result: CandidateResult) -> tuple:
    return (
        str(run_id), result.result_key, result.office, result.election_year,
        result.stage, result.special, result.state_code, result.state_fips,
        result.county_fips, result.district, result.candidate, result.party_text,
        result.party_code, result.writein, result.mode, result.candidate_votes,
        result.total_votes, result.geo_level, result.geo_id, result.source_record_id,
        capture_id, result.record_index,
    )  # fmt: skip


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured read; return the rows written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream, status FROM control.elections_run WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise ElectionsReconciliationError("run has no captured elections read")
            stream, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            cursor.execute(
                """
                SELECT file_index, file_key, file_kind, capture_id::TEXT
                FROM control.elections_file WHERE run_id = %s ORDER BY file_index
                """,
                (str(run_id),),
            )
            files = cursor.fetchall()
            quarantine: list[tuple] = []
            term_rows: list[tuple] = []
            result_rows: list[tuple] = []
            records_total = 0
            refused = False
            term_keys: set[str] = set()
            for _file_index, file_key, file_kind, capture_id in files:
                if file_kind == "listing":
                    continue
                payload = load_captured_payload(connection_factory, UUID(capture_id))
                try:
                    check_body(
                        FileRequest(stream, file_key, "", {}, file_kind, ""), payload
                    )
                except ElectionsPayloadError as error:
                    refused = True
                    quarantine.append(
                        (
                            str(run_id),
                            capture_id,
                            0,
                            error.code,
                            f"file refused: {error.code}",
                        )
                    )
                    continue
                rejected: list[Quarantine]
                if stream == LEGISLATORS:
                    terms, rejected = parse_legislators(payload)
                    records_total += len(terms) + len(rejected)
                    for term in terms:
                        if term.term_key in term_keys:
                            rejected.append(
                                Quarantine(
                                    term.record_index,
                                    "duplicate_row",
                                    "a term span appears twice",
                                )
                            )
                            continue
                        term_keys.add(term.term_key)
                        term_rows.append(_term_row(run_id, capture_id, term))
                elif stream == GOVERNORS:
                    terms, rejected = parse_governors(payload)
                    records_total += len(terms) + len(rejected)
                    term_rows.extend(
                        _term_row(run_id, capture_id, term) for term in terms
                    )
                else:
                    results, rejected, read = parse_results(stream, payload)
                    records_total += read
                    result_rows.extend(
                        _result_row(run_id, capture_id, item) for item in results
                    )
                quarantine.extend(
                    (
                        str(run_id),
                        capture_id,
                        q.record_index,
                        q.error_code,
                        q.error_summary,
                    )
                    for q in rejected
                )
            if quarantine:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_elections.quarantine (
                        run_id, capture_id, record_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, record_index, error_code) DO NOTHING
                    """,
                    quarantine,
                )
            written = 0
            if term_rows:
                execute_values(cursor, _TERM_INSERT, term_rows, page_size=10000)
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_elections.seat_term WHERE run_id = %s",
                    (str(run_id),),
                )
                written = int(cursor.fetchone()[0])
                if written != len(term_rows):
                    raise ElectionsReconciliationError(
                        f"run {run_id} wrote {written} seat terms against {len(term_rows)}"
                    )
            if result_rows:
                execute_values(cursor, _RESULT_INSERT, result_rows, page_size=10000)
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_elections.candidate_result WHERE run_id = %s",
                    (str(run_id),),
                )
                written = int(cursor.fetchone()[0])
                if written != len(result_rows):
                    raise ElectionsReconciliationError(
                        f"run {run_id} wrote {written} results against {len(result_rows)}"
                    )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (resolved.geo_type, resolved.code)
                       %s, %s, resolved.geo_type, resolved.code, NULL, resolved.vintage,
                       resolved.geo_sk,
                       CASE WHEN resolved.geo_sk IS NOT NULL THEN 'exact_code' END,
                       resolved.capture_id,
                       CASE WHEN resolved.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN resolved.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM (
                    SELECT 'state'::TEXT AS geo_type, state_code AS code, geo_sk, capture_id,
                           MAX(EXTRACT(YEAR FROM start_date))::INTEGER AS vintage
                    FROM silver_elections.seat_term WHERE run_id = %s
                    GROUP BY state_code, geo_sk, capture_id
                    UNION ALL
                    SELECT geo_level, COALESCE(county_fips, state_code), geo_sk, capture_id,
                           MAX(election_year)::INTEGER
                    FROM silver_elections.candidate_result
                    WHERE run_id = %s AND geo_level <> 'district'
                    GROUP BY geo_level, COALESCE(county_fips, state_code), geo_sk, capture_id
                ) AS resolved
                ORDER BY resolved.geo_type, resolved.code, resolved.vintage DESC, resolved.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                -- Only a row whose content changed is rewritten
                -- (refresh-churn audit, 2026-11-06).
                WHERE (silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.resolution_method, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.geo_sk, EXCLUDED.resolution_method, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (SOURCE_CODE, f"elections_{stream}", str(run_id), str(run_id)),
            )
            cursor.execute(
                """
                UPDATE control.elections_run
                   SET status = %s, record_count = %s, updated_at = NOW()
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    "quarantined" if refused else "silver_ready",
                    records_total,
                    str(run_id),
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return written


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled read through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.elections_run
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.elections_run WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] in ("captured", "capturing"):
                raise ElectionsPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_elections"
        )
    return published
