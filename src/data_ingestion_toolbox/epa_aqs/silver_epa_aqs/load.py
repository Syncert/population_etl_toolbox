"""Replay one captured AirData file into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the parsed rows, the quarantine, the geography
resolution ledger and the monitor facts land together or not at all. A run
the capture found ``unchanged`` replays nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import get_file
from .parse import parse_file


class AqsReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed row."""


class AqsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured file; return the monitor facts written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT year, capture_id::TEXT, status FROM control.epa_aqs_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise AqsReconciliationError("run has no captured AirData file")
            year, capture_id, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            item = get_file(f"annual_conc_by_monitor:{year}")
            parsed = parse_file(
                load_captured_payload(connection_factory, UUID(capture_id)), item=item
            )
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_epa_aqs.monitor_fact (
                        run_id, monitor_id, sample_duration, pollutant_standard, event_type, year,
                        capture_id, source_row_index, measure, parameter_code, poc, site_number, geo_id,
                        geo_sk, geography_status, completeness, certification, observation_count, units,
                        value_source, value, value_status, date_of_last_change, source_record_id
                    )
                    SELECT incoming.run_id::UUID, incoming.monitor_id, incoming.sample_duration,
                           incoming.pollutant_standard, incoming.event_type, incoming.year::INTEGER,
                           incoming.capture_id::UUID, incoming.source_row_index::INTEGER, incoming.measure,
                           incoming.parameter_code, incoming.poc::INTEGER, incoming.site_number,
                           incoming.geo_id, entity.geo_sk,
                           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                           incoming.completeness, incoming.certification,
                           incoming.observation_count::INTEGER, incoming.units, incoming.value_source,
                           incoming.value::NUMERIC, incoming.value_status,
                           incoming.date_of_last_change::DATE, incoming.source_record_id
                    FROM (VALUES %s) AS incoming (
                        run_id, monitor_id, sample_duration, pollutant_standard, event_type, year,
                        capture_id, source_row_index, measure, parameter_code, poc, site_number, geo_id,
                        completeness, certification, observation_count, units, value_source, value,
                        value_status, date_of_last_change, source_record_id
                    )
                    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
                    ON CONFLICT (run_id, monitor_id, sample_duration, pollutant_standard, event_type) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            obs.monitor_id,
                            obs.sample_duration,
                            obs.pollutant_standard,
                            obs.event_type,
                            item.year,
                            capture_id,
                            obs.source_row_index,
                            obs.measure,
                            obs.parameter_code,
                            obs.poc,
                            obs.site_number,
                            obs.geo_id,
                            obs.completeness,
                            obs.certification,
                            obs.observation_count,
                            obs.units,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.date_of_last_change,
                            obs.source_record_id,
                        )
                        for obs in parsed.observations
                    ],
                    page_size=10000,
                )
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_epa_aqs.quarantine (
                        run_id, capture_id, source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            capture_id,
                            q.source_row_index,
                            q.error_code,
                            q.error_summary,
                        )
                        for q in parsed.quarantined
                    ],
                )
            refused = any(q.source_row_index == 0 for q in parsed.quarantined)
            cursor.execute(
                """
                UPDATE control.epa_aqs_file
                   SET row_count = %s, in_scope_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    parsed.row_count,
                    parsed.in_scope_row_count,
                    "quarantined" if refused else "silver_ready",
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (fact.geo_id)
                       %s, 'airdata_annual_conc_by_monitor', 'county',
                       REPLACE(REPLACE(fact.geo_id, 'state:', ''), '|county:', ''), NULL, fact.year,
                       fact.geo_sk,
                       CASE WHEN fact.geo_sk IS NOT NULL THEN 'exact_code' END,
                       fact.capture_id, fact.geography_status,
                       CASE WHEN fact.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_epa_aqs.monitor_fact AS fact
                WHERE fact.run_id = %s
                ORDER BY fact.geo_id, fact.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (SOURCE_CODE, str(run_id)),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_epa_aqs.monitor_fact WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if facts != len(parsed.observations):
                raise AqsReconciliationError(
                    f"run {run_id} wrote {facts} monitor facts against {len(parsed.observations)} parsed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled file through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.epa_aqs_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.epa_aqs_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise AqsPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_epa_aqs")
    return published
