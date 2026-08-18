"""Provider-neutral publisher discovery, harvesting, and durable event handling."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from data_ingestion_toolbox.normalization import sanitize_error_message

PUBLISHER_VIEW = "metric_publisher"
REQUIRED_COLUMNS = (
    "source_code",
    "publisher_contract_version",
    "source_object_key",
    "source_object_type",
    "metric_display_name",
    "units",
    "measure_kind",
    "valid_geo_grains",
    "valid_time_grains",
    "aggregation_characteristic",
    "physical_lineage",
    "source_watermark",
    "source_run_id",
    "publication_time",
    "source_name",
    "source_type",
    "reference_url",
)


@dataclass(frozen=True)
class Publisher:
    schema: str
    view: str = PUBLISHER_VIEW


def discover_publishers(database_connection: Any) -> list[Publisher]:
    """Find any schema that exposes the complete standard publisher view."""
    with database_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, array_agg(column_name ORDER BY ordinal_position)
            FROM information_schema.columns
            WHERE table_name = %s
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            GROUP BY table_schema
            ORDER BY table_schema
            """,
            (PUBLISHER_VIEW,),
        )
        discovered = cursor.fetchall()
    required = set(REQUIRED_COLUMNS)
    return [
        Publisher(schema)
        for schema, columns in discovered
        if required.issubset(set(columns))
    ]


def _publisher_rows(database_connection: Any, publisher: Publisher) -> list[tuple]:
    statement = sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(map(sql.Identifier, REQUIRED_COLUMNS)),
        sql.Identifier(publisher.schema),
        sql.Identifier(publisher.view),
    )
    with database_connection.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def harvest_publisher(
    connection_factory: Callable[[], Any],
    publisher: Publisher,
    *,
    retirement_grace_harvests: int = 2,
) -> int:
    """Harvest one publisher in an isolated, source-locked transaction."""
    if retirement_grace_harvests < 1:
        raise ValueError("retirement grace must be positive")
    database_connection = connection_factory()
    try:
        rows = _publisher_rows(database_connection, publisher)
        if not rows:
            database_connection.rollback()
            return 0
        documents = [dict(zip(REQUIRED_COLUMNS, row)) for row in rows]
        source_codes = {str(item["source_code"]).strip().upper() for item in documents}
        versions = {str(item["publisher_contract_version"]) for item in documents}
        if len(source_codes) != 1 or len(versions) != 1:
            raise ValueError("publisher rows disagree on source or contract version")
        source_code = source_codes.pop()
        contract_version = versions.pop()
        publication_time = max(item["publication_time"] for item in documents)
        source_watermark = max(str(item["source_watermark"]) for item in documents)
        source_run_ids = {
            item["source_run_id"]
            for item in documents
            if item["source_run_id"] is not None
        }
        source_run_id = source_run_ids.pop() if len(source_run_ids) == 1 else None

        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"gold_glossary.harvest:{source_code}",),
            )
            cursor.execute(
                """
                SELECT last_publication_time
                FROM gold_glossary.publisher_harvest_state
                WHERE source_code = %s AND status = 'success'
                """,
                (source_code,),
            )
            prior = cursor.fetchone()
            if prior and prior[0] is not None and prior[0] >= publication_time:
                database_connection.rollback()
                return 0
            first = documents[0]
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_source_system (
                    source_code, source_name, source_type, reference_url
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_code) DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    source_type = EXCLUDED.source_type,
                    reference_url = EXCLUDED.reference_url,
                    last_harvested_at = NOW()
                """,
                (
                    source_code,
                    first["source_name"],
                    first["source_type"],
                    first["reference_url"],
                ),
            )
            cursor.execute(
                """
                INSERT INTO gold_glossary.publisher_registry (
                    source_code, publisher_schema, publisher_view,
                    publisher_contract_version, discovery_status,
                    last_discovered_at, last_error
                ) VALUES (%s, %s, %s, %s, 'active', NOW(), NULL)
                ON CONFLICT (source_code) DO UPDATE SET
                    publisher_schema = EXCLUDED.publisher_schema,
                    publisher_view = EXCLUDED.publisher_view,
                    publisher_contract_version = EXCLUDED.publisher_contract_version,
                    discovery_status = 'active',
                    last_discovered_at = NOW(),
                    last_error = NULL
                """,
                (source_code, publisher.schema, publisher.view, contract_version),
            )
            records = [
                (
                    f"{source_code}:{item['source_object_key']}",
                    source_code,
                    item["source_object_type"],
                    item["source_object_key"],
                    item["metric_display_name"],
                    item["units"],
                    item["measure_kind"],
                    item["valid_geo_grains"] or [],
                    item["valid_time_grains"] or [],
                    item["aggregation_characteristic"],
                    Json(item["physical_lineage"] or {}),
                    contract_version,
                    item["source_watermark"],
                    str(item["source_run_id"]) if item["source_run_id"] else None,
                    item["publication_time"],
                )
                for item in documents
            ]
            execute_values(
                cursor,
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, source_code, source_object_type,
                    source_object_key, metric_display_name, units, measure_kind,
                    valid_geo_grains, valid_time_grains,
                    aggregation_characteristic, physical_lineage,
                    publisher_contract_version, source_watermark,
                    source_run_id, publication_time
                ) VALUES %s
                ON CONFLICT (metric_code) DO UPDATE SET
                    source_object_type = EXCLUDED.source_object_type,
                    source_object_key = EXCLUDED.source_object_key,
                    metric_display_name = EXCLUDED.metric_display_name,
                    units = EXCLUDED.units,
                    measure_kind = EXCLUDED.measure_kind,
                    valid_geo_grains = EXCLUDED.valid_geo_grains,
                    valid_time_grains = EXCLUDED.valid_time_grains,
                    aggregation_characteristic = EXCLUDED.aggregation_characteristic,
                    physical_lineage = EXCLUDED.physical_lineage,
                    publisher_contract_version = EXCLUDED.publisher_contract_version,
                    source_watermark = EXCLUDED.source_watermark,
                    source_run_id = EXCLUDED.source_run_id,
                    publication_time = EXCLUDED.publication_time,
                    harvested_at = NOW(),
                    freshness_state = 'current',
                    missing_harvest_count = 0
                """,
                records,
            )
            keys = [str(item["source_object_key"]) for item in documents]
            cursor.execute(
                """
                UPDATE gold_glossary.dim_metric_catalog
                   SET missing_harvest_count = missing_harvest_count + 1,
                       freshness_state = CASE
                           WHEN missing_harvest_count + 1 >= %s THEN 'retired'
                           ELSE 'stale'
                       END,
                       harvested_at = NOW()
                 WHERE source_code = %s
                   AND NOT (source_object_key = ANY(%s))
                """,
                (retirement_grace_harvests, source_code, keys),
            )
            cursor.execute(
                """
                INSERT INTO gold_glossary.publisher_harvest_state (
                    source_code, publisher_contract_version,
                    last_source_watermark, last_source_run_id,
                    last_publication_time, last_harvest_started_at,
                    last_harvest_completed_at, status, last_error
                ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), 'success', NULL)
                ON CONFLICT (source_code) DO UPDATE SET
                    publisher_contract_version = EXCLUDED.publisher_contract_version,
                    last_source_watermark = EXCLUDED.last_source_watermark,
                    last_source_run_id = EXCLUDED.last_source_run_id,
                    last_publication_time = EXCLUDED.last_publication_time,
                    last_harvest_completed_at = NOW(),
                    status = 'success',
                    last_error = NULL
                """,
                (
                    source_code,
                    contract_version,
                    source_watermark,
                    str(source_run_id) if source_run_id else None,
                    publication_time,
                ),
            )
        database_connection.commit()
        return len(records)
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()


def harvest_all_publishers(connection_factory: Callable[[], Any]) -> dict[str, int | str]:
    """Refresh valid publishers independently; one failure cannot roll back another."""
    discovery_connection = connection_factory()
    try:
        publishers = discover_publishers(discovery_connection)
    finally:
        discovery_connection.close()
    results: dict[str, int | str] = {}
    for publisher in publishers:
        try:
            results[publisher.schema] = harvest_publisher(
                connection_factory, publisher
            )
        except BaseException as error:
            results[publisher.schema] = sanitize_error_message(error)
    return results


def emit_publisher_ready(
    connection_factory: Callable[[], Any],
    *,
    source_code: str,
    publisher_contract_version: str,
    source_watermark: str,
    publication_time: datetime,
    source_run_id: UUID | None = None,
) -> UUID:
    """Append an idempotent durable event after source-gold publication commits."""
    event_id = uuid4()
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.publisher_ready_event (
                    event_id, source_code, publisher_contract_version,
                    source_watermark, source_run_id, publication_time
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    source_code, publisher_contract_version, source_watermark
                ) DO NOTHING
                """,
                (
                    str(event_id),
                    source_code.strip().upper(),
                    publisher_contract_version,
                    source_watermark,
                    str(source_run_id) if source_run_id else None,
                    publication_time,
                ),
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return event_id


def emit_latest_publisher_ready(
    connection_factory: Callable[[], Any], *, publisher_schema: str
) -> UUID | None:
    """Read a source-owned publisher watermark and append its outbox event."""
    database_connection = connection_factory()
    try:
        statement = sql.SQL(
            """
            SELECT source_code, publisher_contract_version, source_watermark,
                   source_run_id, publication_time
            FROM {}.{}
            ORDER BY publication_time DESC, source_watermark DESC
            LIMIT 1
            """
        ).format(sql.Identifier(publisher_schema), sql.Identifier(PUBLISHER_VIEW))
        with database_connection.cursor() as cursor:
            cursor.execute(statement)
            row = cursor.fetchone()
    finally:
        database_connection.close()
    if row is None:
        return None
    return emit_publisher_ready(
        connection_factory,
        source_code=row[0],
        publisher_contract_version=row[1],
        source_watermark=row[2],
        source_run_id=row[3],
        publication_time=row[4],
    )


def process_pending_events(
    connection_factory: Callable[[], Any], *, limit: int = 50
) -> int:
    """Claim pending events and harvest only their named publishers."""
    if limit < 1:
        raise ValueError("event limit must be positive")
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_id, source_code
                FROM control.publisher_ready_event
                WHERE status IN ('pending', 'failed') AND available_at <= NOW()
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (limit,),
            )
            events = cursor.fetchall()
            event_ids = [str(row[0]) for row in events]
            if event_ids:
                cursor.execute(
                    """
                    UPDATE control.publisher_ready_event
                       SET status = 'processing', claimed_at = NOW(),
                           attempt_count = attempt_count + 1
                     WHERE event_id = ANY(%s::UUID[])
                    """,
                    (event_ids,),
                )
        database_connection.commit()
    finally:
        database_connection.close()

    processed = 0
    for event_id, source_code in events:
        lookup = connection_factory()
        try:
            with lookup.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT publisher_schema, publisher_view
                    FROM gold_glossary.publisher_registry
                    WHERE source_code = %s AND discovery_status = 'active'
                    """,
                    (source_code,),
                )
                registry = cursor.fetchone()
        finally:
            lookup.close()
        if registry is None:
            discovery = connection_factory()
            try:
                candidates = discover_publishers(discovery)
            finally:
                discovery.close()
            candidate = next(
                (
                    item
                    for item in candidates
                    if item.schema.lower().endswith(str(source_code).lower())
                ),
                None,
            )
        else:
            candidate = Publisher(registry[0], registry[1])
        event_connection = connection_factory()
        try:
            if candidate is None:
                raise LookupError(f"publisher not found for {source_code}")
            harvest_publisher(connection_factory, candidate)
            with event_connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE control.publisher_ready_event
                       SET status = 'processed', processed_at = NOW(), last_error = NULL
                     WHERE event_id = %s
                    """,
                    (str(event_id),),
                )
            event_connection.commit()
            processed += 1
        except BaseException as error:
            event_connection.rollback()
            with event_connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE control.publisher_ready_event
                       SET status = 'failed', last_error = %s
                     WHERE event_id = %s
                    """,
                    (sanitize_error_message(error), str(event_id)),
                )
            event_connection.commit()
        finally:
            event_connection.close()
    return processed
