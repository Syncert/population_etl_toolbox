"""ARCH-002 independent and provider-extensible glossary contracts."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2 import sql
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    emit_latest_publisher_ready,
    harvest_publisher,
    process_pending_events,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_arbitrary_fourth_source_is_harvested_idempotently(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-020 — publisher discovery needs no provider-specific DDL."""
    token = uuid4().hex[:10].lower()
    schema = f"gold_fixture_{token}"
    source_code = f"FIXTURE_{token.upper()}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cursor.execute(
                sql.SQL(
                    """
                    CREATE VIEW {}.metric_publisher AS
                    SELECT
                        %s::TEXT AS source_code,
                        '1.0'::TEXT AS publisher_contract_version,
                        'metric-1'::TEXT AS source_object_key,
                        'series'::TEXT AS source_object_type,
                        'Fixture metric'::TEXT AS metric_display_name,
                        'widgets'::TEXT AS units,
                        'level'::TEXT AS measure_kind,
                        ARRAY['NATIONAL']::TEXT[] AS valid_geo_grains,
                        ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
                        NULL::TEXT AS aggregation_characteristic,
                        '{{"schema":"fixture"}}'::JSONB AS physical_lineage,
                        'watermark-1'::TEXT AS source_watermark,
                        NULL::UUID AS source_run_id,
                        '2099-01-01T00:00:00Z'::TIMESTAMPTZ AS publication_time,
                        'Fixture provider'::TEXT AS source_name,
                        'fixture'::TEXT AS source_type,
                        'https://example.test/fixture'::TEXT AS reference_url
                    """
                ).format(sql.Identifier(schema)),
                (source_code,),
            )
        writer.commit()
    finally:
        writer.close()

    try:
        publisher = Publisher(schema)
        assert harvest_publisher(postgres_connection_factory, publisher) == 1
        assert harvest_publisher(postgres_connection_factory, publisher) == 0
        assert (
            emit_latest_publisher_ready(
                postgres_connection_factory, publisher_schema=schema
            )
            is not None
        )
        assert (
            emit_latest_publisher_ready(
                postgres_connection_factory, publisher_schema=schema
            )
            is not None
        )
        assert process_pending_events(postgres_connection_factory) == 1

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT source_object_type, source_object_key, units,
                           aggregation_characteristic, freshness_state,
                           publisher_contract_version, source_watermark
                    FROM gold_glossary.dim_metric_catalog
                    WHERE source_code = %s
                    """,
                    (source_code,),
                )
                assert cursor.fetchone() == (
                    "series",
                    "metric-1",
                    "widgets",
                    None,
                    "current",
                    "1.0",
                    "watermark-1",
                )
                cursor.execute(
                    """
                    SELECT count(*), min(status), max(attempt_count)
                    FROM control.publisher_ready_event
                    WHERE source_code = %s
                    """,
                    (source_code,),
                )
                assert cursor.fetchone() == (1, "processed", 1)
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event WHERE source_code = %s",
                    (source_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE source_code = %s",
                    (source_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.publisher_harvest_state WHERE source_code = %s",
                    (source_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.publisher_registry WHERE source_code = %s",
                    (source_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_source_system WHERE source_code = %s",
                    (source_code,),
                )
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )
            cleanup.commit()
        finally:
            cleanup.close()
