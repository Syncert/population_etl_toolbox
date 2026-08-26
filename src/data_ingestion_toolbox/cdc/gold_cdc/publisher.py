"""Atomic publication gate for reconciled CDC releases."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID


class CdcPublicationError(RuntimeError):
    """A CDC release has not satisfied the silver publication gate."""


def publish_release(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset_id: str,
    release_watermark: str,
) -> int:
    """Atomically expose one reconciled release through deterministic views."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_cdc.dim_dataset_release
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE asset_id = %s AND release_watermark = %s
                   AND status IN ('silver_ready', 'published')
                RETURNING asset_id
                """,
                (asset_id, release_watermark),
            )
            if cursor.fetchone() is None:
                raise CdcPublicationError(
                    "CDC release is not reconciled and cannot publish"
                )
            cursor.execute(
                """
                UPDATE control.cdc_dataset_release
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status IN ('silver_ready', 'published')
                """,
                (str(run_id),),
            )
            if cursor.rowcount != 1:
                raise CdcPublicationError("CDC control release is not silver-ready")
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM gold_cdc.health_observation
                WHERE asset_id = %s AND release_watermark = %s
                """,
                (asset_id, release_watermark),
            )
            count = int(cursor.fetchone()[0])
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

    emit_latest_publisher_ready(connection_factory, publisher_schema="gold_cdc")
    return count
