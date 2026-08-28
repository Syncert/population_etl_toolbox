"""Atomic publication gate for reconciled USDA NASS releases.

Publication is a status transition, not a copy: the gold relations are views
over reconciled silver, so exposing a release and emitting its glossary
publisher event happen in one place and only after silver reconciliation.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID


class NassPublicationError(RuntimeError):
    """A USDA NASS release has not satisfied the silver publication gate."""


def publish_release(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product_id: str,
    release_watermark: str,
) -> int:
    """Atomically expose one reconciled release through deterministic views."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_nass.dim_dataset_release
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE product_id = %s AND release_watermark = %s
                   AND status IN ('silver_ready', 'published')
                RETURNING product_id
                """,
                (product_id, release_watermark),
            )
            if cursor.fetchone() is None:
                raise NassPublicationError(
                    "USDA NASS release is not reconciled and cannot publish"
                )
            cursor.execute(
                """
                UPDATE control.usda_nass_release
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status IN ('silver_ready', 'published')
                """,
                (str(run_id),),
            )
            if cursor.rowcount != 1:
                raise NassPublicationError(
                    "USDA NASS control release is not silver-ready"
                )
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM gold_nass.crop_observation
                WHERE product_id = %s AND release_watermark = %s
                """,
                (product_id, release_watermark),
            )
            count = int(cursor.fetchone()[0])
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

    emit_latest_publisher_ready(connection_factory, publisher_schema="gold_nass")
    return count
