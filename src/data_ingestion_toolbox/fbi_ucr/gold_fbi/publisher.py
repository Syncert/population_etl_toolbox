"""Atomic publication gate for reconciled FBI UCR releases."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID


class FbiPublicationError(RuntimeError):
    """An FBI UCR release has not satisfied the silver publication gate."""


def publish_release(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product_id: str,
    release_key: str,
) -> int:
    """Atomically expose one reconciled release through deterministic views."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_fbi.dim_ucr_dataset_release
                   SET status = 'published', published_at = NOW(),
                       updated_at = NOW()
                 WHERE product_id = %s AND release_key = %s
                   AND status IN ('silver_ready', 'published')
                RETURNING product_id
                """,
                (product_id, release_key),
            )
            if cursor.fetchone() is None:
                raise FbiPublicationError(
                    "FBI release is not reconciled and cannot publish"
                )
            cursor.execute(
                """
                UPDATE control.fbi_ucr_release
                   SET status = 'published', published_at = NOW(),
                       updated_at = NOW()
                 WHERE run_id = %s AND status IN ('silver_ready', 'published')
                """,
                (str(run_id),),
            )
            if cursor.rowcount != 1:
                raise FbiPublicationError("FBI control release is not silver-ready")
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM gold_fbi.crime_observation
                WHERE product_id = %s AND release_key = %s
                """,
                (product_id, release_key),
            )
            count = int(cursor.fetchone()[0])
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

    emit_latest_publisher_ready(connection_factory, publisher_schema="gold_fbi")
    return count
