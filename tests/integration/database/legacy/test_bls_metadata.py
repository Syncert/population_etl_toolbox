"""Bounded live BLS metadata contract using the disposable database."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls import metadata

pytestmark = [
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.external,
    pytest.mark.slow,
]


def test_live_bls_metadata_sync_retains_program_and_laus_geographies(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: EXT-009 — live BLS metadata populates required program fields."""
    monkeypatch.setattr(metadata, "_get_pg_connection", postgres_connection_factory)
    programs = ("la", "ln", "ce", "cu", "jt")
    try:
        assert metadata.sync_bls_datasets_table() >= len(programs)
        for program in programs:
            assert metadata.sync_bls_series_metadata(program) > 0
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT program),
                           BOOL_AND(series_id <> ''), BOOL_AND(title IS NOT NULL)
                    FROM raw_bls.bls_series WHERE program = ANY(%s)
                    """,
                    (list(programs),),
                )
                assert cursor.fetchone() == (len(programs), True, True)
                cursor.execute(
                    """
                    SELECT COUNT(*) FILTER (WHERE area_code LIKE 'ST%'),
                           COUNT(*) FILTER (WHERE area_code LIKE 'CN%'),
                           COUNT(*) FILTER (WHERE area_code LIKE 'MT%')
                    FROM raw_bls.bls_series WHERE program = 'la'
                    """
                )
                assert all(count > 0 for count in cursor.fetchone())
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_bls.bls_series WHERE program = ANY(%s)",
                    (list(programs),),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_datasets WHERE program = ANY(%s)",
                    (list(programs),),
                )
            cleanup.commit()
        finally:
            cleanup.close()
