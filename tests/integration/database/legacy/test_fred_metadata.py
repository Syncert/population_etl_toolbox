"""Bounded live FRED metadata contract using the disposable database."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import metadata
from data_ingestion_toolbox.fred.config import CONFIG

pytestmark = [
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.external,
    pytest.mark.slow,
]


def test_bounded_fred_metadata_sync_populates_required_fields(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: EXT-010 — live FRED metadata upserts into disposable tables."""
    if not CONFIG.has_api_key:
        pytest.skip(
            "FRED_API_KEY is optional; scheduled credentialed job reports this skip"
        )
    series_id = "UNRATE"
    monkeypatch.setattr(metadata, "_get_pg_connection", postgres_connection_factory)
    try:
        assert metadata.sync_fred_datasets_table() > 0
        assert metadata.sync_fred_series_metadata([series_id]) == 1
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT title, units, frequency, observation_start, observation_end
                    FROM raw_fred.fred_series WHERE series_id = %s
                    """,
                    (series_id,),
                )
                row = cursor.fetchone()
                assert row is not None
                assert all(value is not None for value in row)
                cursor.execute(
                    "SELECT COUNT(*) FROM raw_fred.fred_datasets WHERE series_id = %s",
                    (series_id,),
                )
                assert cursor.fetchone()[0] >= 1
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                # sync_fred_datasets_table() writes the whole configured
                # (domain, series) set, not just the series under test.
                # Deleting only that one left the rest behind as committed
                # state, and DQ-FRED-002 then correctly reported configured
                # datasets whose series rows were absent -- which failed the
                # "empty warehouse" quality test and, through it, every test
                # that needs a promotable release certification.
                cursor.execute(
                    "DELETE FROM raw_fred.fred_datasets "
                    "WHERE (domain, series_id) IN %s",
                    (
                        tuple(
                            (domain, configured)
                            for domain, series in CONFIG.curated_by_domain.items()
                            for configured in series
                        ),
                    ),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_datasets WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_series WHERE series_id = %s",
                    (series_id,),
                )
            cleanup.commit()
        finally:
            cleanup.close()
