"""Bounded live FRED ingestion contract using the disposable database."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import ingest
from data_ingestion_toolbox.fred.config import CONFIG

pytestmark = [
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.external,
    pytest.mark.slow,
]


def test_bounded_live_fred_slice_loads_values_and_missing_flags(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: EXT-008 — one live FRED series reaches production raw storage."""
    if not CONFIG.has_api_key:
        pytest.skip(
            "FRED_API_KEY is optional; scheduled credentialed job reports this skip"
        )
    domain = "external_contract_fred"
    monkeypatch.setattr(ingest, "_get_pg_connection", postgres_connection_factory)
    try:
        loaded = ingest.ingest_slice(
            domain=domain,
            series_ids=["UNRATE"],
            date_start="2023-01-01",
            date_end="2023-03-31",
        )
        assert loaded == 3
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE value IS NOT NULL),
                           BOOL_AND(is_missing = (value IS NULL))
                    FROM raw_fred.fred_long
                    WHERE domain = %s AND series_id = 'UNRATE'
                      AND obs_date BETWEEN '2023-01-01' AND '2023-03-31'
                    """,
                    (domain,),
                )
                assert cursor.fetchone() == (3, 3, True)
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE domain = %s", (domain,)
                )
            cleanup.commit()
        finally:
            cleanup.close()
