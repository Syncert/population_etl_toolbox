"""Failure injection at the production capture/replay boundary."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import ingest
from data_ingestion_toolbox.fred.silver_fred.replay import FredCapturePayloadError

pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.slow]


def test_parser_failure_preserves_capture_and_records_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-003, RES-004 — committed capture survives failed replay."""
    domain = f"resilience_{uuid4().hex[:10]}"
    series_id = f"RES_{uuid4().hex[:12].upper()}"
    monkeypatch.setattr(ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(
        ingest,
        "fetch_fred_observations",
        lambda **_kwargs: {"observations": ["not-an-object"]},
    )

    with pytest.raises(FredCapturePayloadError):
        ingest.ingest_slice(domain, [series_id], "2097-01-01", "2097-01-31")

    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT run.status, request.status, COUNT(capture.capture_id),
                          COUNT(quarantine.quarantine_id)
                   FROM control.ingestion_run run
                   JOIN control.ingestion_request request USING (run_id, source_code)
                   JOIN raw_capture.response_capture capture USING (request_id, run_id, source_code)
                   LEFT JOIN control.capture_quarantine quarantine USING (capture_id, run_id, source_code)
                   WHERE request.request_parameters ->> 'domain' = %s
                   GROUP BY run.status, request.status""",
                (domain,),
            )
            assert cursor.fetchone() == ("failed", "quarantined", 1, 1)
    finally:
        conn.close()


def test_raw_capture_rejects_mutation_after_successful_ingest(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: RES-004 — retry safety relies on immutable committed payloads."""
    domain = f"immutable_{uuid4().hex[:10]}"
    series_id = f"IMM_{uuid4().hex[:12].upper()}"
    monkeypatch.setattr(ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(
        ingest,
        "fetch_fred_observations",
        lambda **_kwargs: {"observations": [{"date": "2097-01-01", "value": "1"}]},
    )
    assert ingest.ingest_slice(domain, [series_id], "2097-01-01", "2097-01-31") == 1

    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor, pytest.raises(Exception, match="append-only"):
            cursor.execute(
                """UPDATE raw_capture.response_capture SET http_status = 201
                   WHERE request_parameters ->> 'domain' = %s""",
                (domain,),
            )
        conn.rollback()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT http_status FROM raw_capture.response_capture WHERE request_parameters ->> 'domain' = %s",
                (domain,),
            )
            assert cursor.fetchone() == (200,)
    finally:
        conn.close()
