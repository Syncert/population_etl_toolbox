"""ARCH-007 FRED capture-first ingestion contracts."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred import ingest

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_fred_ingest_commits_capture_before_silver_and_bypasses_legacy_raw(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-020, DB-023 — production FRED uses capture-first replay."""
    token = uuid4().hex[:12]
    domain = f"capture_ingest_{token}"
    series_id = f"CAPTURE_{token.upper()}"
    payload = {
        "observations": [
            {
                "date": "2024-01-01",
                "value": "3.75",
                "realtime_start": "2024-02-01",
                "realtime_end": "2024-02-01",
            },
            {
                "date": "2024-02-01",
                "value": ".",
                "realtime_start": "2024-03-01",
                "realtime_end": "2024-03-01",
            },
        ]
    }

    monkeypatch.setattr(ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(ingest, "fetch_fred_observations", lambda **_kwargs: payload)
    monkeypatch.setattr(ingest.time, "sleep", lambda _delay: None)

    assert ingest.ingest_slice(
        domain=domain,
        series_ids=[series_id],
        date_start="2024-01-01",
        date_end="2024-02-29",
    ) == 2

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT run.status, request.status, capture.capture_id,
                       revision.value_source, revision.value_status
                FROM control.ingestion_run AS run
                JOIN control.ingestion_request AS request USING (run_id, source_code)
                JOIN raw_capture.response_capture AS capture
                  USING (request_id, run_id, source_code)
                JOIN silver_fred.observation_revision AS revision
                  USING (capture_id)
                WHERE run.source_code = 'FRED'
                  AND request.request_parameters ->> 'domain' = %s
                ORDER BY revision.observation_index
                """,
                (domain,),
            )
            rows = cursor.fetchall()
            assert [(row[0], row[1], row[3], row[4]) for row in rows] == [
                ("success", "captured", "3.75", "valid"),
                ("success", "captured", ".", "missing"),
            ]
            cursor.execute(
                "SELECT COUNT(*) FROM raw_fred.fred_long WHERE series_id = %s",
                (series_id,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()
