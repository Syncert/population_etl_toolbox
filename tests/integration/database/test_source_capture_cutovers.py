"""Capture-first production ingestion contracts for migrated source adapters."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls import ingest as bls_ingest
from data_ingestion_toolbox.census_acs import ingest as census_ingest

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def capture_cutover_cleanup(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[list[str]]:
    """Remove the revision rows these ingests commit.

    ``ingest_slice`` writes through the production path, so its rows outlive
    the test. Left behind, the next run's Census transform reads them, finds
    the geographies their suites have since cleaned up, and refuses with
    "silver_ref geography history is incomplete" -- a failure in a suite that
    did nothing wrong, on the second run only.
    """
    variables: list[str] = []
    try:
        yield variables
    finally:
        if not variables:
            return
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_census.observation_revision "
                    "WHERE variable_name = ANY(%s)",
                    (variables,),
                )
                cursor.execute(
                    "DELETE FROM silver_bls.observation_revision "
                    "WHERE series_id = ANY(%s)",
                    (variables,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_census_ingest_captures_array_and_bypasses_legacy_raw(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    capture_cutover_cleanup: list[str],
) -> None:
    """Covers: DB-020, DB-023 — Census normalization starts after capture."""
    token = uuid4().hex[:8].upper()
    variable = f"B{token[:5]}_001E"
    capture_cutover_cleanup.append(variable)
    payload = [[variable, "state", "county"], ["123", "55", "001"]]
    monkeypatch.setattr(
        census_ingest, "_get_pg_connection", postgres_connection_factory
    )
    monkeypatch.setattr(
        census_ingest, "get_curated_variables", lambda _year, _dataset: [variable]
    )
    monkeypatch.setattr(census_ingest, "fetch_acs_api", lambda **_kwargs: payload)
    monkeypatch.setattr(census_ingest.time, "sleep", lambda _delay: None)

    assert census_ingest.ingest_slice(2099, "acs5", "county", "55") == 1

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT value_source, value_status, state_fips_source,
                       county_fips_source
                FROM silver_census.observation_revision
                WHERE variable_name = %s
                """,
                (variable,),
            )
            assert cursor.fetchone() == ("123", "valid", "55", "001")
            cursor.execute("SELECT to_regclass('raw_census.acs_long')")
            assert cursor.fetchone() == (None,)
    finally:
        reader.close()


def test_bls_ingest_captures_complete_response_and_bypasses_legacy_raw(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    capture_cutover_cleanup: list[str],
) -> None:
    """Covers: DB-020, DB-022 — BLS source strings replay from captures."""
    token = uuid4().hex[:10].upper()
    series_id = f"TESTBLS{token}"
    capture_cutover_cleanup.append(series_id)
    payload = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": series_id,
                    "data": [
                        {
                            "year": "2099",
                            "period": "M01",
                            "periodName": "January",
                            "value": "4.20",
                            "latest": "true",
                            "footnotes": [{"text": "Preliminary"}],
                        }
                    ],
                }
            ]
        },
    }
    monkeypatch.setattr(bls_ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(
        bls_ingest, "get_curated_series_for_program", lambda _program: [series_id]
    )
    monkeypatch.setattr(bls_ingest, "fetch_bls_api", lambda **_kwargs: payload)
    monkeypatch.setattr(bls_ingest.time, "sleep", lambda _delay: None)

    assert bls_ingest.ingest_slice("ce", 2099, 2099) == 1

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT year_source, value_source, latest_source,
                       value_status, footnotes_source
                FROM silver_bls.observation_revision
                WHERE series_id = %s
                """,
                (series_id,),
            )
            row = cursor.fetchone()
            assert row[:4] == ("2099", "4.20", "true", "valid")
            assert "Preliminary" in row[4]
            cursor.execute("SELECT to_regclass('raw_bls.bls_long')")
            assert cursor.fetchone() == (None,)
    finally:
        reader.close()
