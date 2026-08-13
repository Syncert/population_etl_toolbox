"""Deterministic source metadata synchronization against disposable PostgreSQL."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls import metadata as bls_metadata
from data_ingestion_toolbox.census_acs import metadata as census_metadata
from data_ingestion_toolbox.fred import metadata as fred_metadata

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def metadata_token(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    token = uuid4().hex[:10].upper()
    try:
        yield token
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute("DELETE FROM raw_census.acs_variables WHERE year = 2095")
                cursor.execute("DELETE FROM raw_census.acs_datasets WHERE year = 2095")
                cursor.execute(
                    "DELETE FROM raw_census.acs_tables "
                    "WHERE dataset = 'acs5' AND table_id = 'B99998'"
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s",
                    (f"TEST_CE_META_{token}",),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_series WHERE series_id = %s",
                    (f"TEST_FRED_META_{token}",),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_census_dataset_and_variable_metadata_upsert_changed_fields(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    metadata_token: str,
) -> None:
    """Covers: ETL-026 — Census metadata filters, adds MOE, and updates."""
    monkeypatch.setattr(
        census_metadata, "_get_pg_connection", postgres_connection_factory
    )
    monkeypatch.setattr(
        census_metadata.CONFIG,
        "curated_tables",
        [*census_metadata.CONFIG.curated_tables, "B99998"],
    )
    monkeypatch.setattr(
        census_metadata,
        "fetch_acs_datasets_from_data_json",
        lambda: [
            {
                "title": f"ACS 5-year fixture {metadata_token}",
                "year": 2095,
                "identifier": "https://api.census.gov/data/id/ACSDT5Y2095",
            }
        ],
    )
    census_metadata.sync_acs_dataset_table()
    census_metadata.sync_acs_dataset_table()

    label = {"value": "Estimate!!Total population"}

    def variables_payload(_year: int, _dataset: str) -> dict:
        return {
            "variables": {
                "B99998_001E": {
                    "label": label["value"],
                    "concept": "Population",
                    "predicateType": "int",
                    "group": "B99998",
                    "attributes": "B99998_001M,B99998_001EA",
                },
                "NAME": {"label": "Name"},
            }
        }

    monkeypatch.setattr(census_metadata, "fetch_variables_json", variables_payload)
    census_metadata.sync_variable_metadata_for_year(2095, "acs5")
    label["value"] = "Estimate!!Total population revised"
    census_metadata.sync_variable_metadata_for_year(2095, "acs5")

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT dataset, census_id FROM raw_census.acs_datasets
                WHERE year = 2095
                """
            )
            assert cursor.fetchall() == [("acs5", "ACSDT5Y2095")]
            cursor.execute(
                """
                SELECT variable_name, label, concept, predicate_type
                FROM raw_census.acs_variables
                WHERE dataset = 'acs5' AND year = 2095
                ORDER BY variable_name
                """
            )
            assert cursor.fetchall() == [
                (
                    "B99998_001E",
                    "Estimate!!Total population revised",
                    "Population",
                    "int",
                ),
                (
                    "B99998_001M",
                    "Margin of Error!!Total population revised",
                    "Population",
                    "int",
                ),
            ]
    finally:
        reader.close()


def test_bls_series_metadata_upsert_replaces_production_fields(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    metadata_token: str,
) -> None:
    """Covers: ETL-026 — BLS normalized metadata replays without duplicates."""
    series_id = f"TEST_CE_META_{metadata_token}"
    title = {"value": "Original payroll title"}
    monkeypatch.setattr(bls_metadata, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(
        bls_metadata,
        "fetch_bls_metadata",
        lambda _program: (
            [
                {
                    "series_id": series_id,
                    "series_title": title["value"],
                    "data_type_code": "01",
                }
            ],
            {},
        ),
    )

    assert bls_metadata.sync_bls_series_metadata("ce") == 1
    title["value"] = "Revised payroll title"
    assert bls_metadata.sync_bls_series_metadata("ce") == 1

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT program, title, measure, raw_metadata->>'series_title'
                FROM raw_bls.bls_series WHERE series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchone() == (
                "ce",
                "Revised payroll title",
                "01",
                "Revised payroll title",
            )
    finally:
        reader.close()


def test_fred_series_metadata_upsert_replaces_dates_and_descriptors(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    metadata_token: str,
) -> None:
    """Covers: ETL-026 — FRED metadata parses dates and updates one series."""
    series_id = f"TEST_FRED_META_{metadata_token}"
    title = {"value": "Original FRED title"}
    monkeypatch.setattr(
        fred_metadata, "_get_pg_connection", postgres_connection_factory
    )
    monkeypatch.setattr(
        fred_metadata,
        "fetch_fred_series_metadata",
        lambda _series_id: {
            "title": title["value"],
            "units": "Percent",
            "frequency": "Monthly",
            "seasonal_adjustment": "Seasonally Adjusted",
            "observation_start": "2000-01-01",
            "observation_end": "2095-12-01",
            "notes": "Fixture metadata",
        },
    )

    assert fred_metadata.sync_fred_series_metadata([series_id]) == 1
    title["value"] = "Revised FRED title"
    assert fred_metadata.sync_fred_series_metadata([series_id]) == 1

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT title, units, frequency, observation_start::TEXT,
                       observation_end::TEXT, raw_metadata->>'title'
                FROM raw_fred.fred_series WHERE series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchone() == (
                "Revised FRED title",
                "Percent",
                "Monthly",
                "2000-01-01",
                "2095-12-01",
                "Revised FRED title",
            )
    finally:
        reader.close()
