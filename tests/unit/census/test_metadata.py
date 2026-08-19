"""ACS catalog discovery and fail-closed synchronization contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs import metadata

pytestmark = pytest.mark.unit


def test_detailed_table_catalog_identity_is_classified() -> None:
    assert metadata._classify_detailed_table_dataset(
        {
            "title": "ACS 5-Year Detailed Tables",
            "year": 2024,
            "identifier": "https://api.census.gov/data/id/ACSDT5Y2024",
        }
    ) == ("acs5", 2024, "ACSDT5Y2024", "ACS 5-Year Detailed Tables")


def test_non_detailed_table_catalog_identity_is_skipped() -> None:
    assert (
        metadata._classify_detailed_table_dataset(
            {
                "title": "ACS 5-Year Data Profiles",
                "year": 2024,
                "identifier": "https://api.census.gov/data/id/ACS5Y2024",
            }
        )
        is None
    )


def test_dataset_sync_rejects_empty_catalog_before_opening_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(metadata, "fetch_acs_datasets_from_data_json", lambda: [])
    monkeypatch.setattr(
        metadata,
        "_get_pg_connection",
        lambda: pytest.fail("database must not be opened for an empty catalog"),
    )

    with pytest.raises(RuntimeError, match="empty ingestion plan"):
        metadata.sync_acs_dataset_table()


def test_dataset_sync_propagates_database_insert_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenCursor:
        def execute(self, *_args, **_kwargs) -> None:
            raise RuntimeError("insert failed")

        def close(self) -> None:
            return None

    class BrokenConnection:
        autocommit = True
        rolled_back = False
        closed = False

        def cursor(self) -> BrokenCursor:
            return BrokenCursor()

        def rollback(self) -> None:
            self.rolled_back = True

        def close(self) -> None:
            self.closed = True

    connection = BrokenConnection()
    monkeypatch.setattr(
        metadata,
        "fetch_acs_datasets_from_data_json",
        lambda: [
            {
                "title": "ACS 5-Year Detailed Tables",
                "year": 2024,
                "identifier": "https://api.census.gov/data/id/ACSDT5Y2024",
            }
        ],
    )
    monkeypatch.setattr(metadata, "_get_pg_connection", lambda: connection)

    with pytest.raises(RuntimeError, match="insert failed"):
        metadata.sync_acs_dataset_table()

    assert connection.rolled_back
    assert connection.closed
