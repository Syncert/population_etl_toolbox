"""FRED metadata synchronization failure contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.fred import metadata

pytestmark = pytest.mark.unit


def test_series_sync_rolls_back_when_configured_metadata_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class Connection:
        rolled_back = False
        closed = False

        def cursor(self) -> Cursor:
            return Cursor()

        def rollback(self) -> None:
            self.rolled_back = True

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(metadata, "_get_pg_connection", lambda: connection)
    monkeypatch.setattr(metadata, "fetch_fred_series_metadata", lambda _series: {})

    with pytest.raises(RuntimeError, match="MISSING: no metadata returned"):
        metadata.sync_fred_series_metadata(["MISSING"])

    assert connection.rolled_back
    assert connection.closed

