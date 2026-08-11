"""ETL unit tests: FRED ingest chunk boundaries.

Covers ETL-019 (chunked() preserves every series exactly once around the
configured series batch limit).
"""

import pytest

from data_ingestion_toolbox.fred.ingest import chunked


def _flatten(chunks: list) -> list:
    return [item for chunk in chunks for item in chunk]


@pytest.mark.unit
class TestFredChunkBoundaries:
    """Covers: ETL-019 — chunked() respects the configured batch size."""

    FRED_BATCH_SIZE = 50  # as configured in FredConfig.fred_api_series_chunk_size

    def test_empty_input_returns_no_chunks(self) -> None:
        """Covers: ETL-019 — size zero produces no FRED request chunks."""
        assert list(chunked([], self.FRED_BATCH_SIZE)) == []

    def test_one_item_returns_one_chunk(self) -> None:
        """Covers: ETL-019 — one series is returned exactly once."""
        result = list(chunked(["UNRATE"], self.FRED_BATCH_SIZE))
        assert len(result) == 1
        assert result[0] == ["UNRATE"]

    def test_exactly_batch_size_produces_one_chunk(self) -> None:
        """Covers: ETL-019 — the configured size produces one full chunk."""
        items = [f"S{i}" for i in range(self.FRED_BATCH_SIZE)]
        result = list(chunked(items, self.FRED_BATCH_SIZE))
        assert len(result) == 1
        assert _flatten(result) == items

    def test_batch_size_plus_one_produces_two_chunks(self) -> None:
        """Covers: ETL-019 — size+1 series cross one chunk boundary."""
        items = [f"S{i}" for i in range(self.FRED_BATCH_SIZE + 1)]
        result = list(chunked(items, self.FRED_BATCH_SIZE))
        assert len(result) == 2
        assert _flatten(result) == items

    def test_all_items_preserved_exactly_once(self) -> None:
        """Covers: ETL-019 — chunking neither duplicates nor loses series."""
        items = [f"SERIES_{i}" for i in range(127)]
        flat = _flatten(list(chunked(items, self.FRED_BATCH_SIZE)))
        assert flat == items

    def test_no_chunk_exceeds_batch_size(self) -> None:
        """Covers: ETL-019 — no request exceeds the FRED batch size."""
        items = list(range(300))
        for chunk in chunked(items, self.FRED_BATCH_SIZE):
            assert len(chunk) <= self.FRED_BATCH_SIZE
