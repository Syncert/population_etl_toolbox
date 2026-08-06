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
    """ETL-019: chunked() respects the configured series batch size."""

    FRED_BATCH_SIZE = 50  # as configured in FredConfig.fred_api_series_chunk_size

    def test_empty_input_returns_no_chunks(self) -> None:
        assert list(chunked([], self.FRED_BATCH_SIZE)) == []

    def test_one_item_returns_one_chunk(self) -> None:
        result = list(chunked(["UNRATE"], self.FRED_BATCH_SIZE))
        assert len(result) == 1
        assert result[0] == ["UNRATE"]

    def test_exactly_batch_size_produces_one_chunk(self) -> None:
        items = [f"S{i}" for i in range(self.FRED_BATCH_SIZE)]
        result = list(chunked(items, self.FRED_BATCH_SIZE))
        assert len(result) == 1
        assert _flatten(result) == items

    def test_batch_size_plus_one_produces_two_chunks(self) -> None:
        items = [f"S{i}" for i in range(self.FRED_BATCH_SIZE + 1)]
        result = list(chunked(items, self.FRED_BATCH_SIZE))
        assert len(result) == 2
        assert _flatten(result) == items

    def test_all_items_preserved_exactly_once(self) -> None:
        items = [f"SERIES_{i}" for i in range(127)]
        flat = _flatten(list(chunked(items, self.FRED_BATCH_SIZE)))
        assert flat == items

    def test_no_chunk_exceeds_batch_size(self) -> None:
        items = list(range(300))
        for chunk in chunked(items, self.FRED_BATCH_SIZE):
            assert len(chunk) <= self.FRED_BATCH_SIZE
