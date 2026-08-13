"""ETL unit tests: Census ingest chunk boundaries.

Covers ETL-008 (chunk function preserves order and every item exactly once
for sizes 0, 1, n, n+1, and exact multiples of the chunk size).
"""

import pytest

from data_ingestion_toolbox.census_acs.ingest import chunked


def _flatten(chunks: list) -> list:
    return [item for chunk in chunks for item in chunk]


@pytest.mark.unit
class TestCensusChunkBoundaries:
    """Covers: ETL-008 — chunked() preserves items and boundary sizes."""

    def test_empty_input_returns_no_chunks(self) -> None:
        """Covers: ETL-008 — size zero produces no chunks."""
        result = list(chunked([], 5))
        assert result == []

    def test_single_item_returns_one_chunk(self) -> None:
        """Covers: ETL-008 — one item is returned exactly once."""
        result = list(chunked(["a"], 5))
        assert result == [["a"]]

    def test_size_equals_chunk_produces_one_chunk(self) -> None:
        """Covers: ETL-008 — an exact-size input produces one full chunk."""
        items = list(range(5))
        result = list(chunked(items, 5))
        assert len(result) == 1
        assert _flatten(result) == items

    def test_size_plus_one_produces_two_chunks(self) -> None:
        """Covers: ETL-008 — n+1 items cross one chunk boundary."""
        items = list(range(6))
        result = list(chunked(items, 5))
        assert len(result) == 2
        assert _flatten(result) == items

    def test_order_preserved_across_chunks(self) -> None:
        """Covers: ETL-008 — chunking preserves source order."""
        items = list(range(13))
        result = list(chunked(items, 4))
        assert _flatten(result) == items

    def test_no_item_duplicated(self) -> None:
        """Covers: ETL-008 — chunking neither duplicates nor loses items."""
        items = [f"var_{i}" for i in range(20)]
        flat = _flatten(list(chunked(items, 7)))
        assert len(flat) == len(items)
        assert flat == items

    def test_exact_multiple_of_chunk_size(self) -> None:
        """Covers: ETL-008 — exact multiples produce only full chunks."""
        items = list(range(12))
        result = list(chunked(items, 4))
        assert len(result) == 3
        assert all(len(c) == 4 for c in result)
        assert _flatten(result) == items

    def test_chunk_size_of_one(self) -> None:
        """Covers: ETL-008 — a chunk size of one preserves every item."""
        items = ["x", "y", "z"]
        result = list(chunked(items, 1))
        assert len(result) == 3
        assert _flatten(result) == items
