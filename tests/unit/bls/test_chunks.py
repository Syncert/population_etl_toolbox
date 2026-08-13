"""ETL unit tests: BLS ingest chunk boundaries.

Covers ETL-014 (chunked() preserves every series exactly once around the
configured API batch limit).
"""

import pytest

from data_ingestion_toolbox.bls.ingest import chunked


def _flatten(chunks: list) -> list:
    return [item for chunk in chunks for item in chunk]


@pytest.mark.unit
class TestBlsChunkBoundaries:
    """Covers: ETL-014 — chunked() respects the API batch limit."""

    BLS_BATCH_LIMIT = 50  # BLS API maximum series per request

    def test_empty_input_returns_no_chunks(self) -> None:
        """Covers: ETL-014 — size zero produces no BLS request chunks."""
        assert list(chunked([], self.BLS_BATCH_LIMIT)) == []

    def test_one_item_returns_one_chunk(self) -> None:
        """Covers: ETL-014 — one series is returned exactly once."""
        result = list(chunked(["LAUST060000000000003"], self.BLS_BATCH_LIMIT))
        assert len(result) == 1
        assert result[0] == ["LAUST060000000000003"]

    def test_exactly_batch_limit_produces_one_chunk(self) -> None:
        """Covers: ETL-014 — the API limit produces one full chunk."""
        items = [f"series_{i}" for i in range(self.BLS_BATCH_LIMIT)]
        result = list(chunked(items, self.BLS_BATCH_LIMIT))
        assert len(result) == 1
        assert _flatten(result) == items

    def test_batch_limit_plus_one_produces_two_chunks(self) -> None:
        """Covers: ETL-014 — limit+1 series cross one chunk boundary."""
        items = [f"series_{i}" for i in range(self.BLS_BATCH_LIMIT + 1)]
        result = list(chunked(items, self.BLS_BATCH_LIMIT))
        assert len(result) == 2
        assert _flatten(result) == items

    def test_all_items_present_exactly_once(self) -> None:
        """Covers: ETL-014 — chunking neither duplicates nor loses series."""
        items = [f"s_{i}" for i in range(127)]
        flat = _flatten(list(chunked(items, self.BLS_BATCH_LIMIT)))
        assert len(flat) == len(items)
        assert flat == items

    def test_chunk_size_never_exceeds_limit(self) -> None:
        """Covers: ETL-014 — no request exceeds the BLS batch limit."""
        items = list(range(200))
        for chunk in chunked(items, self.BLS_BATCH_LIMIT):
            assert len(chunk) <= self.BLS_BATCH_LIMIT
