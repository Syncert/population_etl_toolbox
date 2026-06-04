"""FRED connector wrappers."""

from fred.ingest import ingest_slice
from fred.metadata import sync_fred_datasets_table, sync_fred_series_metadata

__all__ = ["ingest_slice", "sync_fred_datasets_table", "sync_fred_series_metadata"]
