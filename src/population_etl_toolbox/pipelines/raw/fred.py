"""FRED raw ingestion wrapper."""

from __future__ import annotations

from typing import Optional

from fred.ingest import ingest_slice


def ingest_fred(*, domain: str, date_start: str = "2000-01-01", date_end: Optional[str] = None) -> int:
    """Run a single FRED ingestion slice."""
    return ingest_slice(domain=domain, date_start=date_start, date_end=date_end)
