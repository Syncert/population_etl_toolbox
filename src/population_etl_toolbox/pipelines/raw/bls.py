"""BLS raw ingestion wrapper."""

from __future__ import annotations

from typing import Optional

from bls.ingest import ingest_slice


def ingest_bls(*, program: str, start_year: int, end_year: int, geo_level: Optional[str] = None, state_fips: Optional[str] = None) -> int:
    """Run a single BLS ingestion slice."""
    return ingest_slice(
        program=program,
        start_year=start_year,
        end_year=end_year,
        geo_level=geo_level,
        state_fips=state_fips,
    )
