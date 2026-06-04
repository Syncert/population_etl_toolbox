"""Census ACS raw ingestion wrapper."""

from __future__ import annotations

from typing import Optional

from census_acs.ingest import ingest_slice


def ingest_acs(*, year: int, dataset: str, geo_level: str, state_fips: Optional[str] = None) -> int:
    """Run a single ACS ingestion slice."""
    return ingest_slice(year=year, dataset=dataset, geo_level=geo_level, state_fips=state_fips)
