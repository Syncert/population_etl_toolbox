# include/fred/config.py

from __future__ import annotations

import os
from pydantic import BaseModel, Field
from typing import List, Dict


class FredConfig(BaseModel):
    """
    FRED ingestion config.

    Design goals (matching ACS approach):
    - One schema: raw_fred
    - One ingestion framework: expands to any number of series
    - Curated series list drives ingestion/backfill (hash-based slice ledger in raw_fred)
    """

    fred_api_key: str = Field(default_factory=lambda: os.environ.get("FRED_API_KEY", ""))

    # Keep this as a single unified ingestion, but allow logical grouping
    # (These groups are optional; they help you organize series without creating new schemas.)
    domains: List[str] = ["housing"]  # expand later: macro, labor, finance, regional

    curated_series_ids: List[str] = [
        # Put your actual FRED series IDs here, examples:
        # "CPIHOSSL",     # CPI: Housing (example)
        # "CSUSHPINSA",   # Case-Shiller National HPI (example)
        # "RRVRUSQ156N",  # Rental vacancy rate (example)
    ]

    curated_by_domain: Dict[str, List[str]] = {
        # "housing": ["CPIHOSSL", "CSUSHPINSA"]
    }

    postgres_conn_id: str = "public_data"

    # Rate limiting / concurrency controls (mirrors ACS knobs)
    fred_api_global_concurrency: int = 2
    fred_api_min_spacing_seconds: float = 0.25

    # Batch sizes for requests
    fred_api_series_chunk_size: int = 50

    @property
    def has_api_key(self) -> bool:
        return bool(self.fred_api_key)


CONFIG = FredConfig()
