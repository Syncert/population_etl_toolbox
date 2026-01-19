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
    - Curated series list drives ingestion/backfill
      (hash-based slice ledger in raw_fred)

    Philosophy:
    - FRED is the *stable macro spine*
    - Prefer FRED mirrors of BLS where possible for simplicity
    - Keep series count small, high-signal, and product-driven
    """

    fred_api_key: str = Field(
        default_factory=lambda: os.environ.get("FRED_API_KEY", "")
    )

    # Logical groupings ONLY for organization / readability.
    # They should NOT imply separate schemas or pipelines.
    domains: List[str] = [
        "labor_cycle",
        "housing",
        "prices",
        "rates",
        "macro",
    ]

    # Flat list of series IDs that always ingest.
    # This is what your DAG should ultimately expand into slices.
    curated_series_ids: List[str] = [
        # ------------------------------------------------------------------
        # LABOR MARKET / BUSINESS CYCLE (core national signals)
        # ------------------------------------------------------------------
        "PAYEMS",     # Total nonfarm payroll employment (CES mirror)
        "UNRATE",     # Unemployment rate
        "CIVPART",    # Labor force participation rate
        "JTSJOL",     # Job openings: total nonfarm (labor demand / tightness)

        # ------------------------------------------------------------------
        # HOUSING SUPPLY & AFFORDABILITY (leading indicators)
        # ------------------------------------------------------------------
        "PERMIT",         # New housing units authorized by permits
        "HOUST",          # Housing starts
        "MORTGAGE30US",   # 30-year fixed mortgage rate

        # ------------------------------------------------------------------
        # PRICES / INFLATION (used to deflate nominal ACS & wage values)
        # ------------------------------------------------------------------
        "CPIAUCSL",   # CPI-U, all items

        # ------------------------------------------------------------------
        # MACRO / POLICY CONTEXT (scenario & regime modeling)
        # ------------------------------------------------------------------
        "FEDFUNDS",   # Effective federal funds rate
        "DGS10",      # 10-year Treasury yield
        "GDPC1",      # Real GDP
    ]

    # Optional grouping by domain for readability, dashboards, or docs.
    # Ingestion logic should still operate off curated_series_ids.
    curated_by_domain: Dict[str, List[str]] = {
        "labor_cycle": [
            "PAYEMS",
            "UNRATE",
            "CIVPART",
            "JTSJOL",
        ],
        "housing": [
            "PERMIT",
            "HOUST",
            "MORTGAGE30US",
        ],
        "prices": [
            "CPIAUCSL",
        ],
        "rates": [
            "FEDFUNDS",
            "DGS10",
        ],
        "macro": [
            "GDPC1",
        ],
    }

    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    # Rate limiting / concurrency controls
    fred_api_global_concurrency: int = 2
    fred_api_min_spacing_seconds: float = 0.25

    # Chunking for large backfills
    fred_api_series_chunk_size: int = 50

    @property
    def has_api_key(self) -> bool:
        return bool(self.fred_api_key)



CONFIG = FredConfig()
