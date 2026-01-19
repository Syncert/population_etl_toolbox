# include/bls/config.py

from __future__ import annotations

import os
from pydantic import BaseModel, Field
from typing import List, Dict


class BlsConfig(BaseModel):
    """
    BLS ingestion config.

    Design goals (matching ACS approach):
    - One schema: raw_bls
    - One ingestion framework: can expand across BLS programs (LAUS, CES, QCEW, CPI, etc.)
    - Curated series list drives ingestion/backfill (hash-based slice ledger in raw_bls)
    """

    bls_api_key: str = Field(default_factory=lambda: os.environ.get("BLS_API_KEY", ""))

    # Programs you want enabled (expand later: ces, qcew, cpi, etc.)
    programs: List[str] = ["laus"]

    # Curated series IDs (start with LAUS; expand over time)
    #
    # NOTE: Series IDs are the BLS-native identifiers you request from the API.
    # You can keep these as a single flat list, OR later upgrade to a structured dict per program.
    curated_series_ids: List[str] = [
        # --- LAUS examples (STATE-level shown as pattern examples) ---
        # WARNING: These may not be the exact IDs you ultimately choose.
        # Replace with your validated LAUS series IDs once you confirm your target measures/areas.
        #
        # Example commonly-used national series (not guaranteed):
        # "LNS14000000",  # Unemployment rate (national) - often used, but not LAUS
        #
        # For LAUS, series IDs encode area + measure. You will likely store explicit IDs.
    ]

    # Optional: keep a “program -> series list” structure for future expansion
    # If you don’t want it yet, leave empty and use curated_series_ids.
    curated_by_program: Dict[str, List[str]] = {
        # "laus": ["LAUST010000000000003", ...]  # fill once validated
    }

    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    # Rate limiting / concurrency controls (mirrors ACS knobs)
    bls_api_global_concurrency: int = 2
    bls_api_min_spacing_seconds: float = 0.25

    # BLS API commonly restricts max series per request.
    # Keep this configurable; your ingest.py should chunk series accordingly.
    bls_api_series_chunk_size: int = 50

    # How many years per request (some APIs behave better with smaller ranges)
    bls_api_year_chunk_size: int = 10

    @property
    def has_api_key(self) -> bool:
        return bool(self.bls_api_key)


CONFIG = BlsConfig()