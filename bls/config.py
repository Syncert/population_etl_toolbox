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
    - One ingestion framework: supports multiple BLS programs
    - Curated definitions drive ingestion/backfill
      (hash-based slice ledger in raw_bls)

    Philosophy:
    - LAUS is programmatic (generate IDs from area + measure)
    - National series are explicit and minimal
    - Do NOT force QCEW into a fake "series_id" abstraction
    """

    bls_api_key: str = Field(
        default_factory=lambda: os.environ.get("BLS_API_KEY", "")
    )

    # Enabled BLS programs.
    # Start with LAUS; expand later (ces, cpi, jolts, qcew).
    programs: List[str] = [
        "laus",
        "ces",
        "cpi",
        "jolts"
    ]

    # ------------------------------------------------------------------
    # IMPORTANT:
    # For LAUS, you should NOT enumerate every county/state series ID.
    # Instead, treat these as *measure codes* to be combined with
    # area codes dynamically in ingest.py.
    # ------------------------------------------------------------------
    curated_series_ids: List[str] = [
        # These represent LAUS MEASURE CODES, not full IDs.
        # Your ingestion code should expand these using:
        #   LA{seasonal}{area_code}{measure_code}

        "03",  # Unemployment rate
        "04",  # Unemployment level
        "05",  # Employment level
        "06",  # Labor force level
        "08",  # Labor force participation rate

        # Optional / advanced:
        # "07",  # Employment-population ratio
        # "09",  # Civilian noninstitutional population
    ]

    # Optional structured view by program.
    # This becomes useful once CES/CPI/JOLTS are added.
    curated_by_program: Dict[str, List[str]] = {
        "laus": [
            "03",
            "04",
            "05",
            "06",
            "08",
        ],

        # Uncomment when needed:
        "ces": [
            "CES0000000001",  # Total nonfarm payrolls (national)
        ],
        "cpi": [
            "CUUR0000SA0",    # CPI-U, all items
        ],
        "jolts": [
            "JTS000000000000000JOL",  # Job openings, total nonfarm
        ],
    }

    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    # Rate limiting / concurrency controls
    bls_api_global_concurrency: int = 2
    bls_api_min_spacing_seconds: float = 0.25

    # BLS API limits series per request
    bls_api_series_chunk_size: int = 50

    # Year chunking to avoid API instability on long ranges
    bls_api_year_chunk_size: int = 10

    @property
    def has_api_key(self) -> bool:
        return bool(self.bls_api_key)

CONFIG = BlsConfig()