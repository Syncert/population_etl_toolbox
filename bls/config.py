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

    # ------------------------------------------------------------------
    # Enabled BLS programs.
    #
    # Each program has its own series-ID grammar and ingestion logic.
    # DO NOT assume series IDs are interchangeable across programs.
    # ------------------------------------------------------------------
    programs: List[str] = [
        "la",
        "ce",
        "cu",
        "jt",
    ]

    # ------------------------------------------------------------------
    # Curated series definitions by BLS program.
    #
    # IMPORTANT DESIGN NOTES:
    #
    # 1) LAUS entries below are NOT full series IDs.
    #    They are *measure codes* that must be dynamically expanded
    #    in ingest.py using:
    #
    #        LA{seasonal}{area_code}{measure_code}
    #
    #    This avoids hardcoding thousands of county/state series.
    #
    # 2) CES, CPI, and JOLTS entries ARE full series IDs.
    #    These programs do not use area-code expansion in the same way.
    #
    # 3) Treat each program as a separate "instrument":
    #    - LAUS  -> people, residence-based labor conditions (la)
    #    - CES   -> jobs, establishment-based employment     (ce)
    #    - CPI   -> prices / inflation                       (cu)
    #    - JOLTS -> labor market flows and tightness         (jt)
    # ------------------------------------------------------------------
    curated_by_program: Dict[str, List[str]] = {

        # --------------------------------------------------------------
        # LAUS — Local Area Unemployment Statistics (la)
        #
        # Measure codes only.
        # Valid for national, state, county, metro, and city geographies.
        # Counties are typically NOT seasonally adjusted.
        # --------------------------------------------------------------
        "la": [
            "03",  # Unemployment rate (% of labor force)
            "04",  # Unemployment level (count)
            "05",  # Employment level (count)
            "06",  # Labor force level (count)
            "07",  # Employment-population ratio
            "08",  # Labor force participation rate (% of population)
            "09",  # Civilian noninstitutional population
        ],

        # --------------------------------------------------------------
        # CES — Current Employment Statistics (Payroll Survey) (ce)
        #
        # Full series IDs.
        # Measures jobs by place of work, not people.
        # Geography is limited (no counties).
        # --------------------------------------------------------------
        "ce": [
            "CES0000000001",  # Total nonfarm payroll employment (national)
        ],

        # --------------------------------------------------------------
        # CPI — Consumer Price Index - All Urban Consumers (cu)
        #
        # Full series IDs.
        # Used for inflation, real-wage adjustments, and COLA analysis.
        # --------------------------------------------------------------
        "cu": [
            "CUUR0000SA0",    # CPI-U, all items, U.S. city average
        ],

        # --------------------------------------------------------------
        # JOLTS — Job Openings and Labor Turnover Survey (jt)_
        #
        # Full series IDs.
        # Measures labor market churn: openings, hires, quits, separations.
        # --------------------------------------------------------------
        "jt": [
            "JTS000000000000000JOL",  # Job openings, total nonfarm (national)
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