# data_ingestion_toolbox/bls/config.py

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
    - LAUS requests are selected from published metadata by geography + measure
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
        "ln",
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
    #    They are *measure-code filters*. ingest.py selects complete matching
    #    series IDs from raw_bls.bls_series so unavailable combinations are
    #    never requested.
    #
    # 2) CES, CPI, and JOLTS entries ARE full series IDs.
    #    These programs do not use area-code expansion in the same way.
    #
    # 3) Treat each program as a separate "instrument":
    #    - LAUS  -> people, residence-based labor conditions (la)
    #    - CPS   -> national labor force survey (household)  (ln)
    #    - CES   -> jobs, establishment-based employment     (ce)
    #    - CPI   -> prices / inflation                       (cu)
    #    - JOLTS -> labor market flows and tightness         (jt)
    #
    # WARNING:
    # Similar metric names across BLS programs are not automatically
    # comparable. Preserve at least program, series_id, measure_name,
    # unit, seasonal_adjustment, geo_level, and observation_basis in downstream
    # models. Household measures (CPS/LN, LAUS) are not the same thing
    # as establishment/payroll measures (CES), and price indexes (CPI) and
    # flow measures (JOLTS) should not be flattened into the same semantic
    # bucket as level-based labor statistics.
    # ------------------------------------------------------------------
    curated_by_program: Dict[str, List[str]] = {

        # --------------------------------------------------------------
        # LAUS — Local Area Unemployment Statistics (la)
        #
        # Measure codes only.
        # Expanded only for published LAUS subnational geographies. National
        # household labor statistics come from the CPS/LN series below.
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
        # CPS/LN — Current Population Survey (Household Survey) (ln)
        #
        # Full series IDs.
        # National-level labor force statistics from household survey.
        # This is the authoritative source for national unemployment rate.
        # --------------------------------------------------------------
        "ln": [
            "LNS14000000",  # Unemployment rate (national)
            "LNS13000000",  # Unemployment level (national)
            "LNS12000000",  # Employment level (national)
            "LNS11000000",  # Civilian labor force level (national)
            "LNS11300000",  # Labor force participation rate (national)
            "LNS12300000",  # Employment-population ratio (national)
            "LNS15000000",  # Not in labor force (national)
            "LNS13327709",  # U-6 total labor underutilization rate
            "LNS13025703",  # Unemployed 27 weeks and over
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
            "CES0500000001",  # Total private employment
            "CES0500000002",  # Average weekly hours of all employees, total private
            "CES0500000003",  # Average hourly earnings of all employees, total private
            "CES0500000008",  # Average weekly earnings of all employees, total private
        ],

        # --------------------------------------------------------------
        # CPI — Consumer Price Index - All Urban Consumers (cu)
        #
        # Full series IDs.
        # Used for inflation, real-wage adjustments, and COLA analysis.
        # --------------------------------------------------------------
        "cu": [
            "CUUR0000SA0",    # CPI-U, all items, U.S. city average
            "CUUR0000SA0L1E", # CPI-U, all items less food and energy (core CPI)
            "CWUR0000SA0",    # CPI-W, all items, U.S. city average
        ],

        # --------------------------------------------------------------
        # JOLTS — Job Openings and Labor Turnover Survey (jt)_
        #
        # Full series IDs.
        # Measures labor market churn: openings, hires, quits, separations.
        # --------------------------------------------------------------
        "jt": [
            "JTS000000000000000JOL",  # Job openings, total nonfarm (national)
            "JTS000000000000000HIR",  # Hires, total nonfarm (national)
            "JTS000000000000000QUR",  # Quits, total nonfarm (national)
            "JTS000000000000000LDL",  # Layoffs and discharges, total nonfarm (national)
            "JTS000000000000000TSL",  # Total separations, total nonfarm (national)
            "JTS000000000000000OSL",  # Other separations, total nonfarm (national)
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
    bls_api_year_chunk_size: int = 20

    # Airflow max_active_tis_per_dag — caps concurrent mapped tasks to
    # prevent Postgres connection exhaustion.
    silver_max_active_tis: int = 4

    @property
    def has_api_key(self) -> bool:
        return bool(self.bls_api_key)

CONFIG = BlsConfig()

# ---------------------------------------------------------------------------
# Human-readable labels for BLS programs and LAUS measure codes.
# Used by silver transforms for measure-name derivation.
# ---------------------------------------------------------------------------
BLS_PROGRAM_LABELS: Dict[str, str] = {
    "la": "LAUS",
    "ln": "CPS",
    "ce": "CES",
    "cu": "CPI",
    "jt": "JOLTS",
}

LAUS_MEASURE_META: Dict[str, Dict[str, str]] = {
    "03": {"name": "Unemployment Rate", "unit": "Percent", "semantics": "Percent of labor force that is unemployed"},
    "04": {"name": "Unemployment Level", "unit": "Persons", "semantics": "Count of unemployed persons"},
    "05": {"name": "Employment Level", "unit": "Persons", "semantics": "Count of employed persons"},
    "06": {"name": "Labor Force Level", "unit": "Persons", "semantics": "Count of persons in labor force"},
    "07": {"name": "Employment-Population Ratio", "unit": "Percent", "semantics": "Employed as percent of civilian noninstitutional population"},
    "08": {"name": "Labor Force Participation Rate", "unit": "Percent", "semantics": "Labor force as percent of civilian noninstitutional population"},
    "09": {"name": "Civilian Noninstitutional Population", "unit": "Persons", "semantics": "Count of civilian noninstitutional population"},
}
