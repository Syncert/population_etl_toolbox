# data_ingestion_toolbox/bls/config.py

from __future__ import annotations

from typing import List, Dict

import os
from pydantic import BaseModel, Field, field_validator


# LAUS publishes county/county-equivalent data for the 50 states, DC, and
# Puerto Rico. Numeric FIPS ranges contain unassigned codes such as 52.
LAUS_COUNTY_PARENT_FIPS: tuple[str, ...] = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13",
    "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
    "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36",
    "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48",
    "49", "50", "51", "53", "54", "55", "56", "72",
)


class BlsConfig(BaseModel):
    """
    BLS ingestion config.

    Design goals (matching ACS approach):
    - One schema: raw_bls
    - One ingestion framework: supports multiple BLS programs
    - Curated definitions drive ingestion/backfill
      (hash-based slice ledger in control)

    Philosophy:
    - LAUS requests are selected from published metadata by geography + measure
    - National series are explicit and minimal
    - Do NOT force QCEW into a fake "series_id" abstraction
    """

    bls_api_key: str = Field(default_factory=lambda: os.environ.get("BLS_API_KEY", ""))

    # One raw load is one bounded transaction; planners split larger payloads.
    raw_load_max_rows: int = 10_000

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
            "LNS12300060",  # Employment-population ratio, ages 25-54
            "LNS11300060",  # Labor force participation rate, ages 25-54
            "LNS12032194",  # Employed part time for economic reasons
            "LNS13008276",  # Median duration of unemployment
            "LNS14000003",  # Unemployment rate, White
            "LNS14000006",  # Unemployment rate, Black or African American
            "LNS14000009",  # Unemployment rate, Hispanic or Latino
            "LNS14032183",  # Unemployment rate, Asian
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
            "CES0500000008",  # Average hourly earnings, production/nonsupervisory employees
            "CES0500000011",  # Average weekly earnings of all employees, total private
            "CES1000000001",  # Mining and logging employment
            "CES2000000001",  # Construction employment
            "CES3000000001",  # Manufacturing employment
            "CES4000000001",  # Trade, transportation, and utilities employment
            "CES5000000001",  # Information employment
            "CES5500000001",  # Financial activities employment
            "CES6000000001",  # Professional and business services employment
            "CES6500000001",  # Private education and health services employment
            "CES7000000001",  # Leisure and hospitality employment
            "CES8000000001",  # Other services employment
            "CES9000000001",  # Government employment
        ],
        # --------------------------------------------------------------
        # CPI — Consumer Price Index - All Urban Consumers (cu)
        #
        # Full series IDs.
        # Used for inflation, real-wage adjustments, and COLA analysis.
        # --------------------------------------------------------------
        "cu": [
            "CUUR0000SA0",  # CPI-U, all items, U.S. city average
            "CUUR0000SA0L1E",  # CPI-U, all items less food and energy (core CPI)
            "CWUR0000SA0",  # CPI-W, all items, U.S. city average
            "CUUR0000SAF1",  # CPI-U, food
            "CUUR0000SA0E",  # CPI-U, energy
            "CUUR0000SAH1",  # CPI-U, shelter
            "CUUR0000SEHA",  # CPI-U, rent of primary residence
            "CUUR0000SEHC",  # CPI-U, owners' equivalent rent
            "CUUR0000SAM",  # CPI-U, medical care
        ],
        # --------------------------------------------------------------
        # JOLTS — Job Openings and Labor Turnover Survey (jt)_
        #
        # Full series IDs.
        # Measures labor market churn: openings, hires, quits, separations.
        # --------------------------------------------------------------
        "jt": [
            "JTS000000000000000JOL",  # Job openings level, total nonfarm
            "JTS000000000000000JOR",  # Job openings rate, total nonfarm
            "JTS000000000000000HIL",  # Hires level, total nonfarm
            "JTS000000000000000HIR",  # Hires rate, total nonfarm
            "JTS000000000000000QUL",  # Quits level, total nonfarm
            "JTS000000000000000QUR",  # Quits rate, total nonfarm
            "JTS000000000000000LDL",  # Layoffs and discharges level, total nonfarm
            "JTS000000000000000LDR",  # Layoffs and discharges rate, total nonfarm
            "JTS000000000000000TSL",  # Total separations level, total nonfarm
            "JTS000000000000000TSR",  # Total separations rate, total nonfarm
            "JTS000000000000000OSL",  # Other separations level, total nonfarm
            "JTS000000000000000OSR",  # Other separations rate, total nonfarm
            "JTS000000000000000UOR",  # Unemployed persons per job opening
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

    @field_validator("postgres_conn_id")
    @classmethod
    def validate_postgres_conn_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("postgres_conn_id must not be empty")
        return value

    @field_validator("programs")
    @classmethod
    def validate_program_scope(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("configured BLS program scope must not be empty")
        return value

    @field_validator("curated_by_program")
    @classmethod
    def validate_curated_scope(
        cls, value: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        if not value or any(not series for series in value.values()):
            raise ValueError("configured BLS series scope must not be empty")
        return value

    @field_validator(
        "bls_api_global_concurrency",
        "bls_api_series_chunk_size",
        "bls_api_year_chunk_size",
        "raw_load_max_rows",
        "silver_max_active_tis",
    )
    @classmethod
    def validate_positive_size(cls, value: int) -> int:
        if value < 1:
            raise ValueError("BLS concurrency and batch sizes must be at least 1")
        return value

    @field_validator("bls_api_min_spacing_seconds")
    @classmethod
    def validate_nonnegative_spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("bls_api_min_spacing_seconds must not be negative")
        return value


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
    "03": {
        "name": "Unemployment Rate",
        "unit": "Percent",
        "semantics": "Percent of labor force that is unemployed",
    },
    "04": {
        "name": "Unemployment Level",
        "unit": "Persons",
        "semantics": "Count of unemployed persons",
    },
    "05": {
        "name": "Employment Level",
        "unit": "Persons",
        "semantics": "Count of employed persons",
    },
    "06": {
        "name": "Labor Force Level",
        "unit": "Persons",
        "semantics": "Count of persons in labor force",
    },
    "07": {
        "name": "Employment-Population Ratio",
        "unit": "Percent",
        "semantics": "Employed as percent of civilian noninstitutional population",
    },
    "08": {
        "name": "Labor Force Participation Rate",
        "unit": "Percent",
        "semantics": "Labor force as percent of civilian noninstitutional population",
    },
    "09": {
        "name": "Civilian Noninstitutional Population",
        "unit": "Persons",
        "semantics": "Count of civilian noninstitutional population",
    },
}
