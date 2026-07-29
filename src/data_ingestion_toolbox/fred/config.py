# data_ingestion_toolbox/fred/config.py

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
        "ICSA",       # Initial unemployment insurance claims
        "INDPRO",     # Industrial production index

        # ------------------------------------------------------------------
        # HOUSING SUPPLY & AFFORDABILITY (leading indicators)
        # ------------------------------------------------------------------
        "PERMIT",         # New housing units authorized by permits
        "HOUST",          # Housing starts
        "MORTGAGE30US",   # 30-year fixed mortgage rate
        "MSACSR",         # Monthly supply of new houses
        "MSPUS",          # Median sales price of houses sold

        # ------------------------------------------------------------------
        # PRICES / INFLATION (used to deflate nominal ACS & wage values)
        # ------------------------------------------------------------------
        "CPIAUCSL",   # CPI-U, all items
        "PCEPI",      # PCE price index, all items
        "PCEPILFE",   # PCE price index excluding food and energy

        # ------------------------------------------------------------------
        # MACRO / POLICY CONTEXT (scenario & regime modeling)
        # ------------------------------------------------------------------
        "FEDFUNDS",   # Effective federal funds rate
        "DGS10",      # 10-year Treasury yield
        "T10Y2Y",     # 10-year minus 2-year Treasury spread
        "T10YIE",     # 10-year breakeven inflation rate
        "NFCI",       # Chicago Fed National Financial Conditions Index
        "GDPC1",      # Real GDP
        "PCEC96",     # Real personal consumption expenditures
        "DSPIC96",    # Real disposable personal income
        "PSAVERT",    # Personal saving rate
        "RSAFS",      # Advance retail and food-services sales
    ]

    # Optional grouping by domain for readability, dashboards, or docs.
    # Ingestion logic should still operate off curated_series_ids.
    curated_by_domain: Dict[str, List[str]] = {
        "labor_cycle": [
            "PAYEMS",
            "UNRATE",
            "CIVPART",
            "JTSJOL",
            "ICSA",
            "INDPRO",
        ],
        "housing": [
            "PERMIT",
            "HOUST",
            "MORTGAGE30US",
            "MSACSR",
            "MSPUS",
        ],
        "prices": [
            "CPIAUCSL",
            "PCEPI",
            "PCEPILFE",
        ],
        "rates": [
            "FEDFUNDS",
            "DGS10",
            "T10Y2Y",
            "T10YIE",
            "NFCI",
        ],
        "macro": [
            "GDPC1",
            "PCEC96",
            "DSPIC96",
            "PSAVERT",
            "RSAFS",
        ],
    }

    # Airflow connection ID to Postgres
    postgres_conn_id: str = "public_data"

    # Rate limiting / concurrency controls
    fred_api_global_concurrency: int = 2
    fred_api_min_spacing_seconds: float = 0.25

    # Chunking for large backfills
    fred_api_series_chunk_size: int = 50

    # Airflow max_active_tis_per_dag — caps concurrent mapped tasks to
    # prevent Postgres connection exhaustion.
    silver_max_active_tis: int = 4

    @property
    def has_api_key(self) -> bool:
        return bool(self.fred_api_key)

    def configured_series_by_domain(self) -> Dict[str, List[str]]:
        """
        Return the validated, single-owner domain classification.

        A FRED observation is unique by series and date in both raw and silver,
        so assigning one series to multiple domains would make the final domain
        depend on ingestion order.
        """
        duplicate_domains = sorted({
            domain for domain in self.domains if self.domains.count(domain) > 1
        })
        if duplicate_domains:
            raise ValueError(
                f"FRED domains must be unique; duplicates: {duplicate_domains}"
            )

        configured_domains = set(self.domains)
        classified_domains = set(self.curated_by_domain)
        missing_domains = sorted(configured_domains - classified_domains)
        extra_domains = sorted(classified_domains - configured_domains)
        empty_domains = sorted(
            domain
            for domain in self.domains
            if not self.curated_by_domain.get(domain)
        )
        if missing_domains or extra_domains or empty_domains:
            raise ValueError(
                "Invalid FRED domain classification: "
                f"missing={missing_domains}, extra={extra_domains}, "
                f"empty={empty_domains}"
            )

        owners: Dict[str, List[str]] = {}
        for domain in self.domains:
            for series_id in self.curated_by_domain[domain]:
                owners.setdefault(series_id, []).append(domain)

        multiply_classified = {
            series_id: domains
            for series_id, domains in owners.items()
            if len(domains) > 1
        }
        if multiply_classified:
            raise ValueError(
                "Each FRED series must belong to exactly one domain; "
                f"conflicts: {multiply_classified}"
            )

        duplicate_curated = sorted({
            series_id
            for series_id in self.curated_series_ids
            if self.curated_series_ids.count(series_id) > 1
        })
        classified_series = set(owners)
        curated_series = set(self.curated_series_ids)
        unclassified = sorted(curated_series - classified_series)
        uncurated = sorted(classified_series - curated_series)
        if duplicate_curated or unclassified or uncurated:
            raise ValueError(
                "FRED curated series and domain classification must match: "
                f"duplicate_curated={duplicate_curated}, "
                f"unclassified={unclassified}, uncurated={uncurated}"
            )

        return {
            domain: list(self.curated_by_domain[domain])
            for domain in self.domains
        }



CONFIG = FredConfig()
