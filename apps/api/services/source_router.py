"""
apps/api/services/source_router.py
Source detection and table routing for per-source schema architecture.

This module provides utilities for working with the source-first schema design
where each data source (BLS, ACS, FRED) has its own serving tables in separate schemas.
"""

from typing import Optional, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session


SOURCE_TABLE_MAP = {
    'BLS': {
        'reporting': 'gold_bls.rpt_bls_observations',
        'latest': 'gold_bls.mv_bls_latest',
    },
    'CENSUS_ACS': {
        'reporting': 'gold_census.rpt_acs_observations',
        'latest': 'gold_census.mv_acs_latest',
    },
    'FRED': {
        'reporting': 'gold_fred.rpt_fred_observations',
        'latest': 'gold_fred.mv_fred_latest',
    },
}


def get_source_from_metric(db: Session, metric_code: str) -> Optional[str]:
    """
    Look up the source_code for a given metric_code from the metric catalog.
    
    Returns: 'BLS', 'CENSUS_ACS', 'FRED', or None if not found.
    """
    query = text("""
        SELECT source_code
        FROM gold_glossary.dim_metric_catalog
        WHERE metric_code = :metric_code
          AND is_active = TRUE
        LIMIT 1
    """)
    result = db.execute(query, {"metric_code": metric_code}).scalar()
    return result


def get_sources_from_metrics(db: Session, metric_codes: list[str]) -> dict[str, str]:
    """
    Batch lookup sources for multiple metric codes.
    
    Returns: Dict mapping metric_code -> source_code
    """
    if not metric_codes:
        return {}
    
    query = text("""
        SELECT metric_code, source_code
        FROM gold_glossary.dim_metric_catalog
        WHERE metric_code = ANY(:metric_codes)
          AND is_active = TRUE
    """)
    results = db.execute(query, {"metric_codes": metric_codes}).mappings().all()
    return {row['metric_code']: row['source_code'] for row in results}


def get_table_for_source(source_code: str, table_type: str = 'latest') -> str:
    """
    Map source_code and table_type to the fully qualified table name.
    
    Args:
        source_code: 'BLS', 'CENSUS_ACS', or 'FRED'
        table_type: 'reporting' (rpt_*) or 'latest' (mv_*_latest)
    
    Returns:
        Fully qualified table name, e.g., 'gold_bls.mv_bls_latest'
    """
    if source_code not in SOURCE_TABLE_MAP:
        raise ValueError(f"Unknown source: {source_code}")
    return SOURCE_TABLE_MAP[source_code][table_type]


def validate_sources_for_comparison(
    db: Session,
    metric_codes: list[str]
) -> Tuple[bool, Optional[str]]:
    """
    Validate that all metric codes belong to the same source (for comparisons).
    
    Returns:
        (is_valid: bool, error_message: Optional[str])
    """
    if not metric_codes:
        return False, "No metric codes provided"
    
    sources = get_sources_from_metrics(db, metric_codes)
    
    # Check all metrics were found
    if len(sources) != len(metric_codes):
        missing = set(metric_codes) - set(sources.keys())
        return False, f"Metrics not found in catalog: {missing}"
    
    # Check all metrics have the same source
    unique_sources = set(sources.values())
    if len(unique_sources) > 1:
        return False, f"Cannot compare across sources: {unique_sources}. Use cross-source views for multi-source comparisons."
    
    return True, None


def build_source_aware_query(
    metric_code: str,
    db: Session,
    table_type: str = 'latest',
    columns: Optional[list[str]] = None
) -> Tuple[str, str]:
    """
    Build a query fragment for fetching observations from the correct source table.
    
    Returns:
        (source_code, table_name)
    """
    source_code = get_source_from_metric(db, metric_code)
    if not source_code:
        raise ValueError(f"Metric {metric_code} not found in catalog")
    
    table_name = get_table_for_source(source_code, table_type)
    return source_code, table_name


# Source-specific column defaults (for graceful degradation)
SOURCE_COMMON_COLUMNS = {
    'BLS': [
        'source_code', 'observation_date', 'geo_id', 'geo_level',
        'state_fips', 'county_fips', 'state_name', 'county_name',
        'geo_latitude', 'geo_longitude', 'metric_code', 'metric_display_name',
        'value', 'value_type', 'units', 'series_id', 'program_code'
    ],
    'CENSUS_ACS': [
        'source_code', 'observation_date', 'geo_id', 'geo_level',
        'state_fips', 'county_fips', 'state_name', 'county_name',
        'geo_latitude', 'geo_longitude', 'metric_code', 'metric_display_name',
        'value', 'value_type', 'dataset_code', 'vintage_year', 'variable_code'
    ],
    'FRED': [
        'source_code', 'observation_date', 'geo_id', 'geo_level',
        'metric_code', 'metric_display_name', 'value', 'value_type',
        'series_id', 'frequency', 'units'
    ],
}


def get_normalized_columns(source_code: str) -> list[str]:
    """
    Get the list of common queryable columns for a source.
    Use this to build safe cross-source SELECT clauses that won't fail.
    """
    return SOURCE_COMMON_COLUMNS.get(source_code, SOURCE_COMMON_COLUMNS['BLS'])
