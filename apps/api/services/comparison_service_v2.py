"""
apps/api/services/comparison_service_v2.py
Updated comparison service for source-first architecture.

This module handles comparisons within a single source. For cross-source comparisons,
use explicitly created views (e.g., gold.v_labor_vs_income_comparison).
"""

from typing import Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import ComparisonResponse, ComparisonRow
from apps.api.services.source_router import (
    get_sources_from_metrics,
    validate_sources_for_comparison,
    get_table_for_source,
)


def list_metric_comparison(
    db: Session,
    metric_code_a: str,
    metric_code_b: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ComparisonResponse:
    """
    Compare two metrics from the same source.

    For cross-source comparisons (e.g., BLS unemployment vs ACS median income),
    use a source-specific view created in gold schema (e.g., gold.v_labor_vs_income).

    Raises:
        ValueError: If metrics are from different sources
    """
    # Validate both metrics exist and are from the same source
    is_valid, error_msg = validate_sources_for_comparison(
        db, [metric_code_a, metric_code_b]
    )
    if not is_valid:
        raise ValueError(error_msg)

    # Get the source and route to the correct table
    sources = get_sources_from_metrics(db, [metric_code_a, metric_code_b])
    source_code = sources[metric_code_a]  # Both guaranteed to have same source
    table_name = get_table_for_source(source_code, table_type="latest")

    base_sql = f"""
    WITH a AS (
        SELECT
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            value::double precision AS value_a
        FROM {table_name}
        WHERE metric_code = :metric_code_a
          AND (:geo_level IS NULL OR geo_level = :geo_level)
          AND (:state_fips IS NULL OR state_fips = :state_fips)
    ),
    b AS (
        SELECT
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            value::double precision AS value_b
        FROM {table_name}
        WHERE metric_code = :metric_code_b
          AND (:geo_level IS NULL OR geo_level = :geo_level)
          AND (:state_fips IS NULL OR state_fips = :state_fips)
    ),
    joined AS (
        SELECT
            a.geo_id,
            COALESCE(a.geo_level, b.geo_level) AS geo_level,
            COALESCE(a.state_fips, b.state_fips) AS state_fips,
            COALESCE(a.county_fips, b.county_fips) AS county_fips,
            COALESCE(a.state_name, b.state_name) AS state_name,
            COALESCE(a.county_name, b.county_name) AS county_name,
            a.value_a,
            b.value_b,
            (a.value_a - b.value_b) AS difference,
            CASE
                WHEN b.value_b IS NULL OR b.value_b = 0 THEN NULL
                ELSE a.value_a / b.value_b
            END AS ratio
        FROM a
        FULL OUTER JOIN b ON a.geo_id = b.geo_id
    )
    """

    count_query = text(
        base_sql
        + """
        SELECT COUNT(*)::int AS total
        FROM joined
        """
    )

    list_query = text(
        base_sql
        + """
        SELECT
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            value_a,
            value_b,
            difference,
            ratio
        FROM joined
        ORDER BY geo_id
        LIMIT :limit
        OFFSET :offset
        """
    )

    params = {
        "metric_code_a": metric_code_a,
        "metric_code_b": metric_code_b,
        "geo_level": geo_level,
        "state_fips": state_fips,
        "limit": limit,
        "offset": offset,
    }

    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()

    items = [ComparisonRow.model_validate(row) for row in rows]
    return ComparisonResponse(
        metric_a=metric_code_a,
        metric_b=metric_code_b,
        total=total,
        limit=limit,
        offset=offset,
        rows=items,
    )


def list_metric_comparison_cross_source(
    db: Session,
    metric_code_a: str,
    metric_code_b: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ComparisonResponse:
    """
    Compare two metrics from DIFFERENT sources using an explicit cross-source view.

    This function looks for a view named after the two metrics, e.g.:
    - gold.v_bls_vs_acs_labor_income
    - gold.v_labor_unemployment_vs_fred_rates

    You must create these views explicitly for each cross-source use case.

    Args:
        db: Database session
        metric_code_a: First metric (any source)
        metric_code_b: Second metric (different source)
        geo_level: Geographic level filter
        state_fips: State FIPS filter
        limit: Pagination limit
        offset: Pagination offset

    Returns:
        ComparisonResponse with results from the cross-source view

    Raises:
        ValueError: If the cross-source view doesn't exist
    """
    # For now, raise an error directing users to create a view
    raise NotImplementedError(
        f"Cross-source comparison not yet configured. "
        f"Create a view in gold schema for {metric_code_a} vs {metric_code_b} "
        f"and call this service with the view name."
    )
