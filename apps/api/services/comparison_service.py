from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import ComparisonResponse, ComparisonRow


def _relation_exists(db: Session, relation_name: str) -> bool:
    if not hasattr(db, "bind"):
        return True

    exists_query = text("SELECT to_regclass(:relation_name) IS NOT NULL")
    exists = db.execute(exists_query, {"relation_name": relation_name}).scalar()
    if exists is None:
        return True
    return bool(exists)


def _latest_relation_name(db: Session) -> str:
    if _relation_exists(db, "gold.v_metric_latest_by_geo"):
        return "gold.v_metric_latest_by_geo"
    return "gold.mv_latest_dashboard"


def list_metric_comparison(
    db: Session,
    metric_code_a: str,
    metric_code_b: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ComparisonResponse:
    relation_name = _latest_relation_name(db)

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
        FROM {relation_name}
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
        FROM {relation_name}
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
        JOIN b ON a.geo_id = b.geo_id
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
            :metric_code_a AS metric_code_a,
            :metric_code_b AS metric_code_b,
            value_a,
            value_b,
            difference,
            ratio
        FROM joined
        ORDER BY geo_id
        LIMIT :limit OFFSET :offset
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
        metric_code_a=metric_code_a,
        metric_code_b=metric_code_b,
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )
