from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import DistributionBin, DistributionBinsResponse


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


def list_distribution_bins(
    db: Session,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    bin_count: int,
) -> DistributionBinsResponse:
    relation_name = _latest_relation_name(db)

    stats_query = text(
        f"""
        SELECT
            COUNT(*)::int AS total,
            MIN(value)::double precision AS min_value,
            MAX(value)::double precision AS max_value
        FROM {relation_name}
        WHERE metric_code = :metric_code
          AND (:geo_level IS NULL OR geo_level = :geo_level)
          AND (:state_fips IS NULL OR state_fips = :state_fips)
          AND value IS NOT NULL
        """
    )
    params = {
        "metric_code": metric_code,
        "geo_level": geo_level,
        "state_fips": state_fips,
        "bin_count": bin_count,
    }

    stats_row = db.execute(stats_query, params).mappings().one()
    total = int(stats_row["total"] or 0)
    min_value = stats_row["min_value"]
    max_value = stats_row["max_value"]

    if total == 0 or min_value is None or max_value is None:
        return DistributionBinsResponse(
            metric_code=metric_code,
            geo_level=geo_level,
            total=0,
            bin_count=bin_count,
            min_value=None,
            max_value=None,
            items=[],
        )

    min_value = float(min_value)
    max_value = float(max_value)

    if min_value == max_value:
        return DistributionBinsResponse(
            metric_code=metric_code,
            geo_level=geo_level,
            total=total,
            bin_count=bin_count,
            min_value=min_value,
            max_value=max_value,
            items=[
                DistributionBin(
                    bin_index=1,
                    lower_bound=min_value,
                    upper_bound=max_value,
                    count=total,
                )
            ],
        )

    bins_query = text(
        f"""
        SELECT
            LEAST(
                width_bucket(value::double precision, :min_value, :max_value, :bin_count),
                :bin_count
            )::int AS bin_index,
            COUNT(*)::int AS count
        FROM {relation_name}
        WHERE metric_code = :metric_code
          AND (:geo_level IS NULL OR geo_level = :geo_level)
          AND (:state_fips IS NULL OR state_fips = :state_fips)
          AND value IS NOT NULL
        GROUP BY bin_index
        ORDER BY bin_index
        """
    )

    bins_rows = db.execute(
        bins_query,
        {
            **params,
            "min_value": min_value,
            "max_value": max_value,
        },
    ).mappings().all()

    width = (max_value - min_value) / float(bin_count)
    items: list[DistributionBin] = []
    for row in bins_rows:
        bin_index = int(row["bin_index"])
        lower = min_value + (bin_index - 1) * width
        upper = max_value if bin_index == bin_count else min_value + bin_index * width
        items.append(
            DistributionBin(
                bin_index=bin_index,
                lower_bound=lower,
                upper_bound=upper,
                count=int(row["count"]),
            )
        )

    return DistributionBinsResponse(
        metric_code=metric_code,
        geo_level=geo_level,
        total=total,
        bin_count=bin_count,
        min_value=min_value,
        max_value=max_value,
        items=items,
    )
