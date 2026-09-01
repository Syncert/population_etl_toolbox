"""API-derived distribution bins over the reviewed dispatch registry (API-005).

This service used to probe ``to_regclass`` and bin over whichever cross-source
union relation happened to exist, which both violated the no-silent-fallback
rule and left every non-union source with an empty page. It now dispatches
through ``apps.api.registry.OBSERVATION_DISPATCH``: the metric resolves to its
owning source, the newest value per geography is ranked inside that source's
own latest relation, and only ``analysis_ready`` sources are served — a
stratified source is declined with its declared restriction rather than
silently collapsed into meaningless bins.

The bins are explicitly API-derived (equal width over the observed range);
counts are exact counts of provider-published numeric values, and null,
suppressed, or missing values are excluded rather than coerced.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import observation_dispatch
from apps.api.schemas import DistributionBin, DistributionBinsResponse
from apps.api.services.comparison_service import (
    UnknownAnalysisMetric,
    ranked_latest_cte,
)
from apps.api.services.contracts import require_relation
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    _filter_conditions,
    _metric_conditions,
    resolve_metric,
)


def list_distribution_bins(
    db: Session,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    bin_count: int,
) -> DistributionBinsResponse:
    metric = resolve_metric(db, metric_code)
    if metric is None:
        raise UnknownAnalysisMetric("metric_code")
    source_code = str(metric.get("source_code") or "")

    dispatch = observation_dispatch(source_code)
    if not dispatch.analysis_ready:
        restriction = dispatch.analysis_restriction or (
            f"source '{source_code}' is not served by the aligned analysis routes"
        )
        raise NeutralQueryError(restriction)

    conditions, params = _metric_conditions(dispatch, metric_code, metric)
    filter_conditions, filter_params = _filter_conditions(
        dispatch, source_code, {"geo_level": geo_level, "state_fips": state_fips}
    )
    conditions.extend(filter_conditions)
    params.update(filter_params)
    require_relation(db, dispatch.latest_relation)

    base_sql = f"""
    WITH latest AS ({ranked_latest_cte(dispatch, conditions)})
    """
    stats_query = text(
        base_sql
        + """
        SELECT
            COUNT(*)::INT AS total,
            MIN(value)::DOUBLE PRECISION AS min_value,
            MAX(value)::DOUBLE PRECISION AS max_value
        FROM latest
        WHERE value IS NOT NULL
        """
    )

    stats_row = db.execute(stats_query, params).mappings().one()
    total = int(stats_row["total"] or 0)
    min_value = stats_row["min_value"]
    max_value = stats_row["max_value"]

    def _response(
        total: int,
        min_value: Optional[float],
        max_value: Optional[float],
        items: list[DistributionBin],
    ) -> DistributionBinsResponse:
        return DistributionBinsResponse(
            metric_code=metric_code,
            source_code=source_code,
            units=metric.get("units"),
            geo_level=geo_level,
            total=total,
            bin_count=bin_count,
            min_value=min_value,
            max_value=max_value,
            items=items,
        )

    if total == 0 or min_value is None or max_value is None:
        return _response(0, None, None, [])

    min_value = float(min_value)
    max_value = float(max_value)

    if min_value == max_value:
        return _response(
            total,
            min_value,
            max_value,
            [
                DistributionBin(
                    bin_index=1,
                    lower_bound=min_value,
                    upper_bound=max_value,
                    count=total,
                )
            ],
        )

    bins_query = text(
        base_sql
        + """
        SELECT
            LEAST(
                width_bucket(value, :min_value, :max_value, :bin_count),
                :bin_count
            )::INT AS bin_index,
            COUNT(*)::INT AS count
        FROM latest
        WHERE value IS NOT NULL
        GROUP BY bin_index
        ORDER BY bin_index
        """
    )
    bins_rows = (
        db.execute(
            bins_query,
            {
                **params,
                "min_value": min_value,
                "max_value": max_value,
                "bin_count": bin_count,
            },
        )
        .mappings()
        .all()
    )

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

    return _response(total, min_value, max_value, items)
