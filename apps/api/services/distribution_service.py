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
    dispatch_for_metric,
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

    # Through the shared helper, not the registry: a metric whose source has
    # no reviewed entry is a 422 that names the source and points at
    # /catalog/capabilities, exactly as /observations answers it. Reaching
    # into the registry here raised a KeyError this route did not catch
    # (API-078).
    dispatch = dispatch_for_metric(metric)
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

    # One statement, one snapshot, one evaluation of the reduction.
    #
    # The range and the counts used to be two executions of this CTE. Each
    # took its own snapshot, so a `REFRESH MATERIALIZED VIEW CONCURRENTLY`
    # committing between them -- which is what the relation is for -- left
    # `min_value` describing rows the counts no longer measured. A value
    # published below it buckets to 0, a bin `items` never asks for, so the
    # geography disappeared from the bins while `total` still counted it;
    # one published above it was clamped into a last bin whose upper bound
    # the response reported as a maximum it was not. A CTE referenced more
    # than once is evaluated once, which makes both impossible rather than
    # handled (API-084).
    #
    # `width_bucket` rejects a range whose bounds are equal, so a measure
    # with one distinct value bins against an upper bound one unit above it:
    # every value is then the lower bound and falls in bin 1, and the
    # degenerate answer below replaces the bins anyway.
    query = text(
        f"""
    WITH latest AS ({ranked_latest_cte(dispatch, conditions)}),
    published AS (
        SELECT value FROM latest WHERE value IS NOT NULL
    ),
    stats AS (
        SELECT
            COUNT(*)::INT AS total,
            MIN(value)::DOUBLE PRECISION AS min_value,
            MAX(value)::DOUBLE PRECISION AS max_value
        FROM published
    ),
    binned AS (
        SELECT
            LEAST(
                width_bucket(
                    published.value,
                    stats.min_value,
                    CASE
                        WHEN stats.max_value = stats.min_value
                        THEN stats.min_value + 1
                        ELSE stats.max_value
                    END,
                    :bin_count
                ),
                :bin_count
            )::INT AS bin_index,
            COUNT(*)::INT AS count
        FROM published CROSS JOIN stats
        GROUP BY 1
    )
    SELECT stats.total, stats.min_value, stats.max_value,
           binned.bin_index, binned.count
    FROM stats LEFT JOIN binned ON TRUE
    ORDER BY binned.bin_index
    """
    )

    rows = db.execute(query, {**params, "bin_count": bin_count}).mappings().all()
    # `stats` always produces exactly one row -- an aggregate over no rows is
    # still a row -- so the LEFT JOIN answers the range even when nothing was
    # published, and `bin_index` is null on that row.
    first = rows[0]
    total = int(first["total"] or 0)
    min_value = first["min_value"]
    max_value = first["max_value"]
    counts = {
        int(row["bin_index"]): int(row["count"])
        for row in rows
        if row["bin_index"] is not None
    }

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

    # Every bin the caller asked for, including the ones nothing falls into
    # (API-079). ``GROUP BY`` returns no row for an empty bin, and an absent
    # bin and a bin holding zero geographies are different statements: the
    # second is a fact this query measured, and reporting it as the first
    # makes every consumer rebuild the gaps from min/max.
    width = (max_value - min_value) / float(bin_count)
    items = [
        DistributionBin(
            bin_index=bin_index,
            lower_bound=min_value + (bin_index - 1) * width,
            # The last bin closes on the observed maximum rather than on
            # min + n*width, so floating-point width never leaves the largest
            # value outside the range it was binned into.
            upper_bound=(
                max_value if bin_index == bin_count else min_value + bin_index * width
            ),
            count=counts.get(bin_index, 0),
        )
        for bin_index in range(1, bin_count + 1)
    ]

    return _response(total, min_value, max_value, items)
