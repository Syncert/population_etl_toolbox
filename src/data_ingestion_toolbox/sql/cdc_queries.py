"""SQL query builders for the CDC source-explorer endpoints.

The builders return a ``(list_query, count_query, params)`` triple of
``sqlalchemy.text`` objects plus a params dict that can be passed directly to
``Session.execute``.

Every filter value is bound, never interpolated. The only structural choice the
caller makes is which published relation to read: the latest-release projection
or the complete published release history. Neither relation rolls county PLACES
values into state or national CDI values, and neither hides the release,
method, population basis, unit, adjustment, or uncertainty of a value.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

# Published CDC relations owned by migration 010.
LATEST_RELEASE_RELATION = "gold_cdc.latest_release_observation"
RELEASE_HISTORY_RELATION = "gold_cdc.health_observation"

# Values a consumer may filter on. The router validates before the service runs
# so an unknown value is a 422 rather than a silently empty result.
GEOGRAPHY_TYPES: tuple[str, ...] = ("nation", "state", "county")
ADJUSTMENT_STATUSES: tuple[str, ...] = ("crude", "age_adjusted", "source_specific")

# Numeric columns are rendered as text so provider precision survives JSON.
_SELECT_COLUMNS = """
    asset_id AS dataset,
    dataset_title,
    release_watermark,
    measure_id,
    measure_label,
    topic,
    value_type_id,
    value_type_label,
    period_start,
    period_end,
    geo_id,
    geo_type,
    geography_status,
    value_source,
    value::TEXT AS value,
    value_status,
    unit,
    adjustment_status,
    confidence_lower::TEXT AS confidence_lower,
    confidence_upper::TEXT AS confidence_upper,
    footnote_code,
    footnote_text,
    stratum_id,
    strata,
    estimate_method,
    population_basis,
    total_population::TEXT AS total_population,
    population_18_plus::TEXT AS population_18_plus,
    methodology_url,
    geography_basis,
    source_record_id
"""

# Deterministic paging order; observation_sk breaks any remaining tie.
_ORDER_BY = """
    ORDER BY asset_id, measure_id, value_type_id, geo_id,
             period_start, period_end, stratum_id, observation_sk
"""


def relation_for_release(release: Optional[str]) -> str:
    """Return the latest-release projection unless one release is requested."""
    return RELEASE_HISTORY_RELATION if release else LATEST_RELEASE_RELATION


def build_cdc_observation_queries(
    *,
    dataset: Optional[str] = None,
    measure_id: Optional[str] = None,
    value_type_id: Optional[str] = None,
    geo_id: Optional[str] = None,
    geo_type: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    stratum_id: Optional[str] = None,
    adjustment_status: Optional[str] = None,
    release: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[TextClause, TextClause, dict[str, object]]:
    """Build the list and count queries for one CDC observation request."""
    relation = relation_for_release(release)
    clauses: list[str] = []
    params: dict[str, object] = {"limit": limit, "offset": offset}

    if dataset:
        clauses.append("asset_id = :dataset")
        params["dataset"] = dataset
    if measure_id:
        clauses.append("measure_id = :measure_id")
        params["measure_id"] = measure_id
    if value_type_id:
        clauses.append("value_type_id = :value_type_id")
        params["value_type_id"] = value_type_id
    if geo_id:
        clauses.append("geo_id = :geo_id")
        params["geo_id"] = geo_id
    if geo_type:
        clauses.append("geo_type = :geo_type")
        params["geo_type"] = geo_type
    if year_from is not None:
        clauses.append("period_end >= :year_from")
        params["year_from"] = year_from
    if year_to is not None:
        clauses.append("period_start <= :year_to")
        params["year_to"] = year_to
    if stratum_id:
        clauses.append("stratum_id = :stratum_id")
        params["stratum_id"] = stratum_id
    if adjustment_status:
        clauses.append("adjustment_status = :adjustment_status")
        params["adjustment_status"] = adjustment_status
    if release:
        clauses.append("release_watermark = :release")
        params["release"] = release

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    list_query = text(
        f"SELECT {_SELECT_COLUMNS} FROM {relation} {where_clause} {_ORDER_BY} "
        "LIMIT :limit OFFSET :offset"
    )
    count_query = text(f"SELECT COUNT(*) FROM {relation} {where_clause}")
    return list_query, count_query, params
