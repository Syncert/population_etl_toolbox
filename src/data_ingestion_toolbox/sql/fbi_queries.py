"""SQL query builders for the derived FBI county roll-up endpoint.

The builders return ``(list_query, count_query, params)`` triples of
``sqlalchemy.text`` objects plus a params dict that can be passed directly
to ``Session.execute``. Every filter value is bound, never interpolated.

The relations read here are the declared-derived roll-up publications
(ETL-053). The queries add no policy of their own: derivation labeling,
multi-county handling, coverage, and the no-zero rule all live in the gold
view definition, and a row leaves these queries exactly as published.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

LATEST_ROLLUP_RELATION = "gold_fbi.latest_county_rollup"
ROLLUP_HISTORY_RELATION = "gold_fbi.county_rollup"

#: Resolved agency-to-county mapping evidence for the refusal check: a
#: county nothing maps to is refused explicitly rather than served an
#: indistinguishable empty page.
MAPPED_AGENCY_RELATION = "gold_fbi.agency_geography"

# Numeric columns are rendered as text so summed provider precision
# survives JSON; counts and flags keep their native types.
_SELECT_COLUMNS = """
    product_id,
    release_key AS release,
    refresh_date::TEXT AS refresh_date,
    ucr_program,
    offense_code,
    offense_label,
    measure_id,
    measure_form,
    counted_entity_basis,
    unit,
    geo_id,
    county_name,
    state_fips,
    county_fips,
    period,
    period_start::TEXT AS period_start,
    period_end::TEXT AS period_end,
    value::TEXT AS value,
    contributing_oris,
    reporting_agency_count,
    mapped_agency_count,
    includes_multi_county_agency,
    derived,
    derivation_method,
    result_label,
    methodology_note,
    counted_entity_note,
    methodology_url,
    documentation_url
"""

# Deterministic paging order; the roll-up key itself breaks every tie.
_ORDER_BY = """
    ORDER BY product_id, measure_id, geo_id, period_start, release_key
"""


def rollup_relation_for_release(release: Optional[str]) -> str:
    """Return the latest-release projection unless one release is requested."""
    return ROLLUP_HISTORY_RELATION if release else LATEST_ROLLUP_RELATION


def build_county_rollup_queries(
    *,
    product_id: Optional[str] = None,
    measure_id: Optional[str] = None,
    geo_id: Optional[str] = None,
    state_fips: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    release: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[TextClause, TextClause, dict[str, object]]:
    """Build the list and count queries for one county roll-up request."""
    relation = rollup_relation_for_release(release)
    clauses: list[str] = []
    params: dict[str, object] = {"limit": limit, "offset": offset}

    if product_id:
        clauses.append("product_id = :product_id")
        params["product_id"] = product_id
    if measure_id:
        clauses.append("measure_id = :measure_id")
        params["measure_id"] = measure_id
    if geo_id:
        clauses.append("geo_id = :geo_id")
        params["geo_id"] = geo_id
    if state_fips:
        clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips
    if year_from is not None:
        clauses.append("period_end >= MAKE_DATE(:year_from, 1, 1)")
        params["year_from"] = year_from
    if year_to is not None:
        clauses.append("period_start <= MAKE_DATE(:year_to, 12, 31)")
        params["year_to"] = year_to
    if release:
        clauses.append("release_key = :release")
        params["release"] = release

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    list_query = text(
        f"SELECT {_SELECT_COLUMNS} FROM {relation} {where_clause} {_ORDER_BY} "
        "LIMIT :limit OFFSET :offset"
    )
    count_query = text(f"SELECT COUNT(*) FROM {relation} {where_clause}")
    return list_query, count_query, params


def build_county_mapping_evidence_query() -> TextClause:
    """Count resolved and non-resolved county mapping rows for one county.

    The resolved count says whether any agency is mapped to the county at
    all; the unresolved/ambiguous count says whether provider county labels
    exist that this pipeline could not resolve, which is a different fact
    (ETL-050) and is reported with the refusal rather than guessed around.
    """
    return text(
        f"""
        SELECT
            (SELECT COUNT(*)
             FROM {MAPPED_AGENCY_RELATION}
             WHERE relationship_type = 'county'
               AND resolution_status = 'resolved'
               AND geo_id = :geo_id) AS resolved_count,
            (SELECT COUNT(*)
             FROM {MAPPED_AGENCY_RELATION} AS county_label
             WHERE county_label.relationship_type = 'county'
               AND county_label.resolution_status IN ('unresolved', 'ambiguous')
               AND EXISTS (
                   SELECT 1
                   FROM {MAPPED_AGENCY_RELATION} AS state_link
                   WHERE state_link.ori = county_label.ori
                     AND state_link.relationship_type = 'state'
                     AND state_link.geo_id = :state_geo_id
               )) AS unresolved_count
        """
    )
