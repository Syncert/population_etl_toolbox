"""Observation reads over the reviewed serving contracts.

Relation names come from ``apps.api.registry`` and from the two cross-source
contract views named below. Nothing here derives a relation from request text.

Two fallbacks used to live in this module and no longer do. One chose between an
"MVP" and a "legacy" select list by probing ``information_schema`` for four
columns; the other answered a source-specific route from the cross-source schema
when the source's own schema appeared to be missing. Neither could fire on a
warehouse built from ``sql/bootstrap/warehouse_manifest.json`` -- the contract
views it creates carry those four columns for every source -- and the second was
actively unsafe, because answering ``/api/bls/...`` from the cross-source union
returns rows the caller did not ask for under a name that says they did. A
missing serving contract is now a deployment fault that says so.

The one remaining fallback is deliberate and tested (API-027): when the latest
materialized view holds no rows for a metric, the cross-source read falls back to
the durable reporting relation and ranks the newest row per geography. That is
not a guess about which relation exists -- both are declared -- it is a refresh
window, and serving stale-but-real history beats serving an empty page.
"""

from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.registry import ServingContract, serving_contract
from apps.api.schemas import ObservationDashboard, ObservationListResponse
from data_ingestion_toolbox.sql.observation_queries import (
    build_latest_mv_queries,
    build_latest_rpt_fallback_queries,
    build_timeseries_queries,
)

#: The provider-neutral contract views. They union the three sources that
#: predate the per-source serving contracts; API-004 replaces this pair with
#: registry dispatch so every source is reachable through the neutral routes.
CROSS_SOURCE_LATEST_RELATION = "gold.v_metric_latest_by_geo"
CROSS_SOURCE_HISTORY_RELATION = "gold.v_metric_timeseries_by_geo"


class ServingContractUnavailable(RuntimeError):
    """A relation the API declares a dependency on is absent from the warehouse.

    This is a deployment fault, not a client error: the bootstrap manifest did
    not run, ran partially, or ran against a different database than the one the
    API is pointed at. It is raised rather than absorbed so the failure names the
    missing relation in the server log instead of surfacing as an empty page that
    looks like "this metric has no data".
    """


def _relation_is_absent(db: Session, relation_name: str) -> bool:
    """True only when the database positively reports the relation missing.

    A session that cannot answer the question -- a stub in a deterministic unit
    test, or a driver that raises -- is not evidence of absence, so the check
    stays silent rather than inventing a deployment fault from a test double.
    """
    if not hasattr(db, "bind"):
        return False
    try:
        exists = db.execute(
            text("SELECT to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": relation_name},
        ).scalar()
    except SQLAlchemyError:
        return False
    if exists is None:
        return False
    return not bool(exists)


def _require_relation(db: Session, relation_name: str) -> None:
    if _relation_is_absent(db, relation_name):
        raise ServingContractUnavailable(
            f"required serving relation is not present: {relation_name}"
        )


def _source_select_sql(contract: ServingContract) -> str:
    """Project one source's rows onto the shared observation contract.

    A field the source does not publish is selected as a typed ``NULL`` rather
    than omitted, so every source returns the same column set and a consumer
    reading ``margin_of_error`` gets "this source publishes none" instead of a
    missing key.
    """
    seasonal_expr = (
        "seasonal_adjustment_status"
        if contract.publishes_seasonal_adjustment
        else "NULL::TEXT AS seasonal_adjustment_status"
    )
    if contract.publishes_vintage_and_error:
        dataset_expr = "dataset_code"
        dataset_alias_expr = "dataset_code AS dataset"
        vintage_year_expr = "vintage_year"
        vintage_expr = "vintage_year::TEXT AS vintage"
        moe_expr = "margin_of_error::TEXT AS margin_of_error"
        moe_pct_expr = "margin_of_error_pct::TEXT AS margin_of_error_pct"
    else:
        dataset_expr = "NULL::TEXT AS dataset_code"
        dataset_alias_expr = "NULL::TEXT AS dataset"
        vintage_year_expr = "NULL::INT AS vintage_year"
        vintage_expr = "NULL::TEXT AS vintage"
        moe_expr = "NULL::TEXT AS margin_of_error"
        moe_pct_expr = "NULL::TEXT AS margin_of_error_pct"

    return f"""
        source_code,
        source_code AS source,
        observation_date,
        observation_date::TEXT AS period,
        duration_start,
        duration_end,
        time_sk,
        as_of_date,
        as_of_date AS release_date,
        updated_at,
        geo_id,
        geo_level,
        {contract.geo_name_expression} AS geo_name,
        state_fips,
        county_fips,
        state_name,
        county_name,
        geo_latitude,
        geo_longitude,
        metric_code,
        metric_display_name,
        value::TEXT AS value,
        value_type,
        units,
        units AS unit,
        {seasonal_expr},
        {dataset_expr},
        {dataset_alias_expr},
        {vintage_year_expr},
        {vintage_expr},
        {moe_expr},
        {moe_pct_expr}
    """


def _rows_to_response(
    rows, total: int, limit: int, offset: int
) -> ObservationListResponse:
    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=offset, items=items)


def list_latest_observations(
    db: Session,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ObservationListResponse:
    """Newest cross-source values, falling back to durable history when empty."""
    _require_relation(db, CROSS_SOURCE_LATEST_RELATION)

    mv_list_query, mv_count_query, mv_params = build_latest_mv_queries(
        metric_code=metric_code,
        geo_level=geo_level,
        state_fips=state_fips,
        limit=limit,
        offset=offset,
    )
    total = int(db.execute(mv_count_query, mv_params).scalar() or 0)
    rows = db.execute(mv_list_query, mv_params).mappings().all()

    if total == 0:
        # Covers API-027: the latest view refreshes independently of the durable
        # reporting relation, so an empty page here means "not refreshed yet",
        # not "no such data".
        _require_relation(db, CROSS_SOURCE_HISTORY_RELATION)
        rpt_list_query, rpt_count_query, rpt_params = build_latest_rpt_fallback_queries(
            metric_code=metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )
        total = int(db.execute(rpt_count_query, rpt_params).scalar() or 0)
        rows = db.execute(rpt_list_query, rpt_params).mappings().all()

    return _rows_to_response(rows, total, limit, offset)


def list_timeseries_observations(
    db: Session,
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> ObservationListResponse:
    """As-published cross-source history for one geography."""
    _require_relation(db, CROSS_SOURCE_HISTORY_RELATION)

    list_query, count_query, params = build_timeseries_queries(
        metric_code=metric_code,
        geo_id=geo_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    return _rows_to_response(rows, total, limit, offset=0)


def list_latest_observations_for_source(
    db: Session,
    source: str,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ObservationListResponse:
    """Newest values from one source's own serving contract."""
    contract = serving_contract(source)
    _require_relation(db, contract.latest_relation)

    where_clauses = ["metric_code = :metric_code"]
    params: dict = {"metric_code": metric_code, "limit": limit, "offset": offset}
    if geo_level:
        where_clauses.append("UPPER(geo_level) = UPPER(:geo_level)")
        params["geo_level"] = geo_level
    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips
    where_sql = " AND ".join(where_clauses)

    list_query = text(
        f"""
        SELECT
            {_source_select_sql(contract)}
        FROM {contract.latest_relation}
        WHERE {where_sql}
        ORDER BY geo_id ASC
        LIMIT :limit OFFSET :offset
        """
    )
    count_query = text(
        f"""
        SELECT COUNT(*)
        FROM {contract.latest_relation}
        WHERE {where_sql}
        """
    )

    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    return _rows_to_response(rows, total, limit, offset)


def list_timeseries_observations_for_source(
    db: Session,
    source: str,
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> ObservationListResponse:
    """As-published history from one source's own durable serving contract."""
    contract = serving_contract(source)
    _require_relation(db, contract.history_relation)

    where_clauses = ["metric_code = :metric_code", "geo_id = :geo_id"]
    params: dict = {"metric_code": metric_code, "geo_id": geo_id, "limit": limit}
    if start_date:
        where_clauses.append("observation_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        where_clauses.append("observation_date <= :end_date")
        params["end_date"] = end_date
    where_sql = " AND ".join(where_clauses)

    list_query = text(
        f"""
        SELECT
            {_source_select_sql(contract)}
        FROM {contract.history_relation}
        WHERE {where_sql}
        ORDER BY observation_date ASC
        LIMIT :limit
        """
    )
    count_query = text(
        f"""
        SELECT COUNT(*)
        FROM {contract.history_relation}
        WHERE {where_sql}
        """
    )

    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    return _rows_to_response(rows, total, limit, offset=0)
