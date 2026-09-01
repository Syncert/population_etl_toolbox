"""Deterministic contracts for parameterized API SQL query builders."""

from __future__ import annotations

from datetime import date

import pytest

from data_ingestion_toolbox.sql import catalog_queries, observation_queries

pytestmark = [pytest.mark.unit, pytest.mark.api]


def test_metric_query_builder_binds_every_filter() -> None:
    """Covers: API-010, API-017 — metric filters remain bound parameters."""
    view = "gold_glossary.dim_metric"
    list_query, count_query, params = catalog_queries.build_metrics_queries(
        "ACS", True, "population", 25, 50
    )
    rendered_list = str(list_query)
    rendered_count = str(count_query)

    assert f"FROM {view}" in rendered_list
    assert f"FROM {view}" in rendered_count
    assert "UPPER(source_code) = UPPER(:source_code)" in rendered_list
    assert "is_active = TRUE" in rendered_list
    assert "dashboard_suitability" not in rendered_list
    assert "UPPER(metric_code) LIKE UPPER(:q)" in rendered_list
    assert params == {
        "limit": 25,
        "offset": 50,
        "source_code": "ACS",
        "q": "%population%",
    }
    assert "population" not in rendered_list


def test_metric_query_builder_uses_true_for_no_filters() -> None:
    """Covers: API-010 — omitted metric filters produce an unfiltered page."""
    list_query, count_query, params = catalog_queries.build_metrics_queries(
        None, False, None, 10, 0
    )

    assert "WHERE TRUE" in str(list_query)
    assert "WHERE TRUE" in str(count_query)
    assert params == {"limit": 10, "offset": 0}


def test_geography_query_builder_binds_every_filter() -> None:
    """Covers: API-010, API-017 — geography filters remain bound parameters."""
    view = "gold_glossary.dim_geography"
    list_query, count_query, params = catalog_queries.build_geographies_queries(
        "county", "06", "Alameda", 20, 40
    )
    rendered_list = str(list_query)

    assert f"FROM {view}" in rendered_list
    assert f"FROM {view}" in str(count_query)
    assert "UPPER(geo_level) = UPPER(:geo_level)" in rendered_list
    assert "state_fips = :state_fips" in rendered_list
    assert "UPPER(geo_name) LIKE UPPER(:q)" in rendered_list
    assert params == {
        "limit": 20,
        "offset": 40,
        "geo_level": "county",
        "state_fips": "06",
        "q": "%Alameda%",
    }
    assert "Alameda" not in rendered_list


def test_geography_query_builder_uses_true_for_no_filters() -> None:
    """Covers: API-010 — omitted geography filters produce an unfiltered page."""
    list_query, count_query, params = catalog_queries.build_geographies_queries(
        None, None, None, 10, 0
    )

    assert "WHERE TRUE" in str(list_query)
    assert "WHERE TRUE" in str(count_query)
    assert params == {"limit": 10, "offset": 0}


def test_latest_query_builder_binds_every_filter() -> None:
    """Covers: API-010, API-017 — latest filters are parameterized."""
    list_query, count_query, params = observation_queries.build_latest_mv_queries(
        "POP_TOTAL", "state", "06", 5, 10
    )
    rendered_list = str(list_query)

    assert "FROM gold.v_metric_latest_by_geo" in rendered_list
    assert "FROM gold.v_metric_latest_by_geo" in str(count_query)
    assert "metric_code = :metric_code" in rendered_list
    assert "UPPER(geo_level) = UPPER(:geo_level)" in rendered_list
    assert "state_fips = :state_fips" in rendered_list
    assert params == {
        "limit": 5,
        "offset": 10,
        "metric_code": "POP_TOTAL",
        "geo_level": "state",
        "state_fips": "06",
    }
    assert "POP_TOTAL" not in rendered_list


def test_latest_fallback_query_builder_ranks_each_geography() -> None:
    """Covers: API-027 — durable fallback ranks the latest geography row."""
    list_query, count_query, params = (
        observation_queries.build_latest_rpt_fallback_queries(
            "UNEMP", "county", "06", 5, 0
        )
    )
    rendered_list = str(list_query)

    assert "FROM gold.v_metric_timeseries_by_geo" in rendered_list
    assert "ROW_NUMBER() OVER (PARTITION BY geo_id" in rendered_list
    assert "WHERE rn = 1" in rendered_list
    assert "SELECT COUNT(*) FROM ranked WHERE rn = 1" in str(count_query)
    assert params["metric_code"] == "UNEMP"


def _select_output_names(select_sql: str) -> list[str]:
    """Return each select entry's output name, ignoring commas inside calls."""
    names: list[str] = []
    depth = 0
    current = ""
    for character in select_sql:
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        if character == "," and depth == 0:
            names.append(current)
            current = ""
        else:
            current += character
    names.append(current)
    return [
        entry.strip().rsplit(" AS ", 1)[-1].strip() for entry in names if entry.strip()
    ]


def test_latest_fallback_projects_every_column_without_duckdb_syntax() -> None:
    """Covers: API-027 — the ranked fallback is valid PostgreSQL.

    ``SELECT * EXCEPT(rn)`` parses in DuckDB and BigQuery and nowhere in
    PostgreSQL, so the fallback raised a ProgrammingError on the exact request
    it exists to serve: a metric the primary latest view has no row for. The
    endpoint answered 503 instead of an empty result.
    """
    list_query, _count_query, _params = (
        observation_queries.build_latest_rpt_fallback_queries("UNEMP", None, None, 5, 0)
    )
    rendered = str(list_query)

    assert "EXCEPT(" not in rendered
    projection = rendered.split("FROM ranked", 1)[0].rsplit(" SELECT ", 1)[-1]
    assert _select_output_names(projection) == list(
        observation_queries._OBSERVATION_COLUMNS
    )


def test_fallback_projection_matches_the_ranked_select_list() -> None:
    """Covers: API-027 — the projection cannot drift from the ranked CTE."""
    assert _select_output_names(observation_queries._OBSERVATION_SELECT) == list(
        observation_queries._OBSERVATION_COLUMNS
    )


def test_timeseries_query_builder_binds_its_date_window() -> None:
    """Covers: API-010, API-012 — timeseries binds and orders its window."""
    start = date(2024, 1, 1)
    end = date(2024, 12, 31)
    list_query, count_query, params = observation_queries.build_timeseries_queries(
        "UNEMP", "county:06001", start, end, 100
    )
    rendered_list = str(list_query)

    assert "FROM gold.v_metric_timeseries_by_geo" in rendered_list
    assert "FROM gold.v_metric_timeseries_by_geo" in str(count_query)
    assert "observation_date >= :start_date" in rendered_list
    assert "observation_date <= :end_date" in rendered_list
    assert "ORDER BY observation_date ASC" in rendered_list
    assert params == {
        "limit": 100,
        "metric_code": "UNEMP",
        "geo_id": "county:06001",
        "start_date": start,
        "end_date": end,
    }


def test_timeseries_query_omits_absent_date_filters() -> None:
    """Covers: API-010, API-012 — optional dates stay absent when omitted."""
    list_query, _count_query, params = observation_queries.build_timeseries_queries(
        "POP_TOTAL", "state:06", None, None, 50
    )
    rendered_list = str(list_query)

    assert ":start_date" not in rendered_list
    assert ":end_date" not in rendered_list
    assert params == {
        "limit": 50,
        "metric_code": "POP_TOTAL",
        "geo_id": "state:06",
    }
