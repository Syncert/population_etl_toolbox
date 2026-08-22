"""Deterministic contracts for parameterized API SQL query builders."""

from __future__ import annotations

from datetime import date

import pytest

from data_ingestion_toolbox.sql import catalog_queries, observation_queries

pytestmark = [pytest.mark.unit, pytest.mark.api]


@pytest.mark.parametrize(
    ("builder", "view"),
    [
        (catalog_queries.build_metrics_queries, "gold.dim_metric"),
        (catalog_queries.build_metrics_queries_legacy, "gold.dim_metric_catalog"),
        (catalog_queries.build_metrics_queries_glossary, "gold_glossary.dim_metric"),
        (
            catalog_queries.build_metrics_queries_glossary_legacy,
            "gold_glossary.dim_metric_catalog",
        ),
    ],
)
def test_metric_query_builders_bind_every_filter(builder, view: str) -> None:
    """Covers: API-010, API-017 — metric filters remain bound parameters."""
    list_query, count_query, params = builder("ACS", True, "population", 25, 50)
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


@pytest.mark.parametrize(
    ("builder", "view"),
    [
        (catalog_queries.build_geographies_queries, "gold.dim_geography"),
        (catalog_queries.build_geographies_queries_legacy, "gold.dim_geo_latest"),
        (
            catalog_queries.build_geographies_queries_glossary,
            "gold_glossary.dim_geography",
        ),
        (
            catalog_queries.build_geographies_queries_glossary_legacy,
            "gold_glossary.dim_geo_latest",
        ),
    ],
)
def test_geography_query_builders_bind_every_filter(builder, view: str) -> None:
    """Covers: API-010, API-017 — geography filters remain bound parameters."""
    list_query, count_query, params = builder("county", "06", "Alameda", 20, 40)
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


@pytest.mark.parametrize(
    ("builder", "expected_view"),
    [
        (observation_queries.build_latest_mv_queries, "gold.v_metric_latest_by_geo"),
        (
            observation_queries.build_latest_mv_queries_legacy,
            "gold.mv_latest_dashboard",
        ),
    ],
)
def test_latest_query_builders_bind_filters(builder, expected_view: str) -> None:
    """Covers: API-010, API-017 — latest filters are parameterized."""
    list_query, count_query, params = builder("POP_TOTAL", "state", "06", 5, 10)
    rendered_list = str(list_query)

    assert f"FROM {expected_view}" in rendered_list
    assert f"FROM {expected_view}" in str(count_query)
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


def test_source_latest_query_builder_uses_allowlisted_schema() -> None:
    """Covers: API-010 — source-aware latest queries target their schema."""
    list_query, count_query, params = (
        observation_queries.build_latest_mv_queries_for_schema(
            "gold_bls", "UNEMP", None, None, 10, 0
        )
    )

    assert "FROM gold_bls.v_metric_latest_by_geo" in str(list_query)
    assert "FROM gold_bls.v_metric_latest_by_geo" in str(count_query)
    assert params == {"limit": 10, "offset": 0, "metric_code": "UNEMP"}


@pytest.mark.parametrize(
    ("builder", "expected_view"),
    [
        (
            observation_queries.build_latest_rpt_fallback_queries,
            "gold.v_metric_timeseries_by_geo",
        ),
        (
            observation_queries.build_latest_rpt_fallback_queries_legacy,
            "gold.rpt_observation_dashboard",
        ),
    ],
)
def test_latest_fallback_query_builders_rank_each_geography(
    builder, expected_view: str
) -> None:
    """Covers: API-027 — durable fallback ranks the latest geography row."""
    list_query, count_query, params = builder("UNEMP", "county", "06", 5, 0)
    rendered_list = str(list_query)

    assert f"FROM {expected_view}" in rendered_list
    assert "ROW_NUMBER() OVER (PARTITION BY geo_id" in rendered_list
    assert "WHERE rn = 1" in rendered_list
    assert "SELECT COUNT(*) FROM ranked WHERE rn = 1" in str(count_query)
    assert params["metric_code"] == "UNEMP"


def test_source_latest_fallback_query_targets_source_schema() -> None:
    """Covers: API-027 — source fallback uses the durable source view."""
    list_query, count_query, params = (
        observation_queries.build_latest_rpt_fallback_queries_for_schema(
            "gold_fred", "GDP", None, None, 3, 1
        )
    )

    assert "FROM gold_fred.v_metric_timeseries_by_geo" in str(list_query)
    assert "FROM gold_fred.v_metric_timeseries_by_geo" in str(count_query)
    assert params == {"limit": 3, "offset": 1, "metric_code": "GDP"}


@pytest.mark.parametrize(
    ("builder", "expected_view"),
    [
        (
            observation_queries.build_timeseries_queries,
            "gold.v_metric_timeseries_by_geo",
        ),
        (
            observation_queries.build_timeseries_queries_legacy,
            "gold.rpt_observation_dashboard",
        ),
    ],
)
def test_timeseries_query_builders_bind_date_window(
    builder, expected_view: str
) -> None:
    """Covers: API-010, API-012 — timeseries binds and orders its window."""
    start = date(2024, 1, 1)
    end = date(2024, 12, 31)
    list_query, count_query, params = builder("UNEMP", "county:06001", start, end, 100)
    rendered_list = str(list_query)

    assert f"FROM {expected_view}" in rendered_list
    assert f"FROM {expected_view}" in str(count_query)
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


def test_source_timeseries_query_omits_absent_date_filters() -> None:
    """Covers: API-010, API-012 — optional dates stay absent when omitted."""
    list_query, count_query, params = (
        observation_queries.build_timeseries_queries_for_schema(
            "gold_census", "POP_TOTAL", "state:06", None, None, 50
        )
    )
    rendered_list = str(list_query)

    assert "FROM gold_census.v_metric_timeseries_by_geo" in rendered_list
    assert "FROM gold_census.v_metric_timeseries_by_geo" in str(count_query)
    assert ":start_date" not in rendered_list
    assert ":end_date" not in rendered_list
    assert params == {
        "limit": 50,
        "metric_code": "POP_TOTAL",
        "geo_id": "state:06",
    }
