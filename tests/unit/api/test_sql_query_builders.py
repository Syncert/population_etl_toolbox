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
        "county", "06", None, "Alameda", 20, 40
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
        None, None, None, None, 10, 0
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
        "UNEMP", "county:06001", start, end, 100, 25
    )
    rendered_list = str(list_query)

    assert "FROM gold.v_metric_timeseries_by_geo" in rendered_list
    assert "FROM gold.v_metric_timeseries_by_geo" in str(count_query)
    assert "observation_date >= :start_date" in rendered_list
    assert "observation_date <= :end_date" in rendered_list
    # API-074: the order is total, and the page can move past the first one.
    assert (
        "ORDER BY observation_date ASC, as_of_date ASC, dataset_code ASC, "
        "vintage_year ASC" in rendered_list
    )
    assert "LIMIT :limit OFFSET :offset" in rendered_list
    assert params == {
        "limit": 100,
        "offset": 25,
        "metric_code": "UNEMP",
        "geo_id": "county:06001",
        "start_date": start,
        "end_date": end,
    }


def test_timeseries_query_omits_absent_date_filters() -> None:
    """Covers: API-010, API-012 — optional dates stay absent when omitted."""
    list_query, _count_query, params = observation_queries.build_timeseries_queries(
        "POP_TOTAL", "state:06", None, None, 50, 0
    )
    rendered_list = str(list_query)

    assert ":start_date" not in rendered_list
    assert ":end_date" not in rendered_list
    assert params == {
        "limit": 50,
        "offset": 0,
        "metric_code": "POP_TOTAL",
        "geo_id": "state:06",
    }


@pytest.mark.parametrize(
    ("typed", "bound"),
    [
        ("CENSUS_ACS", r"%CENSUS\_ACS%"),
        ("B01003_001", r"%B01003\_001%"),
        ("50%", r"%50\%%"),
        ("_", r"%\_%"),
        (r"a\b", r"%a\\b%"),
        ("population", "%population%"),
    ],
    ids=(
        "underscore-in-source",
        "underscore-in-code",
        "percent",
        "bare-underscore",
        "backslash",
        "unchanged",
    ),
)
def test_catalog_search_matches_literal_text(typed: str, bound: str) -> None:
    """Covers: API-077 — `q` is text to find, not a pattern to run.

    Every metric code in this warehouse carries an underscore, so the most
    ordinary search there is ran as a wildcard pattern; `q=%` returned the
    whole catalog under a filter the caller believed narrowed it.
    """
    for builder, kwargs in (
        (
            catalog_queries.build_metrics_queries,
            {"source_code": None, "active_only": None},
        ),
        (
            catalog_queries.build_geographies_queries,
            {"geo_level": None, "state_fips": None, "active_only": None},
        ),
    ):
        list_query, count_query, params = builder(q=typed, limit=10, offset=0, **kwargs)
        assert params["q"] == bound, builder.__name__
        for rendered in (str(list_query), str(count_query)):
            assert r"ESCAPE '\'" in rendered, builder.__name__
            # Still bound, never interpolated.
            assert bound not in rendered, builder.__name__


def test_latest_fallback_ranks_a_total_order_over_the_union() -> None:
    """Covers: API-086 — a tie in the fallback is decided, not left to the plan.

    The fallback ranked on `observation_date DESC` alone, and this module says
    twelve lines below it why that is not an order over this view: the
    as-published relations behind it hold one row per release of a period, so
    an ACS metric published under two vintages ties on its observation date.
    The group `ROW_NUMBER` picks 1 from held several rows, and which value a
    geography got was whatever the plan produced.
    """
    list_query, _count_query, _params = (
        observation_queries.build_latest_rpt_fallback_queries("UNEMP", None, None, 5, 0)
    )
    rendered = " ".join(str(list_query).split())

    ranking = rendered.split("ROW_NUMBER() OVER", 1)[1].split(")", 1)[0]
    assert "ORDER BY" in ranking, rendered
    ordering = ranking.split("ORDER BY", 1)[1].strip()
    assert ordering == observation_queries._LATEST_SELECTION_ORDER, ordering

    # Derived from the order this module already declares for the same view,
    # read for recency rather than for paging -- not a fourth copy of the
    # three refresh procedures' rules.
    paging_columns = [
        entry.strip().split(" ", 1)[0]
        for entry in observation_queries._TIMESERIES_ORDER.split(",")
    ]
    selection_columns = [
        entry.strip().split(" ", 1)[0]
        for entry in observation_queries._LATEST_SELECTION_ORDER.split(",")
    ]
    assert selection_columns == paging_columns, (
        "the fallback must rank the columns this view is already declared to "
        "be keyed by"
    )


def test_latest_fallback_prefers_a_recorded_release_to_a_missing_one() -> None:
    """Covers: API-086 — an unrecorded identity does not outrank a recorded one.

    `DESC` sorts nulls first in PostgreSQL, so a row carrying no release date
    or no vintage would have won the tie over every row that records one.
    """
    order = observation_queries._LATEST_SELECTION_ORDER
    for column in ("as_of_date", "vintage_year"):
        entry = next(
            part.strip() for part in order.split(",") if part.strip().startswith(column)
        )
        assert entry.endswith("NULLS LAST"), entry

    # The period itself is never absent -- it is the view's own key -- and
    # `acs1` before `acs5` is what ascending `dataset_code` spells, which is
    # the preference the ACS refresh declares.
    assert "observation_date DESC" in order
    assert "dataset_code ASC" in order
