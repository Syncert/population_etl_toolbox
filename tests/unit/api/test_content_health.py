"""API unit tests: the content report the readiness probe cannot give.

Covers: API-137 — ``/api/v1/health/content`` reports, per registered source,
        whether the warehouse publishes a measure a client could ask for; the
        grading rule counts only ``current`` measures, names a registered
        source the catalog holds no row for, and is metered and uncached.

``/health/ready`` answers ``SELECT 1``, so an API whose warehouse holds no
published measure reports itself healthy while every chart on every screen
draws nothing. These are the rules that make that state visible, tested
without a database because a grading rule is a statement about counts.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from apps.api.dependencies import SERVICE_UNAVAILABLE_DETAIL, get_db_session_dep
from apps.api.main import PUBLIC_CACHE_TARGETS, app
from apps.api.ratelimit import EXEMPT_PATHS
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.services.content_health import (
    DEGRADED,
    EMPTY,
    SERVING,
    grade_content,
    grade_source,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

CONTENT_PATH = "/api/v1/health/content"


def _row(source_code: str, **counts: object) -> dict[str, object]:
    """One counted catalog row, with every count defaulting to zero."""
    return {
        "source_code": source_code,
        "metrics_total": 0,
        "metrics_current": 0,
        "metrics_stale": 0,
        "metrics_retired": 0,
        "last_publication_time": None,
        **counts,
    }


def test_only_a_current_measure_makes_a_source_serving() -> None:
    """Covers: API-137 — a catalog of retired measures serves nothing."""
    serving = grade_source(_row("FRED", metrics_total=3, metrics_current=3))
    assert serving["status"] == SERVING

    # BLS carries 13,261 retired codes against 63 active ones. A rule that
    # read "the catalog has rows" would call a source whose every measure has
    # been retired healthy, which is the exact reading that makes an empty
    # screen look like a working deployment.
    retired_only = grade_source(
        _row("BLS", metrics_total=13_261, metrics_retired=13_261)
    )
    assert retired_only["status"] == EMPTY
    assert retired_only["metrics_retired"] == 13_261

    # Stale is not current either: the warehouse has not ended the series, but
    # it is not publishing one a client can ask for now.
    stale_only = grade_source(_row("CDC", metrics_total=4, metrics_stale=4))
    assert stale_only["status"] == EMPTY


def test_a_tally_that_does_not_account_for_the_catalog_says_so() -> None:
    """Covers: API-137 — three counted states, checked against the total."""
    complete = grade_source(
        _row(
            "FRED",
            metrics_total=5,
            metrics_current=2,
            metrics_stale=1,
            metrics_retired=2,
        )
    )
    assert complete["counts_are_complete"] is True

    # A harvest writing a fourth freshness word would leave measures in no
    # counted bucket. Reporting a smaller catalog than the warehouse holds,
    # silently, is the failure this flag exists to refuse.
    partial = grade_source(_row("FRED", metrics_total=9, metrics_current=2))
    assert partial["counts_are_complete"] is False
    assert partial["status"] == SERVING


def test_a_registered_source_the_catalog_never_published_is_named() -> None:
    """Covers: API-137 — the worst case is the one a GROUP BY cannot report.

    A source the API declares observation routes for and the warehouse has
    published no measure for contributes no row to group, so a report built
    only from the query's answer would omit it entirely -- and a reader
    scanning a list of healthy sources would never learn it was missing.
    """
    report = grade_content(
        ["CENSUS_ACS", "FBI_UCR"],
        [_row("CENSUS_ACS", metrics_total=2, metrics_current=2)],
    )

    codes = [source["source_code"] for source in report["sources"]]
    assert codes == ["CENSUS_ACS", "FBI_UCR"]

    absent = report["sources"][1]
    assert absent["status"] == EMPTY
    assert absent["registered"] is True
    assert absent["metrics_total"] == 0
    assert absent["last_publication_time"] is None
    assert report["silent_sources"] == ["FBI_UCR"]


def test_the_overall_status_separates_never_loaded_from_stopped_part_way() -> None:
    """Covers: API-137 — three states, because the two failures differ in kind."""
    registered = ["CENSUS_ACS", "FRED"]
    published = _row("CENSUS_ACS", metrics_total=1, metrics_current=1)

    everything = grade_content(
        registered, [published, _row("FRED", metrics_total=1, metrics_current=1)]
    )
    assert everything["status"] == SERVING
    assert everything["silent_sources"] == []

    partial = grade_content(registered, [published])
    assert partial["status"] == DEGRADED
    assert partial["silent_sources"] == ["FRED"]

    nothing = grade_content(registered, [])
    assert nothing["status"] == EMPTY
    assert nothing["silent_sources"] == registered


def test_a_published_source_no_route_serves_is_reported_but_not_counted() -> None:
    """Covers: API-137 — a warehouse fact to see, not an outage to declare.

    The catalog can publish a source the observation registry declares no
    route for. A reader should see it; grading the deployment down for it
    would report an outage in a promise this API never made.
    """
    report = grade_content(
        ["CENSUS_ACS"],
        [
            _row("CENSUS_ACS", metrics_total=1, metrics_current=1),
            _row("SOME_UNROUTED_SOURCE", metrics_total=7, metrics_current=7),
        ],
    )

    assert report["status"] == SERVING
    assert report["silent_sources"] == []
    unrouted = report["sources"][-1]
    assert unrouted["source_code"] == "SOME_UNROUTED_SOURCE"
    assert unrouted["registered"] is False


class _CountingSession:
    """A session that answers the content query with the given rows."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def execute(self, _query, _parameters=None):
        return self

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _DownSession:
    def execute(self, _query, _parameters=None):
        raise SQLAlchemyError("unreachable")


def _client(session: object) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def test_the_resource_reports_every_registered_source() -> None:
    """Covers: API-137 — the served body grades the application's own registry."""
    rows = [
        _row(code, metrics_total=1, metrics_current=1) for code in OBSERVATION_DISPATCH
    ]
    try:
        response = _client(_CountingSession(rows)).get(CONTENT_PATH)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == SERVING
    assert payload["silent_sources"] == []
    assert {source["source_code"] for source in payload["sources"]} == set(
        OBSERVATION_DISPATCH
    )


def test_an_empty_warehouse_answers_two_hundred_and_says_it_is_empty() -> None:
    """Covers: API-137 — the state is the report, not a refusal.

    A resource that answered 503 on empty content would be unreadable in
    exactly the state a reader needs to read it, and content is not a reason
    to take the process out of the load balancer.
    """
    try:
        response = _client(_CountingSession([])).get(CONTENT_PATH)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == EMPTY
    assert payload["silent_sources"] == sorted(OBSERVATION_DISPATCH)


def test_an_unreachable_warehouse_answers_the_sanitized_refusal() -> None:
    """Covers: API-137 — the one thing this resource cannot report on."""
    try:
        response = _client(_DownSession()).get(CONTENT_PATH)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"] == SERVICE_UNAVAILABLE_DETAIL


def test_the_content_resource_is_metered_and_never_cached() -> None:
    """Covers: API-137 — the two properties its separate router exists for.

    ``apps/api/ratelimit.py`` derives its exempt paths from every route the
    health routers serve, and the response cache from ``CACHEABLE_ROUTERS``.
    A future edit moving this route onto ``health.router`` would publish an
    unauthenticated unmetered grouped scan, and one adding it to the
    cacheable set would answer a monitor with a five-minute-old picture of
    now. Both would pass every other test in this file.
    """
    assert CONTENT_PATH not in EXEMPT_PATHS
    assert not PUBLIC_CACHE_TARGETS.covers(CONTENT_PATH)
    # The versioned liveness resource keeps its exemption: the web
    # application calls it on every page load (API-101).
    assert "/api/v1/health" in EXEMPT_PATHS
