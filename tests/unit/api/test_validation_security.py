"""P0 API bounds, empty/unknown, injection, and query-size contracts."""

from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import DisconnectionError, OperationalError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app

pytestmark = [pytest.mark.unit, pytest.mark.api]


class _Result:
    def __init__(self, rows=None, scalar_value=None) -> None:
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar_value


class _RecordingEmptySession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):
        self.calls.append((str(query), params or {}))
        if "count(*)" in str(query).lower():
            return _Result(scalar_value=0)
        return _Result(rows=[])


class _NoExecuteSession:
    def execute(self, query, params=None):
        raise AssertionError(f"database was called: {query!s}, {params!r}")


class _FailingSession:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def execute(self, _query, _params=None):
        raise self.error


@contextmanager
def _client_for(session) -> Iterator[TestClient]:
    def _override_db():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/v1/catalog/metrics", {"limit": 0}),
        ("/api/v1/catalog/metrics", {"limit": 1001}),
        ("/api/v1/catalog/metrics", {"offset": -1}),
        ("/api/v1/observations/latest", {"metric_code": "POP", "limit": 5001}),
        (
            "/api/v1/comparison",
            {"metric_code_a": "A", "metric_code_b": "B", "limit": 1001},
        ),
    ],
)
def test_pagination_out_of_bounds_is_rejected_before_database(
    path: str, params: dict
) -> None:
    """Covers: API-005 — invalid pagination cannot reach database work."""
    with _client_for(_NoExecuteSession()) as client:
        assert client.get(path, params=params).status_code == 422


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/v1/catalog/metrics", {"limit": 1, "offset": 0}),
        ("/api/v1/catalog/metrics", {"limit": 1000, "offset": 0}),
        ("/api/v1/observations/latest", {"metric_code": "UNKNOWN", "limit": 1}),
        ("/api/v1/observations/latest", {"metric_code": "UNKNOWN", "limit": 5000}),
    ],
)
def test_pagination_boundaries_succeed(path: str, params: dict) -> None:
    """Covers: API-005 — declared pagination boundaries are accepted."""
    with _client_for(_RecordingEmptySession()) as client:
        assert client.get(path, params=params).status_code == 200


def test_empty_latest_results_have_stable_contract() -> None:
    """Covers: API-008 — empty latest results return the stable contract."""
    with _client_for(_RecordingEmptySession()) as client:
        response = client.get(
            "/api/v1/observations/latest", params={"metric_code": "UNKNOWN"}
        )
    assert response.status_code == 200
    assert response.json()["items"] == []
    assert response.json()["total"] == 0


def test_unknown_metric_is_consistently_empty_for_latest_and_history() -> None:
    """Covers: API-009 — unknown metrics return consistent empty responses."""
    session = _RecordingEmptySession()
    with _client_for(session) as client:
        latest = client.get(
            "/api/v1/observations/latest", params={"metric_code": "UNKNOWN"}
        )
        history = client.get(
            "/api/v1/observations/timeseries",
            params={"metric_code": "UNKNOWN", "geo_id": "state:00"},
        )
    assert latest.status_code == history.status_code == 200
    assert latest.json()["items"] == history.json()["items"] == []
    assert latest.json()["total"] == history.json()["total"] == 0


def test_unknown_geography_is_consistently_empty_for_history() -> None:
    """Covers: API-009 — an unknown geography returns the empty contract."""
    with _client_for(_RecordingEmptySession()) as client:
        common = client.get(
            "/api/v1/observations/timeseries",
            params={"metric_code": "POP_TOTAL", "geo_id": "state:00"},
        )
        census = client.get(
            "/api/v1/census/observations/timeseries",
            params={"metric_code": "POP_TOTAL", "geo_id": "state:00"},
        )
    assert common.status_code == census.status_code == 200
    assert common.json()["items"] == census.json()["items"] == []
    assert common.json()["total"] == census.json()["total"] == 0


def test_sql_metacharacters_remain_bound_parameters() -> None:
    """Covers: API-017 — injection text remains bound parameter data."""
    attack = "POP'; DROP TABLE gold.metric_catalog; --"
    session = _RecordingEmptySession()
    with _client_for(session) as client:
        response = client.get(
            "/api/v1/observations/latest", params={"metric_code": attack}
        )
    assert response.status_code == 200
    assert session.calls
    for sql, params in session.calls:
        assert attack not in sql
        assert params["metric_code"] == attack


def test_oversized_metric_is_rejected_before_database() -> None:
    """Covers: API-018 — oversized metrics fail before database work."""
    with _client_for(_NoExecuteSession()) as client:
        response = client.get(
            "/api/v1/observations/latest", params={"metric_code": "X" * 201}
        )
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/v1/observations/latest", {"metric_id": "X" * 201}),
        (
            "/api/v1/observations/timeseries",
            {"metric_code": "POP", "geo_id": "X" * 201},
        ),
        (
            "/api/v1/census/observations/latest",
            {"metric_code": "POP", "geo_level": "X" * 51},
        ),
        (
            "/api/v1/bls/observations/latest",
            {"metric_code": "UNEMP", "state_fips": "123"},
        ),
        ("/api/v1/catalog/metrics", {"source_code": "X" * 51}),
        ("/api/v1/catalog/metrics", {"q": "X" * 201}),
        (
            "/api/v1/comparison",
            {
                "metric_code_a": "A",
                "metric_code_b": "B",
                "geo_level": "X" * 51,
            },
        ),
        (
            "/api/v1/distribution/bins",
            {"metric_code": "POP", "state_fips": "123"},
        ),
    ],
)
def test_endpoint_specific_query_size_limits_precede_database(
    path: str, params: dict
) -> None:
    """Covers: API-018 — endpoint query-size limits precede database work."""
    with _client_for(_NoExecuteSession()) as client:
        assert client.get(path, params=params).status_code == 422


@pytest.mark.parametrize(
    "error",
    [
        OperationalError(
            "SELECT secret FROM host",
            {"password": "do-not-leak"},
            TimeoutError("database.example.test timed out"),
        ),
        DisconnectionError("database.example.test disconnected"),
    ],
    ids=("timeout", "disconnect"),
)
def test_database_timeout_and_disconnect_are_sanitized(error: Exception) -> None:
    """Covers: API-016 — timeout and disconnect return a sanitized 503."""
    with _client_for(_FailingSession(error)) as client:
        response = client.get("/api/v1/catalog/sources")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database service is temporarily unavailable."}
    assert "do-not-leak" not in response.text
    assert "database.example.test" not in response.text


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/health", {}),
        ("/api/v1/observations/latest", {}),
        ("/api/v1/observations/latest", {"metric_code": "X" * 201}),
    ],
)
def test_security_headers_are_present_on_success_and_error_responses(
    path: str, params: dict
) -> None:
    """Covers: API-002 — security headers cover success and error responses."""
    with _client_for(_NoExecuteSession()) as client:
        response = client.get(path, params=params)
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    assert response.headers["permissions-policy"] == (
        "camera=(), microphone=(), geolocation=()"
    )
    assert response.headers["cross-origin-resource-policy"] == "same-site"


#: Real spellings from this API's own surface. Each is a parameter some other
#: route declares, sent to a route that declares a different name for the same
#: idea -- the mistake a client makes by reading one page of the reference and
#: acting on another.
FOREIGN_SPELLINGS = (
    # `/observations` declares `geo_level`; the plural is the reflex.
    (
        "/api/v1/observations",
        {"metric_code": "X", "geo_levels": "COUNTY"},
        "geo_levels",
    ),
    # `/observations` declares `adjustment_status`; `/cdc/observations` declares
    # `adjustment`.
    ("/api/v1/cdc/observations", {"adjustment_status": "crude"}, "adjustment_status"),
    # `/usda-nass/observations` declares `year_start`/`year_end`; CDC and the
    # neutral resource declare `year_from`/`year_to`.
    ("/api/v1/cdc/observations", {"year_start": 2000}, "year_start"),
    ("/api/v1/usda-nass/observations", {"year_from": 2000}, "year_from"),
    # `/catalog/metrics` declares `source_code`, singular.
    ("/api/v1/catalog/metrics", {"source_codes": "BLS"}, "source_codes"),
)


@pytest.mark.parametrize(("path", "params", "unknown"), FOREIGN_SPELLINGS)
def test_a_query_parameter_the_route_does_not_declare_is_refused(
    path: str, params: dict, unknown: str
) -> None:
    """Covers: API-093 — an unknown query parameter is refused, not ignored.

    The serving registry already promises this of a filter a source does not
    declare: "A request using a parameter absent here is rejected with an
    explanation, never silently ignored." It was true only for names the route
    had heard of. FastAPI binds what a signature names and discards the rest,
    so `geo_levels=COUNTY` was answered with every grain, at 200, with a total
    that reads as a complete answer to the question the caller thought they
    asked.

    No database work either: a request this malformed is refused before a
    session is opened.
    """
    with _client_for(_NoExecuteSession()) as client:
        response = client.get(path, params=params)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert unknown in detail, detail


def test_the_refusal_names_the_parameters_the_route_accepts() -> None:
    """Covers: API-093 — the accepted set is the route's own, not a copy.

    Read from the served OpenAPI document rather than written here: a
    parameter added to a signature must be accepted the moment it exists, and
    a message listing a hand-maintained set is a message that goes stale.
    """
    document = app.openapi()
    declared = {
        parameter["name"]
        for parameter in document["paths"]["/api/v1/observations"]["get"]["parameters"]
        if parameter["in"] == "query"
    }
    assert declared, document["paths"]["/api/v1/observations"]["get"]
    with _client_for(_NoExecuteSession()) as client:
        response = client.get("/api/v1/observations", params={"nope": "1"})
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    for name in declared:
        assert name in detail, f"{name} missing from: {detail}"


def _example_path(template: str) -> str:
    """A concrete path for a template, with each parameter given a value."""
    return re.sub(r"\{[^}]+\}", "1", template)


def test_every_served_route_refuses_an_undeclared_query_parameter() -> None:
    """Covers: API-093 — the rule is the application's, not one router's.

    Source-agnostic by construction: the routes come from the served OpenAPI
    document, so a router added later is covered without an edit here. The
    private routes are included deliberately -- the refusal precedes
    authentication, and the parameter list it names is already published in
    that same document, so nothing unpublished is disclosed by it.
    """
    document = app.openapi()
    unanswered: list[str] = []
    with _client_for(_NoExecuteSession()) as client:
        for template, operations in sorted(document["paths"].items()):
            for method in sorted(operations):
                if method.upper() not in {"GET", "POST", "PUT", "DELETE"}:
                    continue
                response = client.request(
                    method.upper(),
                    _example_path(template),
                    params={"not_a_parameter_of_any_route": "1"},
                    json={} if method.upper() in {"POST", "PUT"} else None,
                )
                if response.status_code != 422 or (
                    "not_a_parameter_of_any_route" not in response.text
                ):
                    unanswered.append(
                        f"{method.upper()} {template} answered "
                        f"{response.status_code}, not a refusal naming the "
                        "parameter"
                    )
    assert not unanswered, "\n".join(unanswered)
