"""API unit tests: catalog discovery and capability resources.

Covers: API-037 (catalog reads only the documented glossary contracts and an
        absent contract fails explicitly), API-038 (metric capability detail
        with stable unknown-identifier behavior), API-039 (source capability
        metadata for every completed source), API-040 (per-source publication
        freshness rollup), API-041 (deterministic catalog ordering and stable
        empty-result behavior).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import SERVICE_UNAVAILABLE_DETAIL, get_db_session_dep
from apps.api.main import app
from apps.api.registry import SOURCE_DISCOVERY
from data_ingestion_toolbox.sql import catalog_queries

pytestmark = [pytest.mark.unit, pytest.mark.api]

_GLOSSARY_CONTRACTS = frozenset(
    {
        "gold_glossary.dim_source_system",
        "gold_glossary.dim_metric",
        "gold_glossary.dim_geography",
    }
)

_METRIC_ROW = {
    "metric_code": "CDC:cdi:ALC1_1:crude",
    "metric_display_name": "Alcohol use among youth",
    "source_code": "CDC",
    "source_object_type": "measure",
    "source_object_key": "cdi:ALC1_1:crude",
    "units": "percent",
    "measure_kind": "rate",
    "valid_geo_grains": ["STATE"],
    "valid_time_grains": ["year"],
    "aggregation_characteristic": "not_additive",
    "physical_lineage": {"relation": "gold_cdc.health_observation"},
    "publisher_contract_version": "1.0",
    "source_watermark": "20260101",
    "source_run_id": None,
    "publication_time": datetime(2026, 1, 2, tzinfo=timezone.utc),
    "harvested_at": datetime(2026, 1, 3, tzinfo=timezone.utc),
    "freshness_state": "current",
}


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _RowSession:
    """Answers every non-count query with the configured rows."""

    def __init__(self, rows=None, total=0):
        self._rows = rows or []
        self._total = total

    def execute(self, query, params=None):
        if "COUNT(*)" in str(query) and "FILTER" not in str(query):
            return _FakeResult(scalar_value=self._total)
        return _FakeResult(rows=self._rows)


class _MissingRelationSession:
    """A bound session whose warehouse positively lacks the probed relation."""

    bind = object()

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        if "to_regclass" in rendered:
            return _FakeResult(scalar_value=False)
        raise AssertionError("no query may run once the relation is known absent")


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def _clear_overrides() -> None:
    app.dependency_overrides.clear()


def _relations_in(sql: str) -> set[str]:
    return set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))


# ---------------------------------------------------------------------------
# API-037 — glossary-only catalog reads, explicit failure on absence
# ---------------------------------------------------------------------------


def test_catalog_queries_name_only_the_documented_glossary_contracts() -> None:
    """Covers: API-037 — the reviewed allowlist is exactly the glossary trio."""
    assert catalog_queries.CATALOG_RELATIONS == _GLOSSARY_CONTRACTS

    metrics_list, metrics_count, _ = catalog_queries.build_metrics_queries(
        "CDC", True, "alcohol", 10, 0
    )
    geo_list, geo_count, _ = catalog_queries.build_geographies_queries(
        "county", "06", "Alameda", 10, 0
    )
    detail, _ = catalog_queries.build_metric_detail_query("CDC:cdi:ALC1_1:crude")
    rendered = [
        str(query)
        for query in (
            catalog_queries.SOURCES_QUERY,
            catalog_queries.SOURCE_FRESHNESS_QUERY,
            metrics_list,
            metrics_count,
            geo_list,
            geo_count,
            detail,
        )
    ]
    for sql in rendered:
        assert _relations_in(sql) <= catalog_queries.CATALOG_RELATIONS, sql
        assert "SELECT *" not in sql, "catalog projections must be explicit"


def test_legacy_catalog_builders_and_probing_are_retired() -> None:
    """Covers: API-037 — no path remains that selects a relation by probing."""
    retired = (
        "build_metrics_queries_legacy",
        "build_metrics_queries_glossary",
        "build_metrics_queries_glossary_legacy",
        "build_geographies_queries_legacy",
        "build_geographies_queries_glossary",
        "build_geographies_queries_glossary_legacy",
        "SOURCES_QUERY_GLOSSARY",
    )
    for name in retired:
        assert not hasattr(catalog_queries, name), name

    from apps.api.services import catalog_service

    assert not hasattr(catalog_service, "_relation_exists")


def test_absent_catalog_contract_answers_the_sanitized_503() -> None:
    """Covers: API-037 — a missing glossary contract is a fault, not a guess."""
    session = _MissingRelationSession()
    client = _client_with(session)
    try:
        response = client.get("/api/v1/catalog/metrics")
    finally:
        _clear_overrides()

    assert response.status_code == 503
    assert response.json() == {"detail": SERVICE_UNAVAILABLE_DETAIL}
    assert "gold_glossary" not in response.text
    assert all("to_regclass" in statement for statement in session.statements)


# ---------------------------------------------------------------------------
# API-038 — metric capability detail
# ---------------------------------------------------------------------------


def test_metric_detail_returns_published_semantics_and_routes() -> None:
    """Covers: API-038 — a discovered metric names the routes that serve it."""
    client = _client_with(_RowSession(rows=[dict(_METRIC_ROW)]))
    try:
        response = client.get("/api/v1/catalog/metrics/CDC:cdi:ALC1_1:crude")
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["metric_code"] == _METRIC_ROW["metric_code"]
    assert payload["units"] == "percent"
    assert payload["measure_kind"] == "rate"
    assert payload["valid_geo_grains"] == ["STATE"]
    assert payload["aggregation_characteristic"] == "not_additive"
    assert payload["freshness_state"] == "current"
    assert payload["served_by_neutral_routes"] is True
    assert [route["path"] for route in payload["observation_routes"]] == [
        "/api/v1/cdc/observations",
        "/api/v1/observations",
        "/api/v1/observations/releases",
    ]
    cdc_route = payload["observation_routes"][0]
    assert "dataset" in cdc_route["parameters"]
    assert "release" in cdc_route["parameters"]
    assert "stratum_id" in payload["observation_filters"]
    assert "domain_desc" not in payload["observation_filters"]


def test_metric_detail_for_a_neutral_source_reports_the_neutral_routes() -> None:
    """Covers: API-038 — union-served sources advertise the neutral routes."""
    row = dict(_METRIC_ROW)
    row.update({"metric_code": "BLS:LAUCN06001", "source_code": "BLS", "units": "rate"})
    client = _client_with(_RowSession(rows=[row]))
    try:
        response = client.get("/api/v1/catalog/metrics/BLS:LAUCN06001")
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["served_by_neutral_routes"] is True
    paths = {route["path"] for route in payload["observation_routes"]}
    assert {
        "/api/v1/observations",
        "/api/v1/observations/releases",
        "/api/v1/observations/latest",
        "/api/v1/observations/timeseries",
        "/api/v1/comparison",
        "/api/v1/distribution/bins",
        "/api/v1/bls/observations/latest",
        "/api/v1/bls/observations/timeseries",
    } <= paths


def test_unknown_metric_code_returns_a_stable_404() -> None:
    """Covers: API-038 — an unknown identifier is explained, not an empty page."""
    client = _client_with(_RowSession(rows=[]))
    try:
        response = client.get("/api/v1/catalog/metrics/NO:SUCH:METRIC")
    finally:
        _clear_overrides()

    assert response.status_code == 404
    assert response.json() == {"detail": "metric_code not found"}


# ---------------------------------------------------------------------------
# API-039 — source capability metadata
# ---------------------------------------------------------------------------


def test_capabilities_cover_every_completed_source_in_stable_order() -> None:
    """Covers: API-039 — all seven sources, ordered, none needing a database."""
    client = TestClient(app)
    response = client.get("/api/v1/catalog/capabilities")

    assert response.status_code == 200
    payload = response.json()
    codes = [item["source_code"] for item in payload["items"]]
    assert codes == sorted(SOURCE_DISCOVERY)
    assert payload["total"] == len(codes) == 7

    by_code = {item["source_code"]: item for item in payload["items"]}
    neutral = {
        code for code, item in by_code.items() if item["served_by_neutral_routes"]
    }
    assert neutral == set(SOURCE_DISCOVERY), (
        "since API-004's registry dispatch every completed source is servable"
    )

    fbi = by_code["FBI_UCR"]
    assert fbi["route_segment"] is None
    assert [route["path"] for route in fbi["observation_routes"]] == [
        "/api/v1/observations",
        "/api/v1/observations/releases",
    ]
    assert fbi["datasets"] == ["summarized_violent_crime"]
    assert "subject_type" in fbi["observation_filters"]

    # The analysis routes still read the three-source union views (API-005
    # rebuilds them); advertising them for a dispatch-only source would be the
    # silent empty page the capability resource exists to prevent.
    cdc_paths = {route["path"] for route in by_code["CDC"]["observation_routes"]}
    assert "/api/v1/comparison" not in cdc_paths
    assert "/api/v1/observations/latest" not in cdc_paths


def test_every_advertised_capability_route_is_actually_served() -> None:
    """Covers: API-039 — capability metadata cannot drift from the contract."""
    client = TestClient(app)
    served = app.openapi()["paths"]
    payload = client.get("/api/v1/catalog/capabilities").json()

    for item in payload["items"]:
        for route in item["observation_routes"]:
            operation = served.get(route["path"], {}).get("get")
            assert operation is not None, route["path"]
            served_parameters = sorted(
                parameter["name"]
                for parameter in operation.get("parameters") or []
                if parameter["in"] == "query"
            )
            assert route["parameters"] == served_parameters, route["path"]


def test_capability_datasets_come_from_the_source_registries() -> None:
    """Covers: API-039 — dataset identities are read, not copied."""
    from data_ingestion_toolbox.cdc.registry import enabled_assets
    from data_ingestion_toolbox.usda_nass.registry import enabled_products

    payload = TestClient(app).get("/api/v1/catalog/capabilities").json()
    by_code = {item["source_code"]: item for item in payload["items"]}

    assert by_code["CDC"]["datasets"] == [asset.asset_id for asset in enabled_assets()]
    assert by_code["USDA_NASS"]["datasets"] == [
        product.product_id for product in enabled_products()
    ]


# ---------------------------------------------------------------------------
# API-040 — publication freshness rollup
# ---------------------------------------------------------------------------


def test_freshness_reports_the_published_state_per_source() -> None:
    """Covers: API-040 — the rollup serves the glossary's freshness signal."""
    rows = [
        {
            "source_code": "BLS",
            "metric_count": 12,
            "current_count": 10,
            "stale_count": 2,
            "retired_count": 0,
            "latest_publication_time": datetime(2026, 8, 1, tzinfo=timezone.utc),
            "latest_harvested_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
        },
        {
            "source_code": "CDC",
            "metric_count": 4,
            "current_count": 4,
            "stale_count": 0,
            "retired_count": 0,
            "latest_publication_time": None,
            "latest_harvested_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        },
    ]
    client = _client_with(_RowSession(rows=rows))
    try:
        response = client.get("/api/v1/catalog/freshness")
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["items"][0]["source_code"] == "BLS"
    assert payload["items"][0]["stale_count"] == 2
    assert payload["items"][1]["latest_publication_time"] is None

    rendered = str(catalog_queries.SOURCE_FRESHNESS_QUERY)
    assert "GROUP BY source_code" in rendered
    assert "ORDER BY source_code" in rendered
    for state in ("current", "stale", "retired"):
        assert f"freshness_state = '{state}'" in rendered


def test_freshness_of_an_empty_glossary_is_an_empty_list() -> None:
    """Covers: API-040 — an unharvested warehouse yields an empty rollup."""
    client = _client_with(_RowSession(rows=[]))
    try:
        response = client.get("/api/v1/catalog/freshness")
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert response.json() == {"total": 0, "items": []}


# ---------------------------------------------------------------------------
# API-041 — deterministic ordering and empty results
# ---------------------------------------------------------------------------


def test_catalog_lists_declare_deterministic_ordering() -> None:
    """Covers: API-041 — paging is stable because ordering is stable."""
    metrics_list, _, _ = catalog_queries.build_metrics_queries(None, None, None, 10, 0)
    geo_list, _, _ = catalog_queries.build_geographies_queries(None, None, None, 10, 0)

    assert "ORDER BY metric_code" in str(metrics_list)
    assert "ORDER BY geo_id" in str(geo_list)
    assert "ORDER BY source_code" in str(catalog_queries.SOURCES_QUERY)


def test_empty_catalog_results_are_stable_empty_pages() -> None:
    """Covers: API-041 — no rows is a page shape, not an error."""
    client = _client_with(_RowSession(rows=[], total=0))
    try:
        metrics = client.get("/api/v1/catalog/metrics?q=nothing-matches")
        geographies = client.get("/api/v1/catalog/geographies?q=nothing-matches")
    finally:
        _clear_overrides()

    assert metrics.status_code == 200
    assert metrics.json() == {"total": 0, "limit": 100, "offset": 0, "items": []}
    assert geographies.status_code == 200
    assert geographies.json() == {"total": 0, "limit": 100, "offset": 0, "items": []}
