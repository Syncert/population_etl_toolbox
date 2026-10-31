"""API unit tests: the admin warehouse pages (``/admin/warehouse/*``).

Covers: API-183 (the admin resource is off -- 404 before any credential is
        read -- until a deployment names an admin; a missing token is 401, an
        account not on the list is 403, an operator label or a signed-in email
        on the list is let in; every answer is ``private, no-store``; the
        declared inventory describes every source the API serves; coverage is
        projected through the serving registry's own expressions and a timeout
        is reported, not raised; the schema atlas groups every relation into a
        diagram and draws only edges between boxes it placed).
"""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.api.auth import Account, get_app_session_dep, require_account
from apps.api.dependencies import get_db_session_dep
from apps.api.main import PRIVATE_ROUTERS, PUBLIC_CACHE_TARGETS, app
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.routers import admin as admin_router
from apps.api.services import warehouse_coverage as coverage
from apps.api.services.warehouse_atlas import Relation, build_atlas
from apps.api.services.warehouse_inventory import (
    GRAINS,
    SOURCE_BY_CODE,
    TOPICS,
    WAREHOUSE_SOURCES,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

_ADMIN = Account(user_account_id=1, display_label="nick-laptop", credential_id=10)
_SIGNED_IN = Account(
    user_account_id=2, display_label="self-service:abc", credential_id=11
)


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> "_Rows":
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)

    def scalar(self) -> Any:
        return None


class _Session:
    """Answers every statement with the rows queued for it, in order."""

    def __init__(self, *answers: list[dict[str, Any]] | Exception) -> None:
        self.answers = list(answers)
        self.statements: list[str] = []
        self.rolled_back = 0

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        answer = self.answers.pop(0) if self.answers else []
        if isinstance(answer, Exception):
            raise answer
        return _Rows(answer)

    def rollback(self) -> None:
        self.rolled_back += 1


def _settings(*admins: str) -> SimpleNamespace:
    return SimpleNamespace(api_admin_accounts=frozenset(a.lower() for a in admins))


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    coverage.clear_coverage_cache()
    yield
    app.dependency_overrides.clear()
    coverage.clear_coverage_cache()


def _client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    admins: tuple[str, ...] = ("Nick-Laptop", "nick@example.org"),
    account: Account | None = _ADMIN,
    email: str | None = None,
    warehouse: _Session | None = None,
) -> TestClient:
    monkeypatch.setattr(admin_router, "get_settings", lambda: _settings(*admins))
    monkeypatch.setattr(
        admin_router,
        "load_account",
        lambda _db, user_account_id: SimpleNamespace(email=email),
    )
    if account is not None:
        app.dependency_overrides[require_account] = lambda: account

    def _storage():
        yield _Session()

    def _warehouse():
        yield warehouse or _Session()

    app.dependency_overrides[get_app_session_dep] = _storage
    app.dependency_overrides[get_db_session_dep] = _warehouse
    return TestClient(app)


_PATHS = (
    "/api/v1/admin/warehouse/sources",
    "/api/v1/admin/warehouse/coverage/CENSUS_GEO",
    "/api/v1/admin/warehouse/schema",
)


# ----------------------------------------------------------------- who may read


@pytest.mark.parametrize("path", _PATHS)
def test_no_admin_named_answers_404_before_any_credential(
    monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    """Covers: API-183 — an unconfigured deployment does not advertise the resource."""
    client = _client(monkeypatch, admins=(), account=None)
    response = client.get(path)
    assert response.status_code == 404


def test_a_missing_token_is_401_once_an_admin_is_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-183 — the usual credential refusal, unchanged."""
    monkeypatch.setenv("APP_API_DATABASE_URL", "postgresql://app.invalid/app")
    client = _client(monkeypatch, account=None)
    response = client.get("/api/v1/admin/warehouse/schema")
    assert response.status_code == 401


def test_an_account_not_on_the_list_is_403(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-183 — authenticated is not authorized."""
    client = _client(monkeypatch, account=_SIGNED_IN, email="someone@example.org")
    response = client.get("/api/v1/admin/warehouse/coverage/CENSUS_GEO")
    assert response.status_code == 403
    assert response.json() == {"detail": admin_router.ADMIN_FORBIDDEN_DETAIL}


def test_an_operator_label_or_a_signed_in_email_is_let_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-183 — matched without regard to case, label first."""
    by_label = _client(monkeypatch).get("/api/v1/admin/warehouse/coverage/CENSUS_GEO")
    assert by_label.status_code == 200
    app.dependency_overrides.clear()
    by_email = _client(monkeypatch, account=_SIGNED_IN, email="Nick@Example.org").get(
        "/api/v1/admin/warehouse/coverage/CENSUS_GEO"
    )
    assert by_email.status_code == 200
    for response in (by_label, by_email):
        assert response.headers["cache-control"] == "private, no-store"
        assert response.headers["x-robots-tag"] == "noindex, nofollow"


def test_the_admin_paths_are_outside_every_cacheable_prefix() -> None:
    """Covers: API-183 — an operator description is never publicly cached."""
    assert admin_router.router in PRIVATE_ROUTERS
    for path in _PATHS:
        assert not PUBLIC_CACHE_TARGETS.covers(path), path


def test_an_unknown_source_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-183 — an unknown source is not measured."""
    client = _client(monkeypatch)
    assert client.get("/api/v1/admin/warehouse/coverage/NOPE").status_code == 404


# -------------------------------------------------------------------- inventory


def test_every_served_source_is_described() -> None:
    """Covers: API-183 — a source cannot reach the API without an inventory entry."""
    described = {s.catalog_source_code for s in WAREHOUSE_SOURCES}
    missing = sorted(set(OBSERVATION_DISPATCH) - described)
    assert not missing, f"add these to warehouse_inventory.WAREHOUSE_SOURCES: {missing}"
    grain_codes = {code for code, _ in GRAINS}
    for source in WAREHOUSE_SOURCES:
        assert source.topic in TOPICS, source.code
        assert set(source.grains) <= grain_codes, source.code
        assert set(source.grains.values()) <= {"n", "d", "p"}, source.code
        assert source.gold, source.code
        for first, last in source.years:
            assert first <= last, source.code
    assert len(SOURCE_BY_CODE) == len(WAREHOUSE_SOURCES)


def test_sources_joins_the_live_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-183 — metric counts and grains come from the catalog row."""
    warehouse = _Session(
        [
            {
                "source_code": "CENSUS_BPS",
                "metrics": 12,
                "current_metrics": 11,
                "stale_metrics": 1,
                "geo_grains": ["COUNTY", "NATIONAL"],
                "time_grains": ["ANNUAL", "MONTHLY"],
                "newest_watermark": "2025-12",
                "newest_publication": None,
                "last_harvested": None,
            }
        ]
    )
    response = _client(monkeypatch, warehouse=warehouse).get(
        "/api/v1/admin/warehouse/sources"
    )
    assert response.status_code == 200
    body = response.json()
    assert [s["code"] for s in body["sources"]] == [s.code for s in WAREHOUSE_SOURCES]
    bps = next(s for s in body["sources"] if s["code"] == "CENSUS_BPS")
    assert bps["catalog"]["metrics"] == 12
    assert bps["catalog"]["geo_grains"] == ["COUNTY", "NATIONAL"]
    geo = next(s for s in body["sources"] if s["code"] == "CENSUS_GEO")
    assert geo["catalog"] is None
    assert "dim_metric_catalog" in warehouse.statements[0]


# --------------------------------------------------------------------- coverage


def test_coverage_reads_the_served_expressions_and_folds_grains() -> None:
    """Covers: API-183 — the registry's own grain and period expressions."""
    db = _Session(
        [],  # SET LOCAL statement_timeout
        [
            {
                "grain": "COUNTY",
                "first_year": 2004,
                "last_year": 2024,
                "years": [2004, 2024],
                "row_count": 9,
            },
            {
                "grain": "AGENCY",
                "first_year": 2019,
                "last_year": 2019,
                "years": [2019],
                "row_count": 1,
            },
        ],
    )
    result = coverage.measure_source(db, "FBI_UCR")
    dispatch = OBSERVATION_DISPATCH["FBI_UCR"]
    assert result["status"] == "ok"
    assert result["relation"] == dispatch.latest_relation
    assert "statement_timeout" in db.statements[0]
    assert dispatch.geo_level_expression in db.statements[1]
    assert dispatch.period_start_expression in db.statements[1]
    assert [(g["grain"], g["column"]) for g in result["grains"]] == [
        ("COUNTY", "county"),
        ("AGENCY", "other"),
    ]
    assert result["row_count"] == 10


@pytest.mark.parametrize("code", ["BLS", "FRED"])
def test_newest_only_sources_are_measured_on_their_releases(code: str) -> None:
    """Covers: API-183 — a newest-period table would report one year."""
    db = _Session([], [])
    result = coverage.measure_source(db, code)
    assert result["relation"] == OBSERVATION_DISPATCH[code].released_relation


def test_acs_is_read_from_its_year_partitions_not_scanned() -> None:
    """Covers: API-183 — the 1.8B-row table is never scanned for this page."""
    db = _Session(
        [
            {
                "partition_name": "rpt_acs_observations_2019",
                "partition_schema": "gold_census",
                "bound": "FOR VALUES FROM ('2019-01-01') TO ('2020-01-01')",
                "estimated_rows": 500,
            },
            {
                "partition_name": "rpt_acs_observations_2020",
                "partition_schema": "gold_census",
                "bound": "FOR VALUES FROM ('2020-01-01') TO ('2021-01-01')",
                "estimated_rows": 0,
            },
            {
                "partition_name": "rpt_acs_observations_2021",
                "partition_schema": "gold_census",
                "bound": "FOR VALUES FROM ('2021-01-01') TO ('2022-01-01')",
                "estimated_rows": 700,
            },
            {
                "partition_name": "rpt_acs_observations_unranged",
                "partition_schema": "gold_census",
                "bound": "DEFAULT",
                "estimated_rows": 3,
            },
        ],
        [("COUNTY",), ("TRACT",)],
    )
    result = coverage.measure_source(db, "CENSUS_ACS")
    assert result["status"] == "ok"
    assert result["method"] == "partition_catalog"
    assert result["row_count"] == 1200 and result["row_count_is_estimate"]
    assert {g["grain"]: g["years"] for g in result["grains"]} == {
        "COUNTY": [2019, 2021],
        "TRACT": [2019, 2021],
    }
    assert not any(
        "rpt_acs_observations\n" in s or "FROM gold_census.rpt" in s
        for s in db.statements
    )


def test_a_timeout_is_reported_and_cached_briefly() -> None:
    """Covers: API-183 — a slow source answers timed_out, not a 500."""

    class _Cancelled(Exception):
        pgcode = "57014"

    db = _Session(
        [],
        OperationalError(
            "SELECT", {}, _Cancelled("canceling statement due to statement timeout")
        ),
    )
    first = coverage.source_coverage(db, "CENSUS_LODES", now=0.0)
    assert first["status"] == "timed_out" and not first["cached"]
    assert db.rolled_back == 1
    again = coverage.source_coverage(db, "CENSUS_LODES", now=60.0)
    assert again["cached"]
    retried = coverage.source_coverage(
        _Session([], []), "CENSUS_LODES", now=coverage.FAILED_CACHE_SECONDS + 1.0
    )
    assert retried["status"] == "ok" and not retried["cached"]


def test_decennial_and_geography_are_described_not_scanned() -> None:
    """Covers: API-183 — shared and reference sources run no statement."""
    db = _Session()
    assert coverage.measure_source(db, "CENSUS_DECENNIAL")["status"] == "shared"
    assert coverage.measure_source(db, "CENSUS_GEO")["status"] == "not_served"
    assert db.statements == []


# ------------------------------------------------------------------------ atlas


def _relation(name: str, kind: str = "table", **extra: Any) -> Relation:
    values: dict[str, Any] = {
        "columns": [("id", "bigint", True), ("geo_sk", "bigint", False)],
        "primary_key": ["id"],
        "uniques": [],
        "foreign_keys": [],
        "reads_from": [],
        "partition_key": None,
        "partitions": 0,
        "estimated_rows": None,
        "index_count": 1,
    }
    values.update(extra)
    return Relation(name=name, kind=kind, **values)


def test_the_atlas_places_every_relation_and_only_draws_placed_edges() -> None:
    """Covers: API-183 — every relation lands in a diagram; no dangling edge."""
    relations = {
        r.name: r
        for r in [
            _relation("raw_capture.response_capture"),
            _relation("silver_ref.dim_geo_entity"),
            _relation(
                "silver_bea.observation_revision",
                foreign_keys=[(["geo_sk"], "silver_ref.dim_geo_entity")],
            ),
            _relation(
                "gold_bea.observation_latest",
                kind="view",
                reads_from=["silver_bea.observation_revision"],
            ),
            _relation("gold_mystery.brand_new_table"),
        ]
    }
    atlas = build_atlas(relations)
    placed = {r["name"]: r["domain"] for r in atlas["relations"]}
    assert all(placed.values()), placed
    assert placed["gold_mystery.brand_new_table"] == "other"
    bea = next(d for d in atlas["domains"] if d["source_code"] == "BEA")
    names = {b["name"] for b in bea["boxes"]}
    assert {"silver_bea.observation_revision", "gold_bea.observation_latest"} <= names
    for domain in atlas["domains"]:
        boxes = {b["name"] for b in domain["boxes"]}
        for edge in domain["edges"]:
            assert edge["from"] in boxes and edge["to"] in boxes, (domain["id"], edge)
    kinds = {(e["from"], e["to"]): e["kind"] for e in bea["edges"]}
    assert (
        kinds[("silver_bea.observation_revision", "gold_bea.observation_latest")]
        == "reads"
    )
    assert (
        kinds[("silver_ref.dim_geo_entity", "silver_bea.observation_revision")] == "fk"
    )
    assert atlas["stats"]["views"] == 1


def test_schema_route_returns_the_atlas(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-183 — the route answers the atlas it was given."""
    atlas = build_atlas(
        {"silver_ref.dim_geo_entity": _relation("silver_ref.dim_geo_entity")}
    )
    atlas["read_at"] = "2026-10-31T16:00:00Z"
    monkeypatch.setattr(admin_router, "warehouse_atlas", lambda _db: atlas)
    response = _client(monkeypatch).get("/api/v1/admin/warehouse/schema")
    assert response.status_code == 200
    body = response.json()
    assert body["stats"]["tables"] == 1
    assert body["relations"][0]["name"] == "silver_ref.dim_geo_entity"
