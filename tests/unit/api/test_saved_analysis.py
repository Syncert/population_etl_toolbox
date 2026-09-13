"""API unit tests: saved analysis configurations (ADR-0003, API-007).

Covers: API-059 (bearer authentication: hashed storage, constant-time
        comparison, revoked and malformed credentials refused identically,
        unconfigured storage answers 503 rather than claiming a bad token,
        and no token value reaches a response, header, or log),
        API-060 (ownership is enforced in SQL and another owner's id is
        indistinguishable from one that never existed),
        API-061 (a document is validated against the same capability and
        compatibility contracts the live routes enforce, on write, and its
        staleness is reported rather than repaired on read),
        API-062 (optimistic concurrency: an update states the version it read
        and a mismatch is refused with the current version),
        API-063 (user content is never publicly cached: private no-store
        responses, paths outside the cacheable prefixes).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.auth import get_app_session_dep, hash_token
from apps.api.dependencies import get_db_session_dep
from apps.api.main import PUBLIC_CACHE_TARGETS, app
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.schemas import OBSERVATION_FILTER_BOUNDS, AnalysisDocument
from apps.api.registry import closed_value_refusal
from apps.api.services import saved_analysis_service

pytestmark = [pytest.mark.unit, pytest.mark.api]

_TOKEN = "test-token-value-do-not-log"
_OTHER_TOKEN = "second-account-token"
_NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)

_FRED_METRIC = {
    "metric_code": "FRED:UNRATE",
    "source_code": "FRED",
    "units": "Percent",
    "valid_time_grains": ["MONTHLY"],
    "valid_geo_grains": ["NATIONAL"],
    "aggregation_characteristic": None,
    "physical_lineage": {},
    # `resolve_metric` projects the harvested freshness state, and a document
    # naming a retired measure cannot be replayed (API-119). The fixtures carry
    # it because the row does.
    "freshness_state": "current",
}
_RETIRED_FRED_METRIC = {**_FRED_METRIC, "freshness_state": "retired"}
_CDC_METRIC = {
    "metric_code": "CDC:cdi:X:crude",
    "source_code": "CDC",
    "units": "percent",
    "valid_time_grains": ["ANNUAL"],
    "valid_geo_grains": ["STATE"],
    "aggregation_characteristic": None,
    "physical_lineage": {
        "schema": "gold_cdc",
        "relation": "health_observation",
        "asset_id": "cdi",
        "measure_id": "X",
        "value_type_id": "crude",
    },
}


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def one(self):
        return self._rows[0]

    def scalar(self):
        return self._scalar


class _WarehouseSession:
    """Answers glossary metric lookups only."""

    def __init__(self, metrics: dict[str, dict] | None = None):
        self._metrics = (
            metrics if metrics is not None else {"FRED:UNRATE": _FRED_METRIC}
        )

    def execute(self, query, params=None):
        if "gold_glossary.dim_metric" in str(query):
            row = self._metrics.get((params or {}).get("metric_code"))
            return _Result(rows=[row] if row else [])
        return _Result()


class _StorageSession:
    """An in-memory stand-in for app_api, honouring owner scoping in its keys."""

    def __init__(self, accounts: dict[str, tuple[int, str]], rows=None):
        self._accounts = accounts
        self.rows: list[dict[str, Any]] = list(rows or [])
        self._next_id = max((r["configuration_id"] for r in self.rows), default=0) + 1
        self.committed = 0
        self.statements: list[str] = []

    # -- session protocol ------------------------------------------------
    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        return None

    def execute(self, query, params=None):
        sql = " ".join(str(query).split())
        params = params or {}
        self.statements.append(sql)

        if "app_api.user_account" in sql:
            digest = params.get("token_sha256")
            entry = self._accounts.get(digest)
            if entry is None:
                return _Result(rows=[])
            account_id, label = entry
            return _Result(
                rows=[
                    {
                        "user_account_id": account_id,
                        "display_label": label,
                        "token_sha256": digest,
                    }
                ]
            )

        owner = params.get("owner_user_id")
        owned = [row for row in self.rows if row["owner_user_id"] == owner]

        if sql.startswith("SELECT 1 FROM app_api.saved_analysis_configuration"):
            clash = [
                row
                for row in owned
                if row["name"] == params.get("name")
                and row["configuration_id"] != params.get("configuration_id")
            ]
            return _Result(rows=[{"exists": 1}] if clash else [])

        if sql.startswith("INSERT INTO app_api.saved_analysis_configuration"):
            row = {
                "configuration_id": self._next_id,
                "owner_user_id": owner,
                "name": params["name"],
                "version": 1,
                # JSONB round-trips as a parsed object, as psycopg2 returns it.
                "document": json.loads(params["document"]),
                "created_at": _NOW,
                "updated_at": _NOW,
            }
            self._next_id += 1
            self.rows.append(row)
            return _Result(rows=[dict(row)])

        if sql.startswith("UPDATE app_api.saved_analysis_configuration"):
            for row in owned:
                if row["configuration_id"] != params["configuration_id"]:
                    continue
                if row["version"] != params["expected_version"]:
                    return _Result(rows=[])
                row["name"] = params["name"]
                row["document"] = json.loads(params["document"])
                row["version"] += 1
                row["updated_at"] = _NOW
                return _Result(rows=[dict(row)])
            return _Result(rows=[])

        if sql.startswith("SELECT version FROM"):
            match = [
                row
                for row in owned
                if row["configuration_id"] == params["configuration_id"]
            ]
            return _Result(scalar=match[0]["version"] if match else None)

        if sql.startswith("DELETE FROM app_api.saved_analysis_configuration"):
            match = [
                row
                for row in owned
                if row["configuration_id"] == params["configuration_id"]
            ]
            for row in match:
                self.rows.remove(row)
            return _Result(
                rows=[{"configuration_id": params["configuration_id"]}] if match else []
            )

        if sql.startswith("WITH owned AS"):
            # The listing takes its total and its page in one statement
            # (API-103), so every page row carries the total and an empty page
            # is the LEFT JOIN's count-only row.
            ordered = sorted(
                owned, key=lambda row: (row["name"], row["configuration_id"])
            )
            window = ordered[params["offset"] : params["offset"] + params["limit"]]
            if not window:
                return _Result(rows=[{"total": len(owned), "configuration_id": None}])
            return _Result(rows=[{**row, "total": len(owned)} for row in window])

        # single-row select
        match = [
            row
            for row in owned
            if row["configuration_id"] == params.get("configuration_id")
        ]
        return _Result(rows=[dict(row) for row in match])


def _document(**overrides) -> dict:
    payload = {
        "kind": "observations",
        "metric_code": "FRED:UNRATE",
        "scope": "latest",
        "filters": {"geo_level": "NATIONAL"},
        "visualization": {"chart": "line"},
    }
    payload.update(overrides)
    return payload


def _client(
    storage: _StorageSession,
    warehouse: _WarehouseSession | None = None,
    configured: bool = True,
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> TestClient:
    if monkeypatch is not None:
        monkeypatch.setenv(
            "APP_API_DATABASE_URL",
            "postgresql://app.invalid/app" if configured else "",
        )

    def _storage_override():
        yield storage

    def _warehouse_override():
        yield warehouse or _WarehouseSession()

    app.dependency_overrides[get_app_session_dep] = _storage_override
    app.dependency_overrides[get_db_session_dep] = _warehouse_override
    return TestClient(app)


def _auth(token: str = _TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def accounts() -> dict[str, tuple[int, str]]:
    return {
        hash_token(_TOKEN): (1, "primary"),
        hash_token(_OTHER_TOKEN): (2, "secondary"),
    }


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# API-059 — authentication
# ---------------------------------------------------------------------------


def test_tokens_are_stored_only_as_digests() -> None:
    """Covers: API-059 — the credential itself is never persisted."""
    digest = hash_token(_TOKEN)
    assert digest != _TOKEN
    assert len(digest) == 64
    assert hash_token(_TOKEN) == digest, "hashing is deterministic"


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer wrong-token"},
        {"Authorization": "Basic abc"},
        {"Authorization": "Bearer"},
        {"Authorization": "Bearer "},
    ],
    ids=("absent", "unknown", "wrong-scheme", "no-credential", "empty-credential"),
)
def test_missing_or_invalid_credentials_are_refused_identically(
    accounts, monkeypatch: pytest.MonkeyPatch, headers
) -> None:
    """Covers: API-059 — every failure is the same 401, never a hint."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    response = client.get("/api/v1/analysis-configurations", headers=headers)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json() == {"detail": "a valid bearer token is required"}


def test_revoked_token_is_refused_like_an_unknown_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-059 — revocation is immediate and does not leak state."""
    # A revoked account is simply absent from the active-token lookup.
    storage = _StorageSession(accounts={})
    client = _client(storage, monkeypatch=monkeypatch)
    response = client.get("/api/v1/analysis-configurations", headers=_auth())

    assert response.status_code == 401
    assert response.json() == {"detail": "a valid bearer token is required"}


def test_unconfigured_storage_answers_503_not_401(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-059 — an unverifiable credential is not a wrong credential."""
    storage = _StorageSession(accounts)
    client = _client(storage, configured=False, monkeypatch=monkeypatch)
    response = client.get("/api/v1/analysis-configurations", headers=_auth())

    assert response.status_code == 503
    assert "not configured" in response.json()["detail"]


def test_unconfigured_storage_answers_503_through_the_real_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-059 — the 503 survives real dependency resolution.

    The test above overrides ``get_app_session_dep``, which is exactly the
    dependency that fails when storage is unconfigured: FastAPI resolves a
    path operation's dependencies before its body runs, so ``require_account``
    never reached its own 503 guard on a real deployment. The override
    substituted the broken part, and the suite proved a branch that could not
    be entered. This exercises the real dependency chain.
    """
    monkeypatch.setenv("APP_API_DATABASE_URL", "")
    app.dependency_overrides.clear()
    try:
        response = TestClient(app).get(
            "/api/v1/analysis-configurations", headers=_auth()
        )
    finally:
        app.dependency_overrides.clear()

    # Not a 500: an unconfigured feature is a deployment fact the caller can
    # be told about, not a crash to page an operator over.
    assert response.status_code == 503
    assert "not configured" in response.json()["detail"]


def test_token_never_appears_in_responses_or_logs(
    accounts, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: API-059 — credentials stay out of bodies, headers, and logs."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    with caplog.at_level(logging.INFO):
        response = client.get("/api/v1/analysis-configurations", headers=_auth())

    assert response.status_code == 200
    assert _TOKEN not in response.text
    assert all(_TOKEN not in value for value in response.headers.values())
    assert all(_TOKEN not in record.getMessage() for record in caplog.records)
    assert all(
        hash_token(_TOKEN) not in record.getMessage() for record in caplog.records
    )


# ---------------------------------------------------------------------------
# API-060 — ownership and non-enumeration
# ---------------------------------------------------------------------------


def test_owner_scoping_hides_another_accounts_configuration(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-060 — another owner's id is a 404, not a 403."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)

    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(_OTHER_TOKEN),
        json={"name": "theirs", "document": _document()},
    )
    assert created.status_code == 201
    configuration_id = created.json()["configuration_id"]

    for method, kwargs in (
        ("get", {}),
        (
            "put",
            {
                "json": {
                    "name": "stolen",
                    "document": _document(),
                    "expected_version": 1,
                }
            },
        ),
        ("delete", {}),
    ):
        response = getattr(client, method)(
            f"/api/v1/analysis-configurations/{configuration_id}",
            headers=_auth(),
            **kwargs,
        )
        assert response.status_code == 404, method
        assert response.json() == {"detail": "configuration not found"}

    missing = client.get("/api/v1/analysis-configurations/999999", headers=_auth())
    assert missing.status_code == 404
    assert missing.json() == missing.json() | {"detail": "configuration not found"}

    listing = client.get("/api/v1/analysis-configurations", headers=_auth())
    assert listing.json()["total"] == 0, "another owner's rows are never selected"


def test_every_storage_statement_is_owner_scoped(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-060 — scoping is in the SQL, not a post-filter."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "mine", "document": _document()},
    )
    client.get(
        f"/api/v1/analysis-configurations/{created.json()['configuration_id']}",
        headers=_auth(),
    )
    client.get("/api/v1/analysis-configurations", headers=_auth())

    configuration_statements = [
        sql for sql in storage.statements if "saved_analysis_configuration" in sql
    ]
    assert configuration_statements
    for sql in configuration_statements:
        assert "owner_user_id" in sql, sql


# ---------------------------------------------------------------------------
# API-061 — validation against live contracts
# ---------------------------------------------------------------------------


def test_document_validated_against_live_capability_contracts(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-061 — persistence is not a back door around capabilities."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)

    unknown_metric = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={
            "name": "unknown",
            "document": _document(metric_code="NO:SUCH:METRIC"),
        },
    )
    assert unknown_metric.status_code == 422
    assert "not a published metric" in unknown_metric.json()["detail"]

    bad_filter = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={
            "name": "bad-filter",
            "document": _document(filters={"stratum_id": "s1"}),
        },
    )
    assert bad_filter.status_code == 422
    assert "stratum_id" in bad_filter.json()["detail"]

    contradiction = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={
            "name": "contradiction",
            "document": _document(release="2026-01-01", scope="latest"),
        },
    )
    assert contradiction.status_code == 422
    assert "as_released" in contradiction.json()["detail"]


def test_comparison_document_enforces_the_compatibility_policy(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-061 — a saved comparison obeys the same policy as a live one."""
    warehouse = _WarehouseSession(
        {"FRED:UNRATE": _FRED_METRIC, "CDC:cdi:X:crude": _CDC_METRIC}
    )
    storage = _StorageSession(accounts)
    client = _client(storage, warehouse, monkeypatch=monkeypatch)

    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={
            "name": "incompatible",
            "document": {
                "kind": "comparison",
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "CDC:cdi:X:crude",
                "filters": {},
                "visualization": {},
            },
        },
    )
    assert response.status_code == 422
    assert "not comparable" in response.json()["detail"]


def test_distribution_document_rejects_a_declined_source(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-061 — a stratified source cannot be saved as a distribution."""
    warehouse = _WarehouseSession({"CDC:cdi:X:crude": _CDC_METRIC})
    storage = _StorageSession(accounts)
    client = _client(storage, warehouse, monkeypatch=monkeypatch)

    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={
            "name": "cdc-bins",
            "document": {
                "kind": "distribution",
                "metric_code": "CDC:cdi:X:crude",
                "bin_count": 5,
                "filters": {},
                "visualization": {},
            },
        },
    )
    assert response.status_code == 422
    assert "stratified" in response.json()["detail"]


def test_stale_configuration_is_reported_on_read_not_repaired(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-061, API-119 — retirement is stated, the document kept.

    This simulated retirement by *deleting* the glossary row, so it passed for
    the wrong reason: the contract's retirement leaves the row in place with
    `freshness_state = 'retired'`, and that state was never read (API-119).
    The deleted-row path is asserted where it belongs, on the write of an
    unknown metric_code.
    """
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "goes-stale", "document": _document()},
    )
    configuration_id = created.json()["configuration_id"]
    assert created.json()["validation"] == {"valid": True, "reason": None}

    # The warehouse retires the metric out from under the saved configuration:
    # the row stays, its state changes.
    def _retired_warehouse():
        yield _WarehouseSession({"FRED:UNRATE": _RETIRED_FRED_METRIC})

    app.dependency_overrides[get_db_session_dep] = _retired_warehouse
    stale = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth()
    )

    assert stale.status_code == 200, "a stale configuration is still readable"
    payload = stale.json()
    assert payload["validation"]["valid"] is False
    assert "retired" in payload["validation"]["reason"]
    assert payload["document"]["metric_code"] == "FRED:UNRATE", (
        "the user's document is preserved verbatim, never repaired"
    )


def test_a_retired_measure_cannot_be_stored_as_a_configuration(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-119 — a document the API would not answer is not stored.

    `_require_metric` raised only when the glossary row was absent, so a
    configuration naming a measure the warehouse had already retired stored
    with `validation.valid = true` and replayed as an empty page. The guide's
    promise is that "a saved configuration cannot encode a request the API
    would refuse".
    """
    warehouse = _WarehouseSession({"FRED:UNRATE": _RETIRED_FRED_METRIC})
    client = _client(_StorageSession(accounts), warehouse, monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "retired-series", "document": _document()},
    )

    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert "retired" in detail
    assert "FRED:UNRATE" in detail
    assert "not a published metric" not in detail, (
        "the row is published; it is its observations that are not served"
    )


def test_visualization_block_is_stored_verbatim(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-061 — opaque user content is never interpreted."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    visualization = {"chart": "area", "palette": ["#123456"], "nested": {"a": [1, 2]}}

    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={
            "name": "viz",
            "document": _document(visualization=visualization),
        },
    )
    assert created.status_code == 201
    assert created.json()["document"]["visualization"] == visualization


# ---------------------------------------------------------------------------
# API-062 — optimistic concurrency
# ---------------------------------------------------------------------------


def test_update_requires_the_version_the_caller_read(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-062 — a stale update is refused with the current version."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "concurrent", "document": _document()},
    )
    configuration_id = created.json()["configuration_id"]
    assert created.json()["version"] == 1

    first = client.put(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=_auth(),
        json={
            "name": "concurrent",
            "document": _document(visualization={"chart": "bar"}),
            "expected_version": 1,
        },
    )
    assert first.status_code == 200
    assert first.json()["version"] == 2

    stale = client.put(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=_auth(),
        json={
            "name": "concurrent",
            "document": _document(visualization={"chart": "pie"}),
            "expected_version": 1,
        },
    )
    assert stale.status_code == 409
    assert "current version 2" in stale.json()["detail"]

    unchanged = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth()
    )
    assert unchanged.json()["document"]["visualization"] == {"chart": "bar"}


def test_delete_is_immediate_and_idempotent_for_the_owner(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-062 — deletion is a hard delete, effective at once."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "temporary", "document": _document()},
    )
    configuration_id = created.json()["configuration_id"]

    deleted = client.delete(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth()
    )
    assert deleted.status_code == 204
    assert storage.rows == []

    again = client.delete(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth()
    )
    assert again.status_code == 404


# ---------------------------------------------------------------------------
# API-063 — private content never publicly cached
# ---------------------------------------------------------------------------


def test_user_content_is_never_publicly_cacheable(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-063 — private no-store, and outside the cacheable prefixes."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "private", "document": _document()},
    )
    detail = client.get(
        f"/api/v1/analysis-configurations/{created.json()['configuration_id']}",
        headers=_auth(),
    )
    listing = client.get("/api/v1/analysis-configurations", headers=_auth())

    for response in (created, detail, listing):
        assert response.headers["cache-control"] == "private, no-store"
        assert "x-cache" not in response.headers

    for path in app.openapi()["paths"]:
        if "analysis-configurations" in path:
            assert not PUBLIC_CACHE_TARGETS.covers(path), path


def test_validation_helper_rejects_unknown_kinds_at_the_schema_boundary() -> None:
    """Covers: API-061 — an unmodelled document shape never reaches storage."""
    with pytest.raises(ValueError):
        AnalysisDocument.model_validate({"kind": "sql", "metric_code": "x"})
    with pytest.raises(ValueError):
        AnalysisDocument.model_validate(
            {"kind": "observations", "metric_code": "x", "unexpected": 1}
        )


def test_service_requires_metric_for_each_kind() -> None:
    """Covers: API-061 — a kind without its metric identity is refused."""
    warehouse = _WarehouseSession()
    with pytest.raises(saved_analysis_service.ConfigurationInvalid) as raised:
        saved_analysis_service.validate_document(
            warehouse,
            AnalysisDocument.model_validate({"kind": "observations"}),
        )
    assert "metric_code is required" in raised.value.detail


# ---------------------------------------------------------------------------
# API-082 — a saved view records the reduction it was viewed with
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"newest_per_geography": True, "scope": "latest"}, True),
        (
            {"newest_release_per_period": True, "scope": "as_released"},
            True,
        ),
        # Every contradiction the live route refuses, refused here too.
        ({"newest_per_geography": True, "scope": "as_released"}, False),
        ({"newest_release_per_period": True, "scope": "latest"}, False),
        (
            {
                "newest_release_per_period": True,
                "scope": "as_released",
                "release": "2024",
            },
            False,
        ),
        (
            {
                "newest_per_geography": True,
                "newest_release_per_period": True,
                "scope": "latest",
            },
            False,
        ),
    ],
    ids=(
        "per-geography-with-latest",
        "per-period-with-as-released",
        "per-geography-with-as-released",
        "per-period-with-latest",
        "per-period-with-a-pinned-release",
        "both-at-once",
    ),
)
def test_a_stored_reduction_matches_what_the_live_route_accepts(
    accounts, monkeypatch: pytest.MonkeyPatch, overrides: dict, expected: bool
) -> None:
    """Covers: API-082 — storable is exactly what the route would serve.

    A saved explorer map view is a request for one value per geography.
    Without somewhere to record that, the document replayed as the whole
    latest publication -- for Census PEP, 3,144 counties times six estimated
    years -- and a map drawn from it coloured whichever row arrived last.
    """
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "reduction", "document": _document(**overrides)},
    )

    if expected:
        assert response.status_code == 201, response.json()
        stored = response.json()["document"]
        for name, value in overrides.items():
            assert stored[name] == value
    else:
        assert response.status_code == 422, response.json()


def test_a_document_stored_before_the_fields_existed_replays_unchanged(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-082 — both default to false, so nothing stored moves."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "unchanged", "document": _document()},
    )

    assert response.status_code == 201
    stored = response.json()["document"]
    assert stored["newest_per_geography"] is False
    assert stored["newest_release_per_period"] is False


def test_the_document_still_forbids_an_undeclared_key(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-082 — two declared fields, not an open door."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "extra", "document": _document(newest_per_county=True)},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# API-112 — a document carries only what its own route can send
# ---------------------------------------------------------------------------


def test_the_fields_each_kind_carries_are_the_ones_its_route_declares() -> None:
    """Covers: API-112 — the per-kind field sets are read, not asserted.

    `AnalysisDocument` is one model for three kinds, and the three routes do
    not take the same parameters. The registry says which fields each kind
    carries; this checks that claim against the served contract in both
    directions, so a parameter added to one of the three routes without a
    line in the registry fails rather than becoming a field nothing validates.
    """
    from apps.api.registry import CONFIGURATION_DOCUMENT_FIELDS, CONFIGURATION_ROUTES

    document = app.openapi()
    # `kind` names the route; `filters` is the per-source capability
    # contract, checked elsewhere; `visualization` is opaque user content.
    fields = set(AnalysisDocument.model_fields) - {"kind", "filters", "visualization"}
    assert fields, "parsing broke: the document declares no request fields"

    assert set(CONFIGURATION_ROUTES) == set(CONFIGURATION_DOCUMENT_FIELDS)
    for kind, path in CONFIGURATION_ROUTES.items():
        operation = document["paths"][path]["get"]
        declared = {
            parameter["name"]
            for parameter in operation.get("parameters") or []
            if parameter.get("in") == "query"
        }
        credited = CONFIGURATION_DOCUMENT_FIELDS[kind]
        assert credited <= declared, (
            f"{path} declares no {sorted(credited - declared)}, which the "
            f"registry says a {kind} configuration carries"
        )
        withheld = (fields - credited) & declared
        assert not withheld, (
            f"{path} accepts {sorted(withheld)}, which the registry withholds "
            f"from a {kind} configuration: a storable field nothing validates"
        )


@pytest.mark.parametrize(
    ("overrides", "refused"),
    [
        # A release pinned on a kind whose route has no release to send it
        # to: the reader saved "as released in 2022" and would reopen to the
        # latest publication, told nothing.
        (
            {
                "kind": "distribution",
                "scope": "as_released",
                "release": "2022-01-01",
            },
            ("release", "scope"),
        ),
        (
            {"kind": "distribution", "newest_per_geography": True},
            ("newest_per_geography",),
        ),
        (
            {
                "kind": "comparison",
                "metric_code": None,
                "metric_code_a": "FRED:UNRATE",
                "metric_code_b": "FRED:UNRATE",
                "bin_count": 5,
            },
            ("bin_count",),
        ),
        # The pair belongs to the comparison route alone; an observations
        # document carrying one stores a second intent nothing replays.
        ({"metric_code_a": "FRED:UNRATE"}, ("metric_code_a",)),
        ({"bin_count": 7}, ("bin_count",)),
    ],
)
def test_a_field_the_route_cannot_send_is_not_stored(
    accounts, monkeypatch: pytest.MonkeyPatch, overrides: dict, refused: tuple
) -> None:
    """Covers: API-112 — an intent the API cannot honour is not stored.

    The document's own docstring promises a stored configuration "can never
    encode a request the API would refuse". This is the case that is worse:
    not a request the API refuses, but one it accepted and could not replay.
    `/distribution/bins` and `/comparison` take neither a scope, a release
    nor a reduction, so those values were stored, reported `valid: true`
    every time they were read, and dropped silently on the way back to the
    route.

    The asymmetry was the tell: `extra="forbid"` refused a field the API had
    never heard of and accepted one it knew its route could not use.
    """
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "cross-kind", "document": _document(**overrides)},
    )

    assert response.status_code == 422, response.json()
    detail = response.json()["detail"]
    for name in refused:
        assert name in detail, detail
    # The refusal says where the value could not have gone, and what the
    # kind does carry, so the reader can fix the document rather than guess.
    assert overrides.get("kind", "observations") in detail
    assert "has no such parameter" in detail


@pytest.mark.parametrize(
    "kind_fields",
    [
        {"kind": "distribution", "bin_count": 9},
        {"kind": "distribution"},
        {
            "kind": "comparison",
            "metric_code": None,
            "metric_code_a": "FRED:UNRATE",
            "metric_code_b": "FRED:UNRATE",
        },
        {"scope": "as_released", "release": "2022-01-01"},
        {"scope": "latest", "newest_per_geography": True},
    ],
)
def test_a_field_the_kind_does_carry_is_stored_as_it_is(
    accounts, monkeypatch: pytest.MonkeyPatch, kind_fields: dict
) -> None:
    """Covers: API-112 — the refusal is narrow.

    A field left at its default changes no request, so it is never a refusal:
    every kind's document spells `scope`, `release` and both reductions in
    JSONB whether or not its route can use them, and a document written
    before this check existed must keep validating exactly as it did.
    """
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "carried", "document": _document(**kind_fields)},
    )

    assert response.status_code == 201, response.json()
    stored = response.json()["document"]
    for name, value in kind_fields.items():
        assert stored[name] == value
    assert response.json()["validation"] == {"valid": True, "reason": None}


def test_a_stored_cross_kind_field_is_reported_on_read_not_repaired(
    accounts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-112 — a document already stored is stated, never rewritten.

    A configuration written before this check keeps its content: the read
    reports it invalid with the reason, which is the same answer this API
    gives a configuration whose metric was retired. Repairing it would
    substitute the API's guess for the reader's intent -- and the guess here
    would be either "you meant the latest publication" or "you meant a
    different kind", which are different analyses.
    """
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "written-before", "document": _document(kind="distribution")},
    )
    configuration_id = created.json()["configuration_id"]
    assert created.json()["validation"]["valid"] is True

    # What such a row looks like in storage: a pin the route never took.
    storage.rows[0]["document"]["scope"] = "as_released"
    storage.rows[0]["document"]["release"] = "2022-01-01"

    read = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth()
    )
    assert read.status_code == 200, "the reader's content is still readable"
    payload = read.json()
    assert payload["validation"]["valid"] is False
    assert "release" in payload["validation"]["reason"]
    assert payload["document"]["release"] == "2022-01-01", (
        "the document is returned verbatim, never repaired"
    )


# ---------------------------------------------------------------------------
# API-091 — a stored filter value the live route would refuse
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"geo_id": "x" * 201}, "at most 200 characters"),
        ({"geo_level": "x" * 51}, "at most 50 characters"),
        ({"year_from": 99999}, "at most 2200"),
        ({"year_to": 1600}, "at least 1700"),
        ({"year_from": "not-a-year"}, "must be a whole number"),
    ],
)
def test_a_filter_value_the_route_refuses_is_not_stored(filters, expected) -> None:
    """Covers: API-091 — the names were checked and the values were not.

    `AnalysisDocument` promises a stored configuration can never encode a
    request the API would refuse. A 5,000-character `geo_id` against the
    route's declared 200 stored clean, listed clean, reported `valid: true`,
    and failed only when its owner tried to reopen it.
    """
    document = AnalysisDocument(
        kind="observations",
        metric_code=_CDC_METRIC["metric_code"],
        filters=filters,
    )
    with pytest.raises(saved_analysis_service.ConfigurationInvalid) as refused:
        saved_analysis_service.validate_document(
            _WarehouseSession({_CDC_METRIC["metric_code"]: _CDC_METRIC}), document
        )
    assert expected in refused.value.detail
    assert next(iter(filters)) in refused.value.detail


def test_a_filter_value_inside_the_bound_is_stored_as_it_is() -> None:
    """Covers: API-091 — the check refuses, it does not rewrite."""
    filters = {"geo_id": "x" * 200, "year_from": 1700, "year_to": 2200}
    document = AnalysisDocument(
        kind="observations",
        metric_code=_CDC_METRIC["metric_code"],
        filters=dict(filters),
    )
    saved_analysis_service.validate_document(
        _WarehouseSession({_CDC_METRIC["metric_code"]: _CDC_METRIC}), document
    )
    assert document.filters == filters


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        # Coerced with `int()` and stored as valid, while the route answers
        # `?year_from=2020.7` with a 422: the bound was measured against
        # 2020, a year the caller never wrote.
        ({"year_from": 2020.7}, "must be a whole number"),
        ({"year_to": "2020.7"}, "must be a whole number"),
        # `int(True) == 1`, so the reason used to be "must be at least 1700".
        ({"year_from": True}, "must be a whole number"),
        ({"year_from": "not a year"}, "must be a whole number"),
        ({"year_from": ""}, "must be a whole number"),
        ({"year_from": None}, "must be a value"),
        # A null was refused for `state_fips` (bound 2) and stored for
        # `geo_id` (bound 200), decided by the width of the bound.
        ({"geo_id": None}, "must be a value"),
        ({"state_fips": None}, "must be a value"),
        # `filters` maps a name to *the* value and these parameters take one.
        ({"geo_id": ["01", "02"]}, "must be a single value"),
        ({"geo_id": {"nested": "object"}}, "must be a single value"),
        ({"county_fips": []}, "must be a single value"),
        # Inside the bound and outside the closed set: two characters is what
        # `state_fips` allows and `ZZ` is not a state code, one digit is not
        # the shape the reference layer stores, and `NOPE` is not a grain
        # (API-123). The live routes refuse all three (API-122), so a
        # document carrying one would store clean and replay as a refusal.
        ({"state_fips": "ZZ"}, "must be two digits"),
        ({"state_fips": 6}, "must be two digits"),
        ({"county_fips": "ZZZ"}, "must be three digits"),
        ({"geo_level": "NOPE"}, "must be one of"),
        ({"geo_level": "COUNTRY"}, "must be one of"),
    ],
)
def test_a_filter_value_must_be_one_value_the_route_could_receive(
    filters: dict, expected: str
) -> None:
    """Covers: API-105, API-123 — the shape is stated before the length is measured.

    API-091 checked how long a value is and not what it is, and
    `AnalysisDocument.filters` is `dict[str, Any]`, so an array, an object, a
    null or a fractional number stored clean and reported valid. API-123 adds
    the cases that are the right shape and the right length and still outside
    the closed set the route accepts.
    """
    document = AnalysisDocument(
        kind="observations",
        metric_code=_FRED_METRIC["metric_code"],
        filters=dict(filters),
    )
    with pytest.raises(saved_analysis_service.ConfigurationInvalid) as refused:
        saved_analysis_service.validate_document(
            _WarehouseSession({_FRED_METRIC["metric_code"]: _FRED_METRIC}), document
        )
    assert expected in refused.value.detail
    assert next(iter(filters)) in refused.value.detail


@pytest.mark.parametrize(
    "filters",
    [
        # Read off the live route against a real warehouse: it passes
        # validation for each of these, so refusing them at write would be
        # stricter than the API and a document would be refused for a
        # request that works.
        #
        # `{"state_fips": 6}` used to be here and moved to the refused set
        # above: the live route refuses a one-digit state code now (API-122),
        # because the reference layer stores `06` and `6` can never match it,
        # so storing it is no longer "a request that works".
        {"year_from": "2020.0"},
        {"year_from": 2020.0},
        {"year_from": "2020"},
        {"year_to": 2020},
        {"geo_id": ""},
    ],
)
def test_a_value_the_route_accepts_is_still_stored(filters: dict) -> None:
    """Covers: API-105 — the shape check refuses nothing the route allows."""
    document = AnalysisDocument(
        kind="observations",
        metric_code=_FRED_METRIC["metric_code"],
        filters=dict(filters),
    )
    saved_analysis_service.validate_document(
        _WarehouseSession({_FRED_METRIC["metric_code"]: _FRED_METRIC}), document
    )
    assert document.filters == filters


def test_every_filter_a_source_declares_has_a_bound() -> None:
    """Covers: API-105 — a filter with no bound is an unvalidated value.

    `_require_declared_filters` skips a name it has no bound for, so a filter
    added to a dispatch without an entry here reopens API-091 for that filter
    and nothing says so. The two sets agree today by coincidence; this is the
    coincidence asserted.
    """
    # Read from the declarations alone. This used to seed
    # `{"geo_level", "state_fips"}` -- the hand-written set the analysis
    # kinds unioned in -- so it asserted that widening rather than the
    # sources' own declarations (API-117). Both names are declared by
    # sources anyway, so nothing is lost by asking.
    declared: set[str] = set()
    for dispatch in OBSERVATION_DISPATCH.values():
        declared |= set(dispatch.supported_filters())
    assert declared == set(OBSERVATION_FILTER_BOUNDS), (
        "every filter a source declares must have a bound, and every bound "
        "must belong to a filter some source declares"
    )


@pytest.mark.parametrize(
    ("kind", "metric", "filters", "fragment"),
    [
        # The route takes `state_fips`; Census PEP declares no such filter,
        # because `gold_pep.population_estimate_latest` carries no fips
        # columns. Stored clean, replayed as a 422.
        (
            "distribution",
            "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
            {"state_fips": "06"},
            "filters not supported for source 'CENSUS_PEP': state_fips",
        ),
        (
            "comparison",
            "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
            {"state_fips": "06"},
            "filters not supported for source 'CENSUS_PEP': state_fips",
        ),
        # The mirror: Census ACS declares `year_from` and `geo_id`, and the
        # analysis routes have no such parameter, so the replay is the
        # strict-parameter refusal (API-093) rather than a source refusal.
        (
            "distribution",
            "CENSUS_ACS:acs5:B01003_001",
            {"year_from": 2020},
            "filters not accepted by /api/v1/distribution/bins: year_from",
        ),
        (
            "comparison",
            "CENSUS_ACS:acs5:B01003_001",
            {"geo_id": "state:06|county:025"},
            "filters not accepted by /api/v1/comparison: geo_id",
        ),
    ],
)
def test_an_analysis_filter_is_one_its_own_route_accepts(
    accounts,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    metric: str,
    filters: dict,
    fragment: str,
) -> None:
    """Covers: API-117 — the accepted set is an intersection, not a union.

    The guide promises "a saved configuration cannot encode a request the
    API would refuse". API-091 made that true of filter names and API-105 of
    filter values, for the observations kind. The analysis kinds *unioned*
    `{geo_level, state_fips}` into the accepted set, while the live routes
    union nothing: they pass exactly those two parameters through
    `_filter_conditions`, which rejects any name the source does not
    declare. So both directions stored clean and replayed as a 422.
    """
    warehouse = _WarehouseSession(
        {
            metric: {
                **_FRED_METRIC,
                "metric_code": metric,
                "source_code": metric.split(":", 1)[0],
            }
        }
    )
    client = _client(
        _StorageSession(accounts), warehouse=warehouse, monkeypatch=monkeypatch
    )
    document = {
        "kind": kind,
        "filters": filters,
        "visualization": {},
    }
    if kind == "comparison":
        document["metric_code_a"] = metric
        document["metric_code_b"] = metric
    else:
        document["metric_code"] = metric

    response = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "analysis-filter", "document": document},
    )

    assert response.status_code == 422, response.json()
    assert fragment in response.json()["detail"]


def test_the_analysis_filters_a_document_may_carry_are_the_routes_own() -> None:
    """Covers: API-117 — the declared set is read against the served contract.

    The registry names the filters each kind's route takes. That claim is
    checked here from the served document in both directions, so a parameter
    added to `/distribution/bins` or `/comparison` -- or removed from one --
    fails rather than leaving a document able to carry a filter nothing
    would accept.

    `observations` is `None`: the neutral route declares one query parameter
    per filter in the union of every source's declared set, so its accepted
    filters *are* the source's, and narrowing them here would refuse
    documents the route serves.
    """
    from apps.api.registry import (
        CONFIGURATION_FILTER_PARAMETERS,
        CONFIGURATION_ROUTES,
    )

    document = app.openapi()
    every_declared_filter: set[str] = set()
    for dispatch in OBSERVATION_DISPATCH.values():
        every_declared_filter |= set(dispatch.supported_filters())

    for kind, route in CONFIGURATION_ROUTES.items():
        declared = {
            parameter["name"]
            for parameter in document["paths"][route]["get"]["parameters"]
            if parameter.get("in") == "query"
        }
        # The filters the route takes: its query parameters that name a
        # filter some source declares.
        filters_it_takes = declared & every_declared_filter
        expected = CONFIGURATION_FILTER_PARAMETERS[kind]
        if expected is None:
            assert filters_it_takes == every_declared_filter, (
                f"{route} no longer takes every declared filter, so a "
                "document validated against the source's set could carry one "
                "it would refuse"
            )
        else:
            assert filters_it_takes == set(expected), (
                f"{route} takes {sorted(filters_it_takes)}; the registry says "
                f"{sorted(expected)}"
            )


def test_the_declared_bounds_are_the_ones_the_route_serves() -> None:
    """Covers: API-091 — one declaration, and the contract proves it.

    The route reads these bounds, so the two agree by construction today. This
    asserts it against the contract the application actually serves, so they
    still agree if the route ever stops reading the declaration.
    """
    parameters = {
        parameter["name"]: parameter["schema"]
        for parameter in app.openapi()["paths"]["/api/v1/observations"]["get"][
            "parameters"
        ]
    }
    for name, bound in OBSERVATION_FILTER_BOUNDS.items():
        served = parameters[name]
        # Optional query parameters are served as `anyOf[type, null]`.
        shape = next(entry for entry in served["anyOf"] if entry.get("type") != "null")
        if bound.max_length is not None:
            assert shape["maxLength"] == bound.max_length, name
        if bound.minimum is not None:
            assert shape["minimum"] == bound.minimum, name
        if bound.maximum is not None:
            assert shape["maximum"] == bound.maximum, name


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("geo_level", "NOPE"),
        ("state_fips", "ZZ"),
        ("county_fips", "ZZZ"),
    ],
)
def test_storage_and_the_route_refuse_the_same_value(name: str, value: str) -> None:
    """Covers: API-123 — storage is not a back door for a refused request.

    API-117 made storage refuse a filter *name* the route would refuse, and
    said why: "storage is not a back door for a request the API would
    refuse", and "a filter ... stored clean and replayed as a 422". The value
    was never checked, so API-122's closed-set refusal reopened exactly that
    gap the moment it landed. Both layers read
    `registry.closed_value_refusal`, and this asserts the agreement rather
    than the implementation: the live route refuses it, and so does the
    document validator, with the same reason text.
    """
    route_refusal = closed_value_refusal(name, value)
    assert route_refusal is not None, f"{name}={value} is not refused at all"

    document = AnalysisDocument(
        kind="observations",
        metric_code=_FRED_METRIC["metric_code"],
        filters={name: value},
    )
    with pytest.raises(saved_analysis_service.ConfigurationInvalid) as refused:
        saved_analysis_service.validate_document(
            _WarehouseSession({_FRED_METRIC["metric_code"]: _FRED_METRIC}), document
        )
    assert route_refusal in refused.value.detail
    assert name in refused.value.detail
