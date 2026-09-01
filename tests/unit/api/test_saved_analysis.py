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
from apps.api.main import app
from apps.api.middleware import CACHEABLE_PREFIXES
from apps.api.schemas import AnalysisDocument
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
}
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

        if sql.startswith("SELECT COUNT(*)"):
            return _Result(scalar=len(owned))

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

        if "ORDER BY name" in sql:
            ordered = sorted(
                owned, key=lambda row: (row["name"], row["configuration_id"])
            )
            window = ordered[params["offset"] : params["offset"] + params["limit"]]
            return _Result(rows=[dict(row) for row in window])

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
    """Covers: API-061 — a retired capability is stated, the document kept."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(),
        json={"name": "goes-stale", "document": _document()},
    )
    configuration_id = created.json()["configuration_id"]
    assert created.json()["validation"] == {"valid": True, "reason": None}

    # The warehouse retires the metric out from under the saved configuration.
    def _retired_warehouse():
        yield _WarehouseSession({})

    app.dependency_overrides[get_db_session_dep] = _retired_warehouse
    stale = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth()
    )

    assert stale.status_code == 200, "a stale configuration is still readable"
    payload = stale.json()
    assert payload["validation"]["valid"] is False
    assert "not a published metric" in payload["validation"]["reason"]
    assert payload["document"]["metric_code"] == "FRED:UNRATE", (
        "the user's document is preserved verbatim, never repaired"
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
            assert not path.startswith(CACHEABLE_PREFIXES), path


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
