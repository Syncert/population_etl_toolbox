"""API unit tests: evidence packets (ADR-0004).

Covers: API-068 (the shared request-body bound refuses an oversize body on
        every authenticated write path before any parsing, by declared
        length and as a chunked body streams, and never touches storage),
        API-069 (packet ownership is enforced in SQL and another owner's id
        is indistinguishable from one that never existed),
        API-070 (contradictions are refused at write naming the block, while
        incompleteness is stored and reported per block on read, and a
        measure retired after storage is reported -- not repaired -- on the
        block that carries it),
        API-071 (optimistic concurrency, hard deletion, private no-store
        responses outside the cacheable prefixes, and a list summary that
        carries size but never a verdict).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.auth import get_app_session_dep, hash_token
from apps.api.dependencies import get_db_session_dep
from apps.api.main import PUBLIC_CACHE_TARGETS, app
from apps.api.middleware import (
    REQUEST_TOO_LARGE_DETAIL,
    RequestBodyLimitMiddleware,
)
from apps.api.schemas.evidence_packet import EvidencePacketDocument
from apps.api.services import evidence_packet_service

pytestmark = [pytest.mark.unit, pytest.mark.api]

_TOKEN = "packet-token-value-do-not-log"
_OTHER_TOKEN = "second-packet-account-token"
_NOW = datetime(2026, 9, 12, tzinfo=timezone.utc)

_FRED_METRIC = {
    "metric_code": "FRED:UNRATE",
    "source_code": "FRED",
    "units": "Percent",
    "valid_time_grains": ["MONTHLY"],
    "valid_geo_grains": ["NATIONAL"],
    "aggregation_characteristic": None,
    "physical_lineage": {},
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
    """Answers glossary metric lookups only, and counts them."""

    def __init__(self, metrics: dict[str, dict] | None = None):
        self._metrics = (
            metrics if metrics is not None else {"FRED:UNRATE": _FRED_METRIC}
        )
        self.lookups: list[str] = []

    def execute(self, query, params=None):
        if "gold_glossary.dim_metric" in str(query):
            code = (params or {}).get("metric_code")
            self.lookups.append(code)
            row = self._metrics.get(code)
            return _Result(rows=[row] if row else [])
        return _Result()


class _StorageSession:
    """An in-memory app_api stand-in, honouring owner scoping in its keys."""

    def __init__(self, accounts: dict[str, tuple[int, str]]):
        self._accounts = accounts
        self.rows: list[dict[str, Any]] = []
        self._next_id = 1
        self.statements: list[str] = []

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def execute(self, query, params=None):
        sql = " ".join(str(query).split())
        params = params or {}
        self.statements.append(sql)

        if "app_api.user_account" in sql:
            entry = self._accounts.get(params.get("token_sha256"))
            if entry is None:
                return _Result(rows=[])
            account_id, label = entry
            return _Result(
                rows=[
                    {
                        "user_account_id": account_id,
                        "display_label": label,
                        "token_sha256": params.get("token_sha256"),
                    }
                ]
            )

        owner = params.get("owner_user_id")
        owned = [row for row in self.rows if row["owner_user_id"] == owner]

        if sql.startswith("SELECT 1 FROM app_api.evidence_packet"):
            clash = [
                row
                for row in owned
                if row["name"] == params.get("name")
                and row["packet_id"] != params.get("packet_id")
            ]
            return _Result(rows=[{"exists": 1}] if clash else [])
        if sql.startswith("INSERT INTO app_api.evidence_packet"):
            row = {
                "packet_id": self._next_id,
                "owner_user_id": owner,
                "name": params["name"],
                "version": 1,
                "document": json.loads(params["document"]),
                "created_at": _NOW,
                "updated_at": _NOW,
            }
            self._next_id += 1
            self.rows.append(row)
            return _Result(rows=[dict(row)])
        if sql.startswith("UPDATE app_api.evidence_packet"):
            for row in owned:
                if row["packet_id"] != params["packet_id"]:
                    continue
                if row["version"] != params["expected_version"]:
                    return _Result(rows=[])
                row["name"] = params["name"]
                row["document"] = json.loads(params["document"])
                row["version"] += 1
                return _Result(rows=[dict(row)])
            return _Result(rows=[])
        if sql.startswith("SELECT version FROM"):
            match = [row for row in owned if row["packet_id"] == params["packet_id"]]
            return _Result(scalar=match[0]["version"] if match else None)
        if sql.startswith("DELETE FROM app_api.evidence_packet"):
            match = [row for row in owned if row["packet_id"] == params["packet_id"]]
            for row in match:
                self.rows.remove(row)
            return _Result(rows=[{"packet_id": params["packet_id"]}] if match else [])
        if sql.startswith("WITH owned AS"):
            # The listing takes its total and its page in one statement
            # (API-103), so every page row carries the total and an empty page
            # is the LEFT JOIN's count-only row.
            ordered = sorted(owned, key=lambda row: (row["name"], row["packet_id"]))
            window = ordered[params["offset"] : params["offset"] + params["limit"]]
            if not window:
                return _Result(rows=[{"total": len(owned), "packet_id": None}])
            return _Result(rows=[{**row, "total": len(owned)} for row in window])
        match = [row for row in owned if row["packet_id"] == params.get("packet_id")]
        return _Result(rows=[dict(row) for row in match])


def _query(**overrides) -> dict:
    document = {
        "kind": "observations",
        "metric_code": "FRED:UNRATE",
        "scope": "latest",
        "filters": {"geo_level": "NATIONAL"},
        "visualization": {},
    }
    document.update(overrides)
    return document


def _envelope(**overrides) -> dict:
    envelope = {
        "metric_codes": ["FRED:UNRATE"],
        "source_codes": ["FRED"],
        "geo_id": "US",
        "geo_level": "NATIONAL",
        "scope": "latest",
        "release": "",
        "period": "2026-08",
        "units": "Percent",
        "transformation": "none",
        "api_query": "/api/v1/observations?metric_code=FRED%3AUNRATE",
        "caveats": [],
    }
    envelope.update(overrides)
    return envelope


def _block(block_id="unemployment", type="analysis", **overrides) -> dict:
    block = {"block_id": block_id, "type": type, "title": "Unemployment"}
    if type in {"analysis", "table", "map"}:
        block["envelope"] = _envelope()
        block["document"] = _query()
    block.update(overrides)
    return block


def _packet(*blocks, title="Needs assessment") -> dict:
    return {
        "schema_version": 1,
        "title": title,
        "purpose": "Describe the need",
        "blocks": list(blocks)
        if blocks
        else [
            {
                "block_id": "summary",
                "type": "text",
                "title": "Summary",
                "content": "...",
            },
            _block(),
        ],
    }


def _client(
    storage: _StorageSession,
    warehouse: _WarehouseSession | None = None,
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> TestClient:
    if monkeypatch is not None:
        monkeypatch.setenv("APP_API_DATABASE_URL", "postgresql://app.invalid/app")
    active_warehouse = warehouse or _WarehouseSession()

    def _storage_override():
        yield storage

    def _warehouse_override():
        yield active_warehouse

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
# API-068 — the shared request-body bound
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path", ["/api/v1/evidence-packets", "/api/v1/analysis-configurations"]
)
def test_oversize_declared_body_is_refused_before_parsing(
    accounts, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    """Covers: API-068 — a declared length over the bound never reaches storage."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    padding = "x" * 300_000
    response = client.post(
        path,
        headers=_auth(),
        json={"name": "huge", "document": {"padding": padding}},
    )
    assert response.status_code == 413
    assert response.json() == {"detail": REQUEST_TOO_LARGE_DETAIL}
    assert response.headers["cache-control"] == "no-store"
    assert storage.statements == [], "the body was refused before any storage statement"


def test_chunked_oversize_body_is_refused_as_it_streams() -> None:
    """Covers: API-068 — omitting content-length does not dodge the bound."""
    seen: list[str] = []

    async def inner(scope, receive, send):
        seen.append("entered")
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                seen.append("disconnected")
                return
        # An application that reached here would have parsed a truncated body.

    middleware = RequestBodyLimitMiddleware(inner, max_bytes=100)
    chunks = [
        {"type": "http.request", "body": b"a" * 60, "more_body": True},
        {"type": "http.request", "body": b"b" * 60, "more_body": True},
        {"type": "http.request", "body": b"c" * 60, "more_body": False},
    ]
    sent: list[dict] = []

    async def receive():
        return chunks.pop(0)

    async def send(message):
        sent.append(message)

    import asyncio

    asyncio.run(
        middleware(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/v1/evidence-packets",
                "headers": [],
            },
            receive,
            send,
        )
    )
    assert seen == ["entered", "disconnected"]
    assert sent[0]["type"] == "http.response.start" and sent[0]["status"] == 413
    assert chunks, "the third chunk was never read: the stream was cut at the bound"


def test_body_exactly_at_the_bound_is_accepted() -> None:
    """Covers: API-068 — the bound is inclusive."""
    delivered: list[bytes] = []

    async def inner(scope, receive, send):
        message = await receive()
        delivered.append(message["body"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = RequestBodyLimitMiddleware(inner, max_bytes=10)
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"0123456789", "more_body": False}

    async def send(message):
        sent.append(message)

    import asyncio

    asyncio.run(
        middleware(
            {
                "type": "http",
                "method": "POST",
                "path": "/x",
                "headers": [(b"content-length", b"10")],
            },
            receive,
            send,
        )
    )
    assert delivered == [b"0123456789"]
    assert sent[0]["status"] == 200


# ---------------------------------------------------------------------------
# API-069 — ownership and non-enumeration
# ---------------------------------------------------------------------------


def test_another_owners_packet_is_a_404_not_a_403(accounts, monkeypatch) -> None:
    """Covers: API-069 — indistinguishable from one that never existed."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(_OTHER_TOKEN),
        json={"name": "theirs", "document": _packet()},
    )
    assert created.status_code == 201, created.text
    packet_id = created.json()["packet_id"]

    for method, kwargs in (
        ("get", {}),
        (
            "put",
            {"json": {"name": "stolen", "document": _packet(), "expected_version": 1}},
        ),
        ("delete", {}),
    ):
        response = getattr(client, method)(
            f"/api/v1/evidence-packets/{packet_id}", headers=_auth(), **kwargs
        )
        assert response.status_code == 404, method
        assert response.json() == {"detail": "packet not found"}

    missing = client.get("/api/v1/evidence-packets/999999", headers=_auth())
    assert missing.json() == {"detail": "packet not found"}
    assert client.get("/api/v1/evidence-packets", headers=_auth()).json()["total"] == 0

    packet_statements = [sql for sql in storage.statements if "evidence_packet" in sql]
    assert packet_statements
    for sql in packet_statements:
        assert "owner_user_id" in sql, sql


def test_unauthenticated_requests_are_refused(accounts, monkeypatch) -> None:
    """Covers: API-069 — every packet route requires a bearer token."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    assert client.get("/api/v1/evidence-packets").status_code == 401
    assert (
        client.post(
            "/api/v1/evidence-packets", json={"name": "x", "document": _packet()}
        ).status_code
        == 401
    )
    assert (
        client.get("/api/v1/evidence-packets/1", headers=_auth("wrong")).status_code
        == 401
    )


# ---------------------------------------------------------------------------
# API-070 — contradictions refused, incompleteness reported
# ---------------------------------------------------------------------------


def test_incomplete_analytical_block_is_stored_and_reported(
    accounts, monkeypatch
) -> None:
    """Covers: API-070 — save-and-come-back-to-it must work."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    empty = {"block_id": "condition", "type": "analysis", "title": "Condition"}
    partial = _block(
        "partial",
        envelope=_envelope(period="", api_query=""),
    )
    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "draft", "document": _packet(_block(), empty, partial)},
    )
    assert created.status_code == 201, created.text
    validation = created.json()["validation"]
    assert validation["valid"] is False
    assert validation["reason"] == "2 of 3 analytical blocks cannot be read as evidence"
    by_id = {state["block_id"]: state for state in validation["blocks"]}
    assert by_id["unemployment"] == {
        "block_id": "unemployment",
        "valid": True,
        "reason": None,
        "missing": [],
    }
    assert by_id["condition"]["valid"] is False
    assert "no reproducibility envelope" in by_id["condition"]["reason"]
    assert by_id["condition"]["missing"] == [
        "metric_codes",
        "source_codes",
        "geo_id",
        "period",
        "api_query",
    ]
    assert by_id["partial"]["missing"] == ["period", "api_query"]
    assert "without the context" in by_id["partial"]["reason"]

    # The document is stored exactly as composed, gaps and all.
    detail = client.get(
        f"/api/v1/evidence-packets/{created.json()['packet_id']}", headers=_auth()
    )
    stored = {block["block_id"]: block for block in detail.json()["document"]["blocks"]}
    assert stored["condition"]["envelope"] is None
    assert stored["partial"]["envelope"]["period"] == ""


@pytest.mark.parametrize(
    ("block", "fragment"),
    [
        pytest.param(
            _block(envelope=_envelope(metric_codes=["FRED:UNRATE", "FRED:CPIAUCSL"])),
            "names measure(s) FRED:CPIAUCSL in its envelope that its query does not ask for",
            id="envelope-names-a-measure-the-query-does-not-ask-for",
        ),
        pytest.param(
            _block(envelope=_envelope(scope="as_released", release="2026-01")),
            "records scope 'as_released' but its query asks for 'latest'",
            id="envelope-scope-disagrees-with-query",
        ),
        pytest.param(
            _block(
                envelope=_envelope(scope="as_released", release="2026-02"),
                document=_query(scope="as_released", release="2026-01"),
            ),
            "records release '2026-02' but its query asks for '2026-01'",
            id="envelope-release-disagrees-with-query",
        ),
        pytest.param(
            {
                "block_id": "limits",
                "type": "caveat",
                "title": "Limits",
                "envelope": _envelope(),
            },
            "is caveat prose and cannot carry a query or an envelope",
            id="prose-block-carrying-an-envelope",
        ),
        pytest.param(
            {
                "block_id": "method",
                "type": "methodology",
                "title": "Method",
                "document": _query(),
            },
            "is methodology prose and cannot carry a query or an envelope",
            id="prose-block-carrying-a-query",
        ),
        pytest.param(
            _block(
                document=_query(metric_code="NO:SUCH:METRIC"),
                envelope=_envelope(metric_codes=["NO:SUCH:METRIC"]),
            ),
            "block 'unemployment': metric_code 'NO:SUCH:METRIC' is not a published metric",
            id="query-the-live-contracts-refuse",
        ),
        pytest.param(
            _block(document=_query(filters={"stratum_id": "s1"})),
            "filters not supported for source 'FRED'",
            id="query-with-an-undeclared-filter",
        ),
    ],
)
def test_contradictions_are_refused_at_write_naming_the_block(
    accounts, monkeypatch, block: dict, fragment: str
) -> None:
    """Covers: API-070 — a contradiction never reaches storage."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "bad", "document": _packet(block)},
    )
    assert response.status_code == 422, response.text
    assert fragment in response.json()["detail"]
    assert storage.rows == []


def test_duplicate_block_ids_are_refused(accounts, monkeypatch) -> None:
    """Covers: API-070 — block identity is part of the composition."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "dup", "document": _packet(_block("twice"), _block("twice"))},
    )
    assert response.status_code == 422
    assert "appears more than once" in response.json()["detail"]


def test_unknown_block_type_and_stray_fields_never_reach_storage() -> None:
    """Covers: API-070 — the schema boundary refuses shapes the contract lacks."""
    with pytest.raises(ValueError):
        EvidencePacketDocument.model_validate(
            _packet({"block_id": "f", "type": "forecast"})
        )
    with pytest.raises(ValueError):
        EvidencePacketDocument.model_validate({**_packet(), "version": 2})
    with pytest.raises(ValueError):
        EvidencePacketDocument.model_validate({**_packet(), "schema_version": 2})


def test_retired_measure_is_reported_on_its_block_not_repaired(
    accounts, monkeypatch
) -> None:
    """Covers: API-070 — stale names the block and keeps the document."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={
            "name": "goes-stale",
            "document": _packet(_block("kept"), _block("retired")),
        },
    )
    packet_id = created.json()["packet_id"]
    assert created.json()["validation"]["valid"] is True

    def _retired_warehouse():
        yield _WarehouseSession({})

    app.dependency_overrides[get_db_session_dep] = _retired_warehouse
    stale = client.get(f"/api/v1/evidence-packets/{packet_id}", headers=_auth())
    assert stale.status_code == 200
    validation = stale.json()["validation"]
    assert validation["valid"] is False
    for state in validation["blocks"]:
        if state["block_id"] in {"kept", "retired"}:
            assert state["valid"] is False
            assert "not a published metric" in state["reason"]
            assert state["missing"] == [], "stale, not incomplete"
    blocks = stale.json()["document"]["blocks"]
    assert all(
        b["document"]["metric_code"] == "FRED:UNRATE"
        for b in blocks
        if b.get("document")
    )


def test_repeated_measures_are_resolved_once_per_request(accounts, monkeypatch) -> None:
    """Covers: API-070 — validation cost does not multiply with block count."""
    warehouse = _WarehouseSession()
    client = _client(_StorageSession(accounts), warehouse, monkeypatch=monkeypatch)
    blocks = [_block(f"b{i}") for i in range(12)]
    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "dozen", "document": _packet(*blocks)},
    )
    assert created.status_code == 201
    # One write-time check and one read-time check of the single distinct query.
    assert warehouse.lookups.count("FRED:UNRATE") == 2


def test_analytical_block_cap_is_enforced() -> None:
    """Covers: API-070 — the ADR's bound is a contract, not a hope."""
    warehouse = _WarehouseSession()
    document = EvidencePacketDocument.model_validate(
        _packet(*[_block(f"b{i}") for i in range(51)])
    )
    with pytest.raises(evidence_packet_service.PacketInvalid) as raised:
        evidence_packet_service.validate_packet(warehouse, document)
    assert "at most 50 analytical blocks" in raised.value.detail


# ---------------------------------------------------------------------------
# API-071 — concurrency, deletion, privacy, list without verdict
# ---------------------------------------------------------------------------


def test_update_requires_the_version_the_caller_read(accounts, monkeypatch) -> None:
    """Covers: API-071 — a stale update is refused with the current version."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "c", "document": _packet()},
    )
    packet_id = created.json()["packet_id"]
    first = client.put(
        f"/api/v1/evidence-packets/{packet_id}",
        headers=_auth(),
        json={"name": "c", "document": _packet(title="second"), "expected_version": 1},
    )
    assert first.status_code == 200 and first.json()["version"] == 2
    stale = client.put(
        f"/api/v1/evidence-packets/{packet_id}",
        headers=_auth(),
        json={"name": "c", "document": _packet(title="third"), "expected_version": 1},
    )
    assert stale.status_code == 409
    assert "current version 2" in stale.json()["detail"]
    unchanged = client.get(f"/api/v1/evidence-packets/{packet_id}", headers=_auth())
    assert unchanged.json()["document"]["title"] == "second"

    taken = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "c", "document": _packet()},
    )
    assert (
        taken.status_code == 409
        and "a packet named 'c' exists" in taken.json()["detail"]
    )


def test_delete_is_immediate_and_a_second_delete_is_404(accounts, monkeypatch) -> None:
    """Covers: API-071 — a hard delete, effective at once."""
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    packet_id = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "t", "document": _packet()},
    ).json()["packet_id"]
    assert (
        client.delete(
            f"/api/v1/evidence-packets/{packet_id}", headers=_auth()
        ).status_code
        == 204
    )
    assert storage.rows == []
    assert (
        client.delete(
            f"/api/v1/evidence-packets/{packet_id}", headers=_auth()
        ).status_code
        == 404
    )


def test_list_carries_size_but_never_a_verdict(accounts, monkeypatch) -> None:
    """Covers: API-071 — the list is for choosing, not for judging."""
    warehouse = _WarehouseSession()
    client = _client(_StorageSession(accounts), warehouse, monkeypatch=monkeypatch)
    empty = {"block_id": "condition", "type": "analysis", "title": "Condition"}
    client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={
            "name": "b-second",
            "document": _packet(
                {"block_id": "summary", "type": "text", "title": "Summary"},
                _block(),
                empty,
            ),
        },
    )
    client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "a-first", "document": _packet()},
    )
    lookups_before = len(warehouse.lookups)

    listing = client.get("/api/v1/evidence-packets", headers=_auth())
    assert listing.status_code == 200
    payload = listing.json()
    assert [item["name"] for item in payload["items"]] == ["a-first", "b-second"]
    second = payload["items"][1]
    assert second["block_count"] == 3 and second["analytical_block_count"] == 2
    assert "validation" not in second and "valid" not in second
    assert len(warehouse.lookups) == lookups_before, (
        "listing performed no glossary lookup"
    )


def test_user_content_is_never_publicly_cacheable(accounts, monkeypatch) -> None:
    """Covers: API-071 — private no-store, outside the cacheable prefixes."""
    client = _client(_StorageSession(accounts), monkeypatch=monkeypatch)
    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "p", "document": _packet()},
    )
    detail = client.get(
        f"/api/v1/evidence-packets/{created.json()['packet_id']}", headers=_auth()
    )
    listing = client.get("/api/v1/evidence-packets", headers=_auth())
    for response in (created, detail, listing):
        assert response.headers["cache-control"] == "private, no-store"
        assert "x-cache" not in response.headers
    for path in app.openapi()["paths"]:
        if "evidence-packets" in path:
            assert not PUBLIC_CACHE_TARGETS.covers(path), path


@pytest.mark.parametrize(
    ("block", "fragment"),
    [
        pytest.param(
            _block(
                envelope=_envelope(geo_id="state:06"),
                document=_query(filters={"geo_id": "state:55"}),
            ),
            "records geography 'state:06'",
            id="envelope-names-another-geography",
        ),
        pytest.param(
            _block(
                envelope=_envelope(geo_level="COUNTY"),
                document=_query(filters={"geo_level": "STATE"}),
            ),
            "records geography grain 'COUNTY'",
            id="envelope-names-another-grain",
        ),
    ],
)
def test_a_block_cannot_name_one_geography_and_query_another(
    accounts, monkeypatch, block: dict, fragment: str
) -> None:
    """Covers: API-099 — the contradiction table reaches the geography.

    The module's own rule is that a block whose envelope names a measure its
    query does not ask for "would display one measure's name over another
    measure's numbers". `geo_id` and `geo_level` are the same kind of field --
    request parameters the block's own `filters` carries, not observations
    about what a source published -- and they were not cross-checked, so a
    packet could store one geography's name over another geography's numbers.
    """
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={"name": "bad", "document": _packet(block)},
    )
    assert response.status_code == 422, response.text
    assert fragment in response.json()["detail"]
    assert storage.rows == []


def test_a_grain_alias_and_its_vocabulary_word_are_one_geography(
    accounts, monkeypatch
) -> None:
    """Covers: API-099 — the comparison is the one `normalize_geo_level` defines.

    API-092 promised the words the vocabulary replaced keep answering, so an
    envelope composed when the catalog published `NATION` and a query asking
    for `NATIONAL` name the same grain. Refusing that pair would make a
    correct packet unstorable.
    """
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={
            "name": "aliased",
            "document": _packet(
                _block(
                    envelope=_envelope(geo_level="NATION"),
                    document=_query(filters={"geo_level": "NATIONAL"}),
                )
            ),
        },
    )
    assert response.status_code == 201, response.text


def test_a_geography_recorded_on_one_side_only_is_incompleteness(
    accounts, monkeypatch
) -> None:
    """Covers: API-099 — refuse contradictions, report incompleteness.

    A block still being composed records what it has. Only two different
    answers to the same question are a contradiction, which is how the scope
    and release checks already read.
    """
    storage = _StorageSession(accounts)
    client = _client(storage, monkeypatch=monkeypatch)
    response = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(),
        json={
            "name": "partial",
            "document": _packet(
                _block(
                    envelope=_envelope(geo_id="state:06", geo_level=""),
                    document=_query(filters={}),
                )
            ),
        },
    )
    assert response.status_code == 201, response.text
