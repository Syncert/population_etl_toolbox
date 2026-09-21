"""API unit tests: the public OpenAPI contract is pinned to a reviewed snapshot.

Covers: API-031 (public contract snapshot), API-032 (every public resource is
        served under the current version and nowhere else), API-033 (the
        unversioned deployment probes stay outside the version policy).

API-002 restructures routers, schemas, and services. That refactor is only safe
if a consumer-visible change cannot pass unnoticed, so the contract itself is
the characterization test: every operation, parameter bound, and response schema
is frozen in ``tests/fixtures/api/openapi_contract.json``.

The snapshot is reviewed evidence, not a cache. Regenerate it deliberately with
``python -m tests.support.regenerate_openapi_contract`` and read the diff; a
snapshot updated to make a red test green is exactly the failure this guards.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.api.main import PUBLIC_ROUTERS, app
from apps.api.ratelimit import EXEMPT_PATHS
from apps.api.routers import health
from apps.api.versioning import CURRENT_VERSION, UNVERSIONED_PATHS, VERSIONED_ROOT
from tests.support.openapi_contract import contract_digest, describe_difference

SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[2] / "fixtures" / "api" / "openapi_contract.json"
)


def _reviewed_digest() -> dict:
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


@pytest.mark.unit
@pytest.mark.api
def test_public_contract_matches_the_reviewed_snapshot() -> None:
    """Covers: API-031 — no operation, bound, or schema field drifts unreviewed."""
    current = contract_digest(app.openapi())
    reviewed = _reviewed_digest()
    assert current == reviewed, (
        "The public API contract changed:\n"
        + describe_difference(reviewed, current)
        + "\n\nIf the change is intended, regenerate the snapshot and review the "
        "diff as part of the change."
    )


@pytest.mark.unit
@pytest.mark.api
def test_every_public_resource_is_served_only_under_the_current_version() -> None:
    """Covers: API-032 — one public surface, no unversioned aliases left.

    API-008 retired the ``/api`` aliases. Every ``/api`` path must now name a
    supported version; an unversioned data route reappearing would be a second
    surface that could drift from the promised one.
    """
    digest = contract_digest(app.openapi())
    served = {key.split(" ", 1)[1] for key in digest["operations"]}

    api_paths = {path for path in served if path.startswith("/api/")}
    assert api_paths, "no API routes are served"

    unversioned = sorted(
        path for path in api_paths if not path.startswith(f"/api/{CURRENT_VERSION}/")
    )
    assert not unversioned, f"unversioned API routes are served again: {unversioned}"


@pytest.mark.unit
@pytest.mark.api
def test_deployment_probes_stay_outside_the_version_policy() -> None:
    """Covers: API-033 — the container probes are not versioned resources."""
    client = TestClient(app)

    for path in sorted(UNVERSIONED_PATHS):
        probe = client.get(path)
        assert probe.status_code in {200, 503}, path
        assert "deprecation" not in probe.headers, path
        assert "sunset" not in probe.headers, path

    assert client.get("/health").json()["status"] == "ok"

    # And the declaration is the router's, both ways. Asserting only that
    # every listed path is served lets a probe added to the router go
    # unlisted, and `UNVERSIONED_PATHS` is what tells the rest of the
    # application which paths carry no data contract.
    from apps.api.routers import health

    assert UNVERSIONED_PATHS == {
        str(route.path) for route in health.probe_router.routes
    }


@pytest.mark.unit
@pytest.mark.api
def test_no_response_carries_a_retirement_signal() -> None:
    """Covers: API-033 — nothing is deprecated, so nothing announces it."""
    client = TestClient(app)

    for path in (
        f"/api/{CURRENT_VERSION}/health",
        f"/api/{CURRENT_VERSION}/catalog/capabilities",
    ):
        response = client.get(path)
        assert response.status_code == 200, path
        assert "deprecation" not in response.headers, path
        assert "sunset" not in response.headers, path
        assert "successor-version" not in response.headers.get("link", ""), path


@pytest.mark.unit
@pytest.mark.api
def test_retired_aliases_are_not_served() -> None:
    """Covers: API-032 — a legacy path is a 404, not a quiet second surface."""
    client = TestClient(app)

    for legacy in ("/api/health", "/api/catalog/metrics", "/api/observations"):
        assert client.get(legacy).status_code == 404, legacy


# ---------------------------------------------------------------------------
# The declared failures are the failures the application raises (API-121)
# ---------------------------------------------------------------------------

ROUTERS_DIRECTORY = Path(__file__).resolve().parents[3] / "apps" / "api" / "routers"


def _raised_statuses(source: str) -> set[int]:
    """Every HTTP status a module hands to ``HTTPException``.

    Read from the source rather than from a list beside it: a refusal added to
    a router is a failure the contract has to declare, and a list would only
    be right until the next one.
    """
    raised: set[int] = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        name = getattr(callee, "id", None) or getattr(callee, "attr", None)
        if name != "HTTPException":
            continue
        candidates = [
            keyword.value for keyword in node.keywords if keyword.arg == "status_code"
        ]
        candidates.extend(node.args[:1])
        for candidate in candidates:
            if isinstance(candidate, ast.Constant) and isinstance(candidate.value, int):
                raised.add(candidate.value)
    return raised


def _operations_by_module() -> dict[str, list[dict]]:
    """The served operations each router module contributes.

    Read from the routers the application factory mounts, paired with the
    served document, rather than from ``app.routes``: this FastAPI version
    wraps an included router in an opaque object, so walking the application
    finds only its own documentation routes.
    """
    document = app.openapi()
    mounted = [(router, VERSIONED_ROOT) for router in PUBLIC_ROUTERS]
    mounted.append((health.probe_router, ""))
    grouped: dict[str, list[dict]] = {}
    for router, prefix in mounted:
        for route in router.routes:
            endpoint = getattr(route, "endpoint", None)
            served = document["paths"].get(f"{prefix}{getattr(route, 'path', '')}")
            if endpoint is None or served is None:
                continue
            module = getattr(endpoint, "__module__", "")
            for method in getattr(route, "methods", set()) or set():
                operation = served.get(method.lower())
                if operation is not None:
                    grouped.setdefault(module, []).append(operation)
    return grouped


def _declared_statuses(operation: dict) -> set[int]:
    return {int(status) for status in operation.get("responses", {})}


@pytest.mark.unit
@pytest.mark.api
def test_every_status_a_router_raises_is_declared_by_it() -> None:
    """Covers: API-121 — a refusal the code can answer is in the contract.

    The published document declared exactly 200, 201, 204 and 422 across all
    39 operations, while the routers raise 401, 404, 409 and 503 by hand and
    the middleware answers 413 and 429. A client generated from
    `/openapi.json` had no branch for any of them.
    """
    grouped = _operations_by_module()
    for path in sorted(ROUTERS_DIRECTORY.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        module = f"apps.api.routers.{path.stem}"
        raised = _raised_statuses(path.read_text(encoding="utf-8"))
        if not raised:
            continue
        operations = grouped.get(module)
        assert operations, f"{module} raises {sorted(raised)} and serves nothing"
        declared: set[int] = set()
        for operation in operations:
            declared |= _declared_statuses(operation)
        # Per module, not per operation: a module's 404 helper belongs to the
        # routes that resolve an identifier, and the routes that do not should
        # not claim it.
        missing = sorted(raised - declared)
        assert not missing, f"{module} can answer {missing} and declares neither"


@pytest.mark.unit
@pytest.mark.api
def test_shared_failures_are_declared_where_they_apply() -> None:
    """Covers: API-121 — the middleware's and dependencies' failures, too.

    These are raised nowhere in a router: the body limit and the rate limiter
    are middleware, the sanitized 503 is a dependency, and the 401 is the
    account dependency. Each is declared for exactly the routes it can reach,
    so the contract neither hides a failure nor invents one.
    """
    document = app.openapi()
    # "Private" here means "can answer 401", which is what the assertion below
    # actually checks. The identity routes qualify on both readings: the
    # account routes require a credential, and the sign-in pair refuses with
    # the same 401 rather than explaining which check a caller tripped.
    private_prefixes = (
        "/api/v1/analysis-configurations",
        "/api/v1/evidence-packets",
        "/api/v1/auth",
        "/api/v1/account",
    )
    for path, item in document["paths"].items():
        for method, operation in item.items():
            declared = _declared_statuses(operation)
            where = f"{method.upper()} {path}"
            # The strict-parameter dependency is applied application-wide.
            assert 422 in declared, f"{where} declares no 422"
            private = path.startswith(private_prefixes)
            assert (401 in declared) is private, f"{where}: 401 vs authentication"
            # Which paths the limiter exempts is the limiter's own answer,
            # read from it rather than restated here. This was a prefix test
            # -- everything under `/api/v1/health` was assumed unmetered --
            # which was true until the content report (API-137) put a
            # warehouse read under that prefix on a router the limiter does
            # not exempt. A prefix cannot see that, and would have insisted
            # the metered route declare no 429.
            rate_limited = path not in EXEMPT_PATHS
            assert (429 in declared) is rate_limited, f"{where}: 429 vs metering"
            # Only a route that parses a body can answer the body limit.
            accepts_body = method.upper() in {"POST", "PUT", "PATCH"}
            assert (413 in declared) is accepts_body, f"{where}: 413 vs a body"


@pytest.mark.unit
@pytest.mark.api
def test_every_declared_failure_carries_the_body_it_answers() -> None:
    """Covers: API-121 — the declared error body is the one the API sends.

    Every hand-raised refusal sends `{"detail": "<sentence>"}`. A 422 has two
    bodies and both are declared, because which one a client gets says who
    refused the request.
    """
    document = app.openapi()
    string_form = {"$ref": "#/components/schemas/ErrorDetail"}
    for path, item in document["paths"].items():
        for method, operation in item.items():
            for status, response in operation["responses"].items():
                if int(status) < 400:
                    continue
                where = f"{method.upper()} {path} {status}"
                assert response.get("description"), f"{where} says nothing"
                schema = response["content"]["application/json"]["schema"]
                if int(status) == 422:
                    assert string_form in schema.get("anyOf", []), where
                    assert {
                        "$ref": "#/components/schemas/HTTPValidationError"
                    } in schema["anyOf"], where
                elif path == "/health/ready":
                    # The readiness probe's own report, or the sanitized
                    # refusal when no session could be opened.
                    assert string_form in schema.get("anyOf", []), where
                else:
                    assert schema == string_form, where

    schemas = document["components"]["schemas"]
    assert schemas["ErrorDetail"]["properties"]["detail"]["type"] == "string"
    assert schemas["HTTPValidationError"]["properties"]["detail"]["type"] == "array"
