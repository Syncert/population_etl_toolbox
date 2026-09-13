"""The published consumer guide describes the contract the API actually serves.

Covers: API-065 — every route the frontend handoff names is served, the
        guide describes the single versioned surface the API actually has,
        and its per-source claims match the capability registry rather than a
        prose copy of it.

A handoff document that drifts from the application is worse than none: a
consumer builds against it. This pins the load-bearing claims.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.versioning import CURRENT_VERSION

pytestmark = [pytest.mark.unit, pytest.mark.api]

GUIDE = Path(__file__).resolve().parents[3] / "docs/reference/API_CONSUMER_GUIDE.md"


def _expand(path: str) -> set[str]:
    """Expand ``{a,b}`` shorthand into one concrete path per option."""
    brace = re.search(r"\{([^{}]*,[^{}]*)\}", path)
    if brace is None:
        return {path}
    expanded: set[str] = set()
    for option in brace.group(1).split(","):
        candidate = path[: brace.start()] + option.strip() + path[brace.end() :]
        expanded |= _expand(candidate)
    return expanded


def _documented_paths() -> set[str]:
    """Every ``/api/v1/...`` path the guide names, expanded from its braces."""
    text = GUIDE.read_text(encoding="utf-8")
    found: set[str] = set()
    for raw in re.findall(r"/api/v1/[A-Za-z0-9/_{},.-]*", text):
        path = raw.rstrip(".,;:)`")
        if path.endswith("...") or path == "/api/v1/":
            continue
        found |= _expand(path)
    return {path for path in found if path.count("/") >= 3}


def test_every_documented_route_is_actually_served() -> None:
    """Covers: API-065 — the handoff cannot name a route that does not exist."""
    served = set(app.openapi()["paths"])
    documented = _documented_paths()
    assert documented, "the guide names no routes; the extraction is broken"

    missing = sorted(path for path in documented if path not in served)
    assert not missing, f"the consumer guide names unserved routes: {missing}"


def test_guide_describes_one_versioned_surface() -> None:
    """Covers: API-065 — the guide cannot promise a surface that is gone."""
    text = GUIDE.read_text(encoding="utf-8")

    assert f"/api/{CURRENT_VERSION}" in text
    for retired in ("Deprecation:", "Sunset:", "successor-version"):
        assert retired not in text, (
            f"the guide still documents the retired alias signal {retired!r}"
        )


def test_retired_aliases_are_not_served() -> None:
    """Covers: API-065 — an unversioned path is a 404, not a second surface."""
    client = TestClient(app)
    for legacy in ("/api/health", "/api/catalog/capabilities", "/api/observations"):
        assert client.get(legacy).status_code == 404, legacy

    versioned = client.get(f"/api/{CURRENT_VERSION}/catalog/capabilities")
    assert versioned.status_code == 200
    assert "deprecation" not in versioned.headers


def test_guide_analysis_claims_match_the_capability_registry() -> None:
    """Covers: API-065 — the declined sources in prose are the declined sources."""
    text = GUIDE.read_text(encoding="utf-8")
    declined = {
        code
        for code, dispatch in OBSERVATION_DISPATCH.items()
        if not dispatch.analysis_ready
    }
    assert declined == {"CDC", "USDA_NASS", "FBI_UCR"}

    analysis_section = text.split("## Analysis", 1)[1].split("##", 1)[0]
    for name in ("CDC", "USDA NASS", "FBI UCR"):
        assert name in analysis_section, (
            f"{name} is declined by the registry but the guide does not say so"
        )
    for name in ("Census ACS", "BLS", "FRED", "Census PEP"):
        assert name in analysis_section


def test_guide_documents_every_neutral_observation_filter() -> None:
    """Covers: API-065 — the documented filter union is the accepted one."""
    text = GUIDE.read_text(encoding="utf-8")
    declared = {
        name
        for dispatch in OBSERVATION_DISPATCH.values()
        for name in dispatch.supported_filters()
    }
    undocumented = sorted(name for name in declared if f"`{name}`" not in text)
    assert not undocumented, f"the guide omits accepted neutral filters: {undocumented}"


def _paged_observation_paths() -> set[str]:
    """Every served observation read that takes ``limit`` and ``offset``."""
    document = app.openapi()
    paged: set[str] = set()
    for path, operations in document["paths"].items():
        get = operations.get("get")
        if get is None or "observations" not in path:
            continue
        names = {
            parameter["name"]
            for parameter in get.get("parameters") or []
            if parameter.get("in") == "query"
        }
        if {"limit", "offset"} <= names:
            paged.add(path)
    return paged


def _guide_spelling(path: str) -> str:
    """One served path as the guide's ordering table spells it.

    The table writes the source-scoped pair once, as `/{source}/...`, because
    every serving contract declares the same order shape. The segments come
    from the registry, so a contract added later is folded the same way
    instead of looking like a missing row.
    """
    from apps.api.registry import SERVING_CONTRACTS

    remainder = path[len("/api/v1") :]
    segment = remainder.split("/")[1] if remainder.count("/") > 1 else ""
    if segment in SERVING_CONTRACTS:
        return "/{source}" + remainder[len(f"/{segment}") :]
    return remainder


def _ordering_table_reads() -> set[str]:
    """The reads the guide's ordering table names, from its first column."""
    text = GUIDE.read_text(encoding="utf-8")
    return set(re.findall(r"^\|\s*`(/[^`]+)`\s*\|", text, flags=re.MULTILINE))


def test_every_paged_observation_read_declares_what_orders_it() -> None:
    """Covers: API-095 — a read that pages says what makes its pages stable.

    The guide promises every paged read a total order, "so two consecutive
    pages can neither repeat a row nor skip one", and then lists four reads.
    It omitted `/observations` -- the resource the same guide tells clients to
    prefer -- and `/observations/releases`, whose order was total only by
    coincidence of the registry until API-095.

    Derived from the served document and the serving registry, so a route that
    grows `limit` and `offset` without a row here fails instead of quietly
    leaving a client to guess.
    """
    documented = _ordering_table_reads()
    assert documented, "the guide's ordering table has no rows; parsing broke"

    missing = sorted(
        {
            _guide_spelling(path)
            for path in _paged_observation_paths()
            if _guide_spelling(path) not in documented
        }
    )
    assert not missing, (
        "these paged observation reads name no ordering in the consumer "
        f"guide: {missing}"
    )


def _envelope_qualifier_schemas() -> dict[str, dict]:
    """The objects the neutral observation envelope nests, from the contract.

    Derived rather than named: whatever `NeutralObservation`'s own properties
    reference is what qualifies a served value, so a qualifier object added
    later is covered without an edit here.
    """
    import json
    import re

    snapshot = json.loads(
        (
            Path(__file__).resolve().parents[3]
            / "tests/fixtures/api/openapi_contract.json"
        ).read_text(encoding="utf-8")
    )
    schemas = snapshot["schemas"]
    observation = schemas["NeutralObservation"]["properties"]
    referenced = {
        word
        for value in observation.values()
        for word in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", str(value))
        if word in schemas
    }
    return {name: schemas[name] for name in sorted(referenced)}


def test_the_guide_names_every_field_that_qualifies_a_value() -> None:
    """Covers: API-102 — "reading a row honestly" needs the field names.

    The envelope nests exactly the objects that say what a source published
    about a number, and the guide described them in prose: "margins of error,
    confidence bounds, or the CV trio". A consumer cannot code against the CV
    trio, and `cv_symbol` is the flag USDA NASS publishes to say an estimate
    is unreliable -- a reader who does not know it exists reads the estimate
    as usable. WEB-053 found the client dropping the same five fields.
    """
    guide = GUIDE.read_text(encoding="utf-8")
    qualifiers = _envelope_qualifier_schemas()
    assert qualifiers, "the envelope nests no objects; the extraction is broken"

    missing = {
        name: sorted(field for field in schema["properties"] if field not in guide)
        for name, schema in qualifiers.items()
    }
    unnamed = {name: fields for name, fields in missing.items() if fields}
    assert not unnamed, (
        f"the consumer guide does not name these published qualifier fields: {unnamed}"
    )
