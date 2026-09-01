"""The published consumer guide describes the contract the API actually serves.

Covers: API-065 — every route the frontend handoff names is served, the
        legacy routes it calls retiring carry their retirement signal with the
        published sunset date, and the guide's per-source claims match the
        capability resource rather than a prose copy of it.

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
from apps.api.versioning import LEGACY_SUNSET_DATE, legacy_path_for

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


def test_guide_names_the_published_sunset_date() -> None:
    """Covers: API-065 — the published window matches the served header."""
    text = GUIDE.read_text(encoding="utf-8")
    assert LEGACY_SUNSET_DATE in text, (
        "the guide must publish the same sunset date the API sends"
    )


def test_legacy_aliases_still_carry_their_retirement_signal() -> None:
    """Covers: API-065 — a consumer learns to migrate from an ordinary response."""
    client = TestClient(app)
    response = client.get(legacy_path_for("/api/v1/catalog/capabilities"))

    assert response.status_code == 200
    assert response.headers["deprecation"] == "true"
    assert response.headers["sunset"] == LEGACY_SUNSET_DATE
    assert response.headers["link"] == (
        '</api/v1/catalog/capabilities>; rel="successor-version"'
    )

    versioned = client.get("/api/v1/catalog/capabilities")
    assert versioned.status_code == 200
    assert "deprecation" not in versioned.headers
    assert versioned.json() == response.json(), (
        "the alias and its successor answer the same contract"
    )


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
