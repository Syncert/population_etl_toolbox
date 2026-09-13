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
#: The frontend handoff names routes too, and this module's own docstring has
#: always claimed to check them. It did not: the extraction below read the
#: consumer guide alone, so a route the handoff named and the API had retired
#: would have failed nothing (API-065).
HANDOFF = (
    Path(__file__).resolve().parents[3] / "docs/reference/WEB_FIRST_WAVE_HANDOFF.md"
)


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
    """Every ``/api/v1/...`` path the published documents name.

    Both of them: the consumer guide a client builds against, and the
    frontend handoff that tells a later plan what it may assume.
    """
    text = GUIDE.read_text(encoding="utf-8") + HANDOFF.read_text(encoding="utf-8")
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


def _paged_paths() -> set[str]:
    """Every served read that takes ``limit`` and ``offset``.

    Deliberately not filtered to the observation routes. API-095 fixed the
    promise for those and its filter was ``"observations" not in path``, so
    the catalog's two paged reads, `/comparison`, `/usda-nass/series` and
    both private stores were outside the sweep entirely (API-106).
    """
    document = app.openapi()
    paged: set[str] = set()
    for path, operations in document["paths"].items():
        get = operations.get("get")
        if get is None:
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


def test_every_paged_read_declares_what_orders_it() -> None:
    """Covers: API-095, API-106 — a read that pages says what makes its pages
    stable.

    The guide promises every paged read a total order, "so two consecutive
    pages can neither repeat a row nor skip one", and then listed four reads.
    API-095 found `/observations` missing -- the resource the same guide tells
    clients to prefer -- and `/observations/releases`, "whose order was total
    only by coincidence of the registry".

    The guard it left behind swept only paths containing `observations`, so
    six of the twenty paged reads were outside it: the catalog's two,
    `/comparison`, `/usda-nass/series`, and both private stores. Every one of
    those orders is total today, and nothing said so to a client or would
    have failed if one were narrowed -- which is the same sentence API-095
    wrote about the releases route (API-106).

    Derived from the served document and the serving registry, so a route that
    grows `limit` and `offset` without a row fails instead of quietly leaving
    a client to guess.
    """
    documented = _ordering_table_reads()
    assert documented, "the guide's ordering table has no rows; parsing broke"

    paged = _paged_paths()
    # A floor, so a change to the served document that stopped matching
    # cannot make this pass by sweeping nothing.
    assert len(paged) >= 20, f"only {len(paged)} paged reads found; parsing broke"

    missing = sorted(
        {
            _guide_spelling(path)
            for path in paged
            if _guide_spelling(path) not in documented
        }
    )
    assert not missing, (
        f"these paged reads name no ordering in the consumer guide: {missing}"
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


#: The release-selection table's claims, read from the guide rather than
#: written here: route, the parameter it names, and the envelope field.
_RELEASE_TABLE_ROW = re.compile(
    r"^\|\s*`(?P<route>/[^`]+)`\s*\|(?P<asks>[^|]*)\|(?P<default>[^|]*)\|"
    r"(?P<envelope>[^|]*)\|\s*$",
    re.MULTILINE,
)


def _release_table_rows() -> list[dict[str, str]]:
    """The rows of the guide's release-selection table."""
    text = GUIDE.read_text(encoding="utf-8")
    section = text.split("### Which release you get", 1)
    if len(section) < 2:
        return []
    body = section[1].split("###", 1)[0]
    return [match.groupdict() for match in _RELEASE_TABLE_ROW.finditer(body)]


def test_the_guide_names_each_routes_release_default() -> None:
    """Covers: API-110 — the release defaults differ, and the guide says so.

    Three resources answer the same question three ways: three envelope field
    names, three vocabularies, and opposite defaults -- `/observations` and
    `/cdc/observations` answer the newest release while
    `/usda-nass/observations` answers every published one. All of it is
    pinned behaviourally at three tiers, and the guide's entire word on the
    two source routes was that they "remain for source-specific
    exploration".

    Every claim in the table is checked against the served document and the
    reviewed snapshot, so the table cannot drift into describing a contract
    the API does not serve.
    """
    import json

    rows = _release_table_rows()
    assert len(rows) >= 3, f"the release table parsed {len(rows)} rows"

    document = app.openapi()
    snapshot = json.loads(
        (
            Path(__file__).resolve().parents[3]
            / "tests/fixtures/api/openapi_contract.json"
        ).read_text(encoding="utf-8")
    )

    for row in rows:
        path = f"/api/v1{row['route']}"
        operation = document["paths"][path]["get"]
        declared = {
            parameter["name"]: parameter
            for parameter in operation.get("parameters") or []
        }

        # Every parameter the row names in backticks is declared on the route.
        named = set(re.findall(r"`(\w+)=", row["asks"]))
        missing = sorted(named - set(declared))
        assert not missing, f"{path} does not declare {missing}"

        # Where the row says there is no history parameter, there is none.
        if "no history parameter" in row["asks"]:
            assert "scope" not in declared and "latest" not in declared, (
                f"{path} declares a history parameter the guide says it lacks"
            )

        # A default the row states in bold or plain text must match the
        # served default where the parameter carries one.
        if "latest=true" in row["asks"]:
            schema = declared["latest"].get("schema", {})
            assert schema.get("default") is False, (
                f"{path} declares `latest` default {schema.get('default')!r}; "
                "the guide says an unqualified read answers every release"
            )
        if "scope=latest" in row["asks"]:
            schema = declared["scope"].get("schema", {})
            assert "latest" in json.dumps(schema), schema

        # The envelope field the row names exists on the route's response.
        field = re.search(r"`(\w+)`", row["envelope"]).group(1)
        response_schema = operation["responses"]["200"]["content"]["application/json"][
            "schema"
        ]
        schema_name = str(response_schema["$ref"]).rsplit("/", 1)[-1]
        assert schema_name in snapshot["schemas"], schema_name
        assert field in snapshot["schemas"][schema_name]["properties"], (
            f"{path} answers {schema_name}, which publishes no `{field}`"
        )


def _snapshot() -> dict:
    """The reviewed OpenAPI snapshot the guide's preamble says pins it."""
    import json

    return json.loads(
        (
            Path(__file__).resolve().parents[3]
            / "tests/fixtures/api/openapi_contract.json"
        ).read_text(encoding="utf-8")
    )


def test_the_guide_describes_both_shapes_of_a_refused_request() -> None:
    """Covers: API-111 — `422` answers two bodies, and the guide says so.

    The guide's preamble stakes everything in it on the reviewed snapshot,
    and its Errors section then said "Every error body is
    `{"detail": "..."}`" -- which the snapshot contradicts for the one status
    the same section calls a request the API can explain: every read declares
    `422` as `HTTPValidationError`, whose `detail` is an array. Read off the
    running application, the snapshot is right.

    Both halves are read here rather than restated, so the prose fails if
    either shape moves in either direction.
    """
    from apps.api.dependencies import get_db_session_dep

    # The refusals below happen before the session is touched; the override
    # exists because FastAPI resolves dependencies before the endpoint runs,
    # and this tier has no database.
    app.dependency_overrides[get_db_session_dep] = lambda: object()
    try:
        client = TestClient(app)
        framework = client.get("/api/v1/catalog/metrics", params={"limit": 5000})
        explained = client.get(
            "/api/v1/observations",
            params={"metric_code": "ANY:thing", "year_from": 2020, "year_to": 2000},
        )
    finally:
        app.dependency_overrides.pop(get_db_session_dep, None)

    # Refused before the endpoint ran: the declared array.
    assert framework.status_code == 422, framework.text
    entries = framework.json()["detail"]
    assert isinstance(entries, list) and entries, framework.text

    snapshot = _snapshot()
    required = set(snapshot["schemas"]["ValidationError"]["required"])
    assert required == {"loc", "msg", "type"}, required
    for entry in entries:
        assert required <= set(entry), entry
        assert isinstance(entry["loc"], list) and entry["loc"], entry
    assert entries[0]["loc"][:2] == ["query", "limit"], entries

    # Refused by the API itself: the sentence every other status answers.
    assert explained.status_code == 422, explained.text
    assert isinstance(explained.json()["detail"], str), explained.text

    # And the contract declares exactly one 422 shape, so the array is not an
    # accident of this one route.
    declared = {
        media
        for operation in snapshot["operations"].values()
        for status, media in (operation.get("responses") or {}).items()
        if status == "422"
    }
    assert declared == {"application/json:HTTPValidationError"}, sorted(declared)
    assert snapshot["schemas"]["HTTPValidationError"]["properties"]["detail"] == (
        "array<ValidationError>"
    )

    text = GUIDE.read_text(encoding="utf-8")
    assert 'Every error body is `{"detail": "..."}`' not in text, (
        "the blanket promise the snapshot contradicts is still in the guide"
    )
    errors = text.split("## Errors", 1)[1].split("\n## ", 1)[0]
    assert '{"detail": "<sentence>"}' in errors, (
        "the guide does not show the shape an API-explained refusal answers"
    )
    for key in sorted(required):
        assert f'"{key}"' in errors, (
            f"the guide's Errors section does not name the `{key}` a client "
            "must read on a validation refusal"
        )


#: Every name a served parameter gives the geography grain. `geo_level` is
#: the neutral one; the two source-scoped routes keep their providers' own
#: spellings, which is what let them drift (API-116).
_GRAIN_PARAMETERS = ("geo_level", "geo_type", "agg_level_desc")


class _EmptyWarehouse:
    """Answers every read with nothing, so only validation can refuse."""

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        class _Result:
            def mappings(self):
                return self

            def all(self):
                return []

            def first(self):
                return None

            def scalar(self):
                return 0

        return _Result()


def test_every_parameter_that_carries_a_grain_takes_the_vocabulary() -> None:
    """Covers: API-116 — one vocabulary, whatever a route calls the parameter.

    The guide promises "a grain read from the catalog can be sent straight
    back … The filter is case-insensitive, and it accepts `NATION` as an
    alias for `NATIONAL`". API-092 and API-094 swept `geo_level`. Two routes
    take the grain under another name -- CDC's `geo_type`, USDA NASS's
    `agg_level_desc` -- and were not swept, so the catalog's own `COUNTY`
    was a 422 on one and `NATION` on both.

    Read off the served document: every route declaring any of the three
    names is sent the lowercase alias, and none may refuse it *for that
    parameter*. A 404 for an unpublished metric is fine; a 422 naming the
    grain is the defect.
    """
    from apps.api.dependencies import get_db_session_dep

    document = app.openapi()
    served: dict[str, list[str]] = {}
    for path, operations in document["paths"].items():
        get = operations.get("get")
        if get is None:
            continue
        names = {
            parameter["name"]
            for parameter in get.get("parameters") or []
            if parameter.get("in") == "query"
        }
        required = tuple(
            parameter["name"]
            for parameter in get.get("parameters") or []
            if parameter.get("in") == "query" and parameter.get("required")
        )
        for name in sorted(names & set(_GRAIN_PARAMETERS)):
            served.setdefault(name, []).append((path, required))

    assert set(served) == set(_GRAIN_PARAMETERS), (
        f"the served contract declares {sorted(served)}; a grain parameter "
        "renamed or retired must be reflected here"
    )

    app.dependency_overrides[get_db_session_dep] = lambda: _EmptyWarehouse()
    try:
        client = TestClient(app)
        for parameter, entries in sorted(served.items()):
            for path, required in sorted(entries):
                # Only what the route itself declares: a NASS route refuses
                # an unknown parameter by naming the ones it accepts, and
                # `agg_level_desc` appears in that list.
                params = {parameter: "nation"}
                for name in required:
                    params.setdefault(name, "FRED:UNRATE")
                response = client.get(path, params=params)
                if response.status_code != 422:
                    continue
                detail = str(response.json().get("detail"))
                assert parameter not in detail, (
                    f"{path} refused the alias 'nation' for {parameter}: {detail}"
                )
    finally:
        app.dependency_overrides.pop(get_db_session_dep, None)
