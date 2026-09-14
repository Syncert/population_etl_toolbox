"""One vocabulary, declared in two languages.

`apps/api/registry.py` declares the five published grains and the two words
the vocabulary replaced; `apps/web/lib/urlState.ts` declares the same two
things again, because a browser cannot import a Python module. Nothing
compared them, and both halves of that duplication have already gone wrong on
their own: WEB-038 dropped `PLACE` and `AGENCY` from the web list, so a shared
link to a Census PEP or FBI UCR view opened on a grain the measure does not
publish, and WEB-076 found the web list carrying no aliases at all while the
API accepted them and ADR-0002 promised they keep answering.

A grain added to the vocabulary -- or an alias added when a published word is
replaced again -- has to reach both declarations, and this is what notices
when it reaches only one. The observation scopes are the second such pair and
are read the same way. Read from the TypeScript source rather than from a
generated artifact, so it holds whether or not the web application has been
built; the API side of the scopes is read from the served document, which is
the declaration a client actually sees.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from apps.api.registry import GEO_GRAIN_ALIASES, GEO_GRAINS

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
URL_STATE = ROOT / "apps/web/lib/urlState.ts"

_LEVELS = re.compile(r"export const GEO_LEVELS = \[(?P<body>[^\]]*)\]", re.S)
_ALIASES = re.compile(
    r"export const GEO_GRAIN_ALIASES: [^=]+= \{(?P<body>[^}]*)\}", re.S
)
_SCOPES = re.compile(r"export const OBSERVATION_SCOPES = \[(?P<body>[^\]]*)\]", re.S)


def _source() -> str:
    assert URL_STATE.exists(), f"{URL_STATE} is gone; the rule read nothing"
    return URL_STATE.read_text(encoding="utf-8")


def _declared_levels() -> tuple[str, ...]:
    match = _LEVELS.search(_source())
    assert match, "GEO_LEVELS is not declared in the shape this rule reads"
    return tuple(re.findall(r'"([^"]+)"', match.group("body")))


def _declared_aliases() -> dict[str, str]:
    match = _ALIASES.search(_source())
    assert match, "GEO_GRAIN_ALIASES is not declared in the shape this rule reads"
    pairs = re.findall(r'(?:"([^"]+)"|(\w+))\s*:\s*"([^"]+)"', match.group("body"))
    return {quoted or bare: word for quoted, bare, word in pairs}


def test_the_two_grain_vocabularies_are_the_same_vocabulary() -> None:
    """Covers: ENV-017 — the API and the web application publish one set of grains.

    In the same order, too: the web list is what the geography picker offers
    (`GEO_GRAIN_ORDER`), and the API's order is the one the guide prints, so a
    difference in order is a difference in what a user is shown first.
    """
    declared = _declared_levels()
    assert declared == tuple(GEO_GRAINS), (
        f"{URL_STATE.relative_to(ROOT)} declares {declared} and "
        f"apps/api/registry.py declares {tuple(GEO_GRAINS)}; a grain in one "
        f"and not the other is a grain the API serves and the application "
        f"cannot select, or the reverse"
    )


def test_the_two_alias_maps_are_the_same_map() -> None:
    """Covers: ENV-017 — a word the vocabulary replaced answers on both sides.

    ADR-0002's promise is about a *stored* value: a saved configuration or a
    shared link holding `NATION` keeps answering. Both ends of that link have
    to honour it -- the API when it replays the query, the application when it
    opens the view -- so an alias in one map and not the other is half a
    promise.
    """
    declared = _declared_aliases()
    assert declared == dict(GEO_GRAIN_ALIASES), (
        f"{URL_STATE.relative_to(ROOT)} declares {declared} and "
        f"apps/api/registry.py declares {dict(GEO_GRAIN_ALIASES)}"
    )


def test_every_alias_names_a_grain_in_the_vocabulary() -> None:
    """Covers: ENV-017 — an alias resolves to a word that is actually served.

    An alias pointing at a retired grain would be worse than absent: it would
    turn a link that used to answer into a request for a grain no route
    serves, which is a refusal rather than the old view.
    """
    levels = set(_declared_levels())
    unknown = sorted(
        f"{alias} -> {word}"
        for alias, word in _declared_aliases().items()
        if word not in levels
    )
    assert not unknown, f"these aliases name a word that is not a grain: {unknown}"
    assert not set(_declared_aliases()) & levels, (
        "an alias that is also a vocabulary word would make one grain two"
    )


@pytest.mark.api
def test_the_two_observation_scopes_are_the_same_two_scopes() -> None:
    """Covers: ENV-017 — the application offers the scopes the API declares.

    The scope decides which relation answers: `latest` reads one publication,
    `as_released` reads every published release, and the guide's whole
    "As released" section rests on the two words meaning the same thing at
    both ends. The API's side is read from the served document -- the
    declaration a generated client sees -- rather than from the router, so
    this compares what is published against what the application sends.

    Marked `api` because reading the served document means importing the
    application: the rest of this module compares two declarations and needs
    only `apps.api.registry`, but this one needs FastAPI installed. The
    `etl-unit` job selects `unit and not api` into an environment built from
    the `airflow-dev` extra, which carries no FastAPI, so without the marker
    the job collects a test it cannot import. `make test-unit` -- the
    `coverage` job, which is the run the evidence register names for ENV
    rows -- installs the `api` extra and still runs it.
    """
    match = _SCOPES.search(_source())
    assert match, "OBSERVATION_SCOPES is not declared in the shape this rule reads"
    declared = tuple(re.findall(r'"([^"]+)"', match.group("body")))

    from apps.api.main import app

    served = next(
        tuple(parameter["schema"]["enum"])
        for parameter in app.openapi()["paths"]["/api/v1/observations"]["get"][
            "parameters"
        ]
        if parameter["name"] == "scope"
    )
    assert declared == served, (
        f"{URL_STATE.relative_to(ROOT)} declares {declared} and the served "
        f"contract declares {served}; a scope in one and not the other is a "
        f"read the application offers and the API refuses, or the reverse"
    )
