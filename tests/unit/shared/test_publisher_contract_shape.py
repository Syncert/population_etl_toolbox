"""A publisher view describes a measure, never a measure-and-release pair.

Covers: ARC-001 — the glossary harvest upserts on
(source_code, source_object_key), so a publisher view that emits one row per
measure *per published release* fails as soon as a second release publishes.
``harvest_all_publishers`` isolates each publisher, so the failure surfaces as
a recorded error rather than a failing DAG, and that source's catalog silently
stops following the warehouse. Three sources shipped that shape before
migration 014; this test states the rule so the fourth cannot.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from apps.api.registry import OBSERVATION_DISPATCH

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOTS = ("sql", "src")
PUBLISHER_VIEWS = ("metric_publisher", "measure_export")

#: Column names that identify one provider release rather than one measure.
RELEASE_COLUMNS = ("release_watermark", "release_key", "refresh_date")

_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(?P<name>[a-z_]+\.[a-z_]+)\s+AS(?P<body>.*?);",
    re.IGNORECASE | re.DOTALL,
)


def _sql_files() -> list[Path]:
    files: list[Path] = []
    for root in SQL_ROOTS:
        files.extend(sorted((REPOSITORY_ROOT / root).rglob("*.sql")))
    return files


def _without_comments(sql: str) -> str:
    """The statement text, with ``--`` line comments removed.

    A comment may contain a semicolon -- `gold_fred`'s publisher explains its
    served-relation join in a sentence with one -- and a statement matcher
    that treats it as the end of the statement silently reads half a view.
    Every guard in this file reads whole statements or reads nothing (DB-036).
    """
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _effective_publisher_definitions() -> dict[str, tuple[str, Path]]:
    """Return each publisher view's last definition in bootstrap order.

    A later migration replaces an earlier definition, so only the final one is
    the contract the warehouse actually serves.
    """
    definitions: dict[str, tuple[str, Path]] = {}
    for path in _sql_files():
        source = _without_comments(path.read_text(encoding="utf-8"))
        for match in _VIEW_PATTERN.finditer(source):
            name = match.group("name").lower()
            if name.rsplit(".", 1)[-1] in PUBLISHER_VIEWS:
                definitions[name] = (match.group("body"), path)
    return definitions


def _outer_group_by(body: str) -> str:
    """Return the view's outermost GROUP BY clause, ignoring subquery ones."""
    depth = 0
    for index, character in enumerate(body):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif depth == 0 and body[index:].upper().startswith("GROUP BY"):
            return body[index:]
    return ""


def test_publisher_views_exist_for_every_release_based_source() -> None:
    """Covers: ARC-001 — the rule inspects real publisher definitions."""
    definitions = _effective_publisher_definitions()
    assert {
        "gold_cdc.metric_publisher",
        "gold_fbi.metric_publisher",
        "gold_nass.metric_publisher",
        "gold_nass.measure_export",
    } <= set(definitions)


def test_no_publisher_view_groups_by_a_release_identifier() -> None:
    """Covers: ARC-001 — a publisher row is one measure, not one release."""
    offenders = []
    for name, (body, path) in sorted(_effective_publisher_definitions().items()):
        clause = _outer_group_by(body).lower()
        for column in RELEASE_COLUMNS:
            if re.search(rf"\b{column}\b", clause):
                offenders.append(f"{name} groups by {column} in {path.name}")
    assert not offenders, (
        "these publisher views emit one row per measure per release, which "
        "breaks the glossary harvest's (source_code, source_object_key) "
        f"upsert once a second release publishes: {offenders}"
    )


_PROJECTION_PATTERN = re.compile(
    r"CREATE\s+(?:MATERIALIZED\s+VIEW|TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[a-z_]+\.[a-z_]+)",
    re.IGNORECASE,
)


def _refreshed_projections() -> set[str]:
    """Served relations that are refreshed projections rather than views.

    A projection's rows lag the fact table until a refresh runs, which is the
    whole reason a publisher must read it: a grain that exists in silver and
    not in the projection is a grain the API cannot answer yet. Read from the
    DDL so a relation that changes shape changes which rule applies to it.
    """
    projections: set[str] = set()
    for path in _sql_files():
        source = _without_comments(path.read_text(encoding="utf-8"))
        for match in _PROJECTION_PATTERN.finditer(source):
            projections.add(match.group("name").lower())
    return projections


def test_every_publisher_reads_the_relation_its_source_serves() -> None:
    """Covers: DB-036 — a published grain is one the API can answer.

    `gold_fred.metric_publisher` reads `mv_fred_latest` and says why: "it is
    the relation the dispatch entry names for a `latest` read: a grain
    published here is one the API can answer". `gold_census` was changed the
    same way under DB-025/DB-028, after 2,487 unanswerable metric/grain pairs.

    `gold_bls.metric_publisher` was not: it read `fact_bls_observation`, a
    view straight over silver, so `valid_geo_grains` and `publication_time`
    advanced at silver ingest, *before* the serving refresh. Land 2025 county
    LAUS in silver, harvest, then refresh, and the catalog published `COUNTY`
    while `/observations?geo_level=COUNTY` read `mv_bls_latest` and answered
    an empty page — and because the harvest fingerprint was recorded, the
    next harvest skipped, so the catalog stayed wrong until something else
    republished.

    Derived from the dispatch registry, so a source added to it inherits the
    rule.
    """
    definitions = _effective_publisher_definitions()
    projections = _refreshed_projections()
    offenders: list[str] = []
    checked = 0
    for source_code, dispatch in sorted(OBSERVATION_DISPATCH.items()):
        latest = dispatch.latest_relation
        if latest.lower() not in projections:
            # The served relation is a view over the source's own silver
            # rows, so a publisher reading those rows *with the same
            # predicates* publishes exactly what is served, and
            # `test_served_geography_resolution.py` is what holds the
            # predicates. A refreshed projection is different: its rows lag
            # the fact table by design, so only the projection knows what the
            # API can answer today.
            continue
        schema = latest.split(".", 1)[0]
        publisher = f"{schema}.metric_publisher"
        definition = definitions.get(publisher)
        if definition is None:
            continue
        body, path = definition
        checked += 1
        # The served relation itself, or the reporting table it is built from:
        # a source whose publisher reads the reporting table reads the same
        # rows the latest projection is derived from.
        served = {latest, dispatch.released_relation}
        # The measure/series arms may read it through the source's own export
        # view, which this file already treats as a publisher definition.
        reachable = {publisher, f"{schema}.measure_export"}
        bodies = [body]
        for name in sorted(reachable):
            extra = definitions.get(name)
            if extra is not None and extra[0] != body:
                bodies.append(extra[0])
        if not any(relation in text for text in bodies for relation in served):
            offenders.append(
                f"{source_code}: {publisher} (in {path.name}) reads none of "
                f"{sorted(served)}"
            )
    assert checked, "no publisher view matched a dispatch entry's schema"
    assert not offenders, (
        "these publishers derive their catalog claims from a relation the API "
        "does not serve: " + "; ".join(offenders)
    )
