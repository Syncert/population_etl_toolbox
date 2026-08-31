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


def _effective_publisher_definitions() -> dict[str, tuple[str, Path]]:
    """Return each publisher view's last definition in bootstrap order.

    A later migration replaces an earlier definition, so only the final one is
    the contract the warehouse actually serves.
    """
    definitions: dict[str, tuple[str, Path]] = {}
    for path in _sql_files():
        source = path.read_text(encoding="utf-8")
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
