"""A catalog grain is one the publisher publishes a value at."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PUBLISHERS = sorted(
    [
        *REPOSITORY_ROOT.glob("src/data_ingestion_toolbox/*/gold_*/DDL/publisher.sql"),
        REPOSITORY_ROOT
        / "src/data_ingestion_toolbox/census_pep/gold_pep/DDL/gold_pep.sql",
    ]
)
GRAIN_AGGREGATE = re.compile(
    r"ARRAY_AGG\(DISTINCT gold_glossary\.geo_grain\((?P<alias>\w+)\.\w+\)\s*"
    r"ORDER BY gold_glossary\.geo_grain\((?P=alias)\.\w+\)\)"
    r"(?P<filter>\s*FILTER\s*\(WHERE\s+(?P=alias)\.value IS NOT NULL\))?",
    re.IGNORECASE,
)


def test_every_publisher_derives_grains_from_published_values_only() -> None:
    """Covers: ARC-001 — no map level is offered where no value is published.

    Every publisher aggregated `valid_geo_grains` over every served row, so a
    grain where the provider withheld every value -- Census publishes the
    detailed-occupation tables B24114 and B24134 nationally only, and serves a
    `null` for every state and county -- was advertised, and the explorer
    offered a state map that could only ever say "value not published".
    """
    aggregates = {
        path.relative_to(REPOSITORY_ROOT).as_posix(): GRAIN_AGGREGATE.findall(
            path.read_text(encoding="utf-8")
        )
        for path in PUBLISHERS
    }

    assert len(aggregates) == 7
    assert all(aggregates.values()), aggregates
    unfiltered = {
        path: [alias for alias, found in matches if not found]
        for path, matches in aggregates.items()
        if any(not found for _alias, found in matches)
    }
    assert unfiltered == {}
