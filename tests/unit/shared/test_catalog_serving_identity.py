"""One metric has one spelling on both published surfaces.

Covers: ARC-005 — the glossary composes a catalog ``metric_code`` as
``source_code || ':' || source_object_key`` for every source
(``data_ingestion_toolbox.glossary.harvest``), so a serving relation that
composes its own ``metric_code`` under a different prefix publishes a second
identity for the same metric. The catalog is the documented discovery surface:
when the two disagree, a consumer that follows the catalog correctly reads a
code the serving layer has never heard of and gets an empty page that is
indistinguishable from a geography with no published values.

Census ACS shipped exactly that from the capture-first cutover (``298b73d``)
until this contract existed: ``gold_census.metric_publisher`` published
``CENSUS_ACS:acs1:B01001_001`` while ``gold_census.rpt_acs_observations``
stored ``ACS:acs1:B01001_001``. Nothing failed, because no test crossed the two
surfaces.

These checks are static and source-agnostic. They read the repository's own SQL
and the reviewed observation-dispatch registry, so a fifth source cannot
reintroduce the defect by typing a prefix that no publisher publishes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from apps.api.registry import OBSERVATION_DISPATCH

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOTS = ("sql", "src")

#: ``CREATE OR REPLACE VIEW <schema>.metric_publisher AS ... 'CODE'::TEXT AS
#: source_code`` -- the publisher declares the source code the harvest composes
#: every catalog ``metric_code`` from.
_PUBLISHER_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(?P<schema>[a-z_]+)\.metric_publisher\s+AS"
    r".*?'(?P<source_code>[A-Z][A-Z0-9_]*)'::TEXT\s+AS\s+source_code",
    re.IGNORECASE | re.DOTALL,
)

#: A composed metric-code prefix: ``'CENSUS_PEP:' || ...``.
_PREFIX_PATTERN = re.compile(r"'(?P<prefix>[A-Z][A-Z0-9_]*):'\s*\|\|")

#: The relation a statement writes or defines, used to attribute a composed
#: prefix to the schema that publishes it.
_TARGET_PATTERN = re.compile(
    r"(?:INSERT\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:MATERIALIZED\s+)?(?:VIEW|TABLE)(?:\s+IF\s+NOT\s+EXISTS)?)\s+"
    r"(?P<schema>[a-z_]+)\.[a-z_]+",
    re.IGNORECASE,
)

_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def _sql_files() -> list[Path]:
    files: list[Path] = []
    for root in SQL_ROOTS:
        files.extend(sorted((REPOSITORY_ROOT / root).rglob("*.sql")))
    return files


def _without_comments(source: str) -> str:
    """Blank out comments while preserving offsets, so positions stay usable."""
    blanked = _BLOCK_COMMENT.sub(lambda match: " " * len(match.group(0)), source)
    return _LINE_COMMENT.sub(lambda match: " " * len(match.group(0)), blanked)


def _publisher_source_codes() -> dict[str, str]:
    """Map each publishing schema to the ``source_code`` it publishes."""
    codes: dict[str, str] = {}
    for path in _sql_files():
        source = _without_comments(path.read_text(encoding="utf-8"))
        for match in _PUBLISHER_PATTERN.finditer(source):
            codes[match.group("schema").lower()] = match.group("source_code")
    return codes


def _composed_prefixes() -> list[tuple[Path, int, str, str]]:
    """Every composed metric-code prefix, attributed to its writing schema.

    Returns ``(path, line number, schema, prefix)``. A prefix composed outside
    any schema-qualified statement is attributed to ``""`` and ignored by the
    checks below, which only judge schemas that publish to the catalog.
    """
    found: list[tuple[Path, int, str, str]] = []
    for path in _sql_files():
        source = _without_comments(path.read_text(encoding="utf-8"))
        targets = [
            (match.start(), match.group("schema").lower())
            for match in _TARGET_PATTERN.finditer(source)
        ]
        for match in _PREFIX_PATTERN.finditer(source):
            schema = ""
            for start, candidate in targets:
                if start > match.start():
                    break
                schema = candidate
            line = source.count("\n", 0, match.start()) + 1
            found.append((path, line, schema, match.group("prefix")))
    return found


def test_publisher_source_codes_are_discovered_for_every_published_schema() -> None:
    """Covers: ARC-005 — the rule reads real publisher declarations."""
    codes = _publisher_source_codes()
    assert codes == {
        "gold_bls": "BLS",
        "gold_cdc": "CDC",
        "gold_census": "CENSUS_ACS",
        "gold_fbi": "FBI_UCR",
        "gold_fred": "FRED",
        "gold_nass": "USDA_NASS",
        "gold_pep": "CENSUS_PEP",
    }


def test_served_metric_codes_are_composed_from_the_published_source_code() -> None:
    """Covers: ARC-005 — serving spells a metric the way the catalog does."""
    codes = _publisher_source_codes()
    prefixes = _composed_prefixes()
    assert prefixes, "no composed metric-code prefixes were found to check"

    disagreements = [
        (
            f"{path.relative_to(REPOSITORY_ROOT).as_posix()}:{line} composes "
            f"'{prefix}:' in {schema}, but {schema}.metric_publisher publishes "
            f"source_code '{codes[schema]}', so the catalog spells the same "
            f"metric '{codes[schema]}:...'"
        )
        for path, line, schema, prefix in prefixes
        if schema in codes and prefix != codes[schema]
    ]
    assert not disagreements, "\n".join(disagreements)


def test_no_serving_relation_composes_an_unpublished_prefix() -> None:
    """Covers: ARC-005 — a prefix no publisher publishes is unreachable."""
    published = set(_publisher_source_codes().values())
    unknown = sorted(
        {
            f"{path.relative_to(REPOSITORY_ROOT).as_posix()}:{line} composes "
            f"'{prefix}:', which no publisher publishes as a source_code"
            for path, line, schema, prefix in _composed_prefixes()
            if schema and prefix not in published
        }
    )
    assert not unknown, "\n".join(unknown)


def test_dispatch_lineage_prefixes_compose_the_glossary_identity() -> None:
    """Covers: ARC-005 — the registry bridges identity, never rewrites it."""
    rewrites = [
        f"{dispatch.source_code} declares lineage_key_prefix "
        f"'{dispatch.lineage_key_prefix}', which is neither empty nor the "
        f"glossary composition '{dispatch.source_code}:'"
        for dispatch in OBSERVATION_DISPATCH.values()
        if dispatch.lineage_key_prefix
        and dispatch.lineage_key_prefix != f"{dispatch.source_code}:"
    ]
    assert not rewrites, "\n".join(rewrites)
