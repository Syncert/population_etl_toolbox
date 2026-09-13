"""A gold view over a fact table with geography_status serves resolved rows.

Covers: DB-035 — `gold_fbi.crime_observation` has excluded rows whose
geography did not resolve since migration 011. Its two siblings,
`gold_cdc.health_observation` and `gold_nass.crop_observation`, filtered on
the release status alone while their fact tables admit
`geography_status = 'unsupported'` -- a provider grain outside the served
vocabulary, with a NULL `geo_id` by constraint in NASS's case. So
`/observations` paged those rows out with `geo_id: null` and a `geo_level`
outside the five words the consumer guide promises, and the publishers, which
aggregate the grain of every fact row, advertised `UNSUPPORTED` as a grain a
client could send back.

The rule is stated over the *shape of the warehouse* rather than over a list
of three views: a fact table that records whether a geography resolved is a
fact table whose served projection has to say so. A fourth source shipping
the same shape fails here rather than in a client.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from apps.api.registry import ALLOWED_OBSERVATION_RELATIONS

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = json.loads(
    (REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json").read_text(
        encoding="utf-8"
    )
)

#: The statuses that name a failure to resolve a geography. A view that serves
#: one of them publishes a row no filter can ask for and no attribution can
#: qualify. `unmapped` is deliberately absent: its grain *is* in the
#: vocabulary and its provider identity is real, and DB-003's reviewed rule is
#: that such a miss is explicit rather than dropped.
UNRESOLVED_STATUSES = ("unsupported", "ambiguous")

_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(?P<name>[a-z_]+\.[a-z_]+)\s+AS(?P<body>.*?);",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[a-z_]+\.[a-z_]+)\s*\((?P<body>.*?)\n\);",
    re.IGNORECASE | re.DOTALL,
)


def _bootstrap_sql() -> list[Path]:
    """Every SQL asset a fresh warehouse applies, in the order it applies it."""
    return [
        REPOSITORY_ROOT / asset["path"]
        for asset in MANIFEST["assets"]
        if asset["path"].endswith(".sql")
    ]


def _tables_recording_resolution() -> set[str]:
    tables: set[str] = set()
    for path in _bootstrap_sql():
        for match in _TABLE_PATTERN.finditer(path.read_text(encoding="utf-8")):
            if "geography_status" in match.group("body"):
                tables.add(match.group("name").lower())
    return tables


def _effective_views() -> dict[str, tuple[str, Path]]:
    """Each view's last definition in bootstrap order, which is what runs."""
    definitions: dict[str, tuple[str, Path]] = {}
    for path in _bootstrap_sql():
        for match in _VIEW_PATTERN.finditer(path.read_text(encoding="utf-8")):
            definitions[match.group("name").lower()] = (match.group("body"), path)
    return definitions


def _reads_directly(body: str, table: str) -> bool:
    """Whether a view body reads the table itself, not a view over it."""
    return re.search(rf"\b(?:FROM|JOIN)\s+{re.escape(table)}\b", body, re.IGNORECASE) is not None


def _excludes_unresolved(body: str) -> bool:
    """Whether the body refuses at least one unresolved status by name."""
    if "geography_status" not in body:
        return False
    return any(f"'{status}'" in body for status in UNRESOLVED_STATUSES)


def test_every_served_observation_relation_serves_resolved_geographies() -> None:
    """Covers: DB-035 — the served projection says what silver records.

    Scoped by the registry the API dispatches through, not by a list here:
    `ALLOWED_OBSERVATION_RELATIONS` is the reviewed set of relations
    `/observations` may name, so a relation added to it inherits the rule. A
    coverage or evidence view that publishes an unresolved geography *as
    evidence* is a different thing and stays out of scope — nobody pages it as
    an observation.
    """
    recording = _tables_recording_resolution()
    assert recording, "no fact table records a geography resolution outcome"

    views = _effective_views()
    unfiltered: list[str] = []
    checked = 0
    for relation in sorted(ALLOWED_OBSERVATION_RELATIONS):
        definition = views.get(relation.lower())
        if definition is None:
            # A materialized view or table, refreshed by its own DDL; those
            # read their source's reporting relation rather than a fact table.
            continue
        body, path = definition
        for table in sorted(recording):
            if not _reads_directly(body, table):
                continue
            checked += 1
            if not _excludes_unresolved(body):
                unfiltered.append(f"{relation} (over {table}, defined in {path.name})")
    assert checked, "no served relation reads a fact table that records resolution"
    assert not unfiltered, (
        "these served relations publish geographies that did not resolve: "
        + ", ".join(unfiltered)
    )


def test_every_publisher_derives_its_grains_from_served_rows() -> None:
    """Covers: DB-035 — a grain no served row carries is not a published grain.

    The publishers aggregate `valid_geo_grains` from the fact rows themselves,
    so the same predicate belongs there: the catalog advertised `UNSUPPORTED`
    for a NASS product with one agricultural-district row, and a client
    sending that word back reached the NASS filter and got an empty 200.
    """
    recording = _tables_recording_resolution()
    offenders: list[str] = []
    for name, (body, path) in sorted(_effective_views().items()):
        if name.rsplit(".", 1)[-1] != "metric_publisher":
            continue
        if "valid_geo_grains" not in body:
            continue
        reads = [table for table in sorted(recording) if _reads_directly(body, table)]
        if not reads:
            continue
        if not _excludes_unresolved(body):
            offenders.append(f"{name} (over {', '.join(reads)}, in {path.name})")
    assert not offenders, (
        "these publishers advertise grains taken from unresolved geographies: "
        + ", ".join(offenders)
    )
