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


def _without_comments(sql: str) -> str:
    """The statement text, with ``--`` line comments removed.

    A predicate named in a comment is documentation, not a filter, and a
    semicolon inside one would end a statement this reader is still in the
    middle of (DB-036).
    """
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _tables_recording_resolution() -> set[str]:
    tables: set[str] = set()
    for path in _bootstrap_sql():
        for match in _TABLE_PATTERN.finditer(
            _without_comments(path.read_text(encoding="utf-8"))
        ):
            if "geography_status" in match.group("body"):
                tables.add(match.group("name").lower())
    return tables


def _effective_views() -> dict[str, tuple[str, Path]]:
    """Each view's last definition in bootstrap order, which is what runs."""
    definitions: dict[str, tuple[str, Path]] = {}
    for path in _bootstrap_sql():
        source = _without_comments(path.read_text(encoding="utf-8"))
        for match in _VIEW_PATTERN.finditer(source):
            definitions[match.group("name").lower()] = (match.group("body"), path)
    return definitions


def _reads_directly(body: str, table: str) -> bool:
    """Whether a view body reads the table itself, not a view over it."""
    return (
        re.search(rf"\b(?:FROM|JOIN)\s+{re.escape(table)}\b", body, re.IGNORECASE)
        is not None
    )


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


# ---------------------------------------------------------------------------
# One grain vocabulary, called rather than copied (DB-037)
# ---------------------------------------------------------------------------

#: A grain-bearing column each source spells in its own words. Upper-casing one
#: is a copy of the vocabulary; `gold_glossary.geo_grain` is the vocabulary.
_GRAIN_COLUMNS = ("geo_level", "geo_type", "subject_type", "agg_level_desc")
_UPPER_CASED_GRAIN = re.compile(
    r"UPPER\(\s*[A-Za-z_]*\.?(?:" + "|".join(_GRAIN_COLUMNS) + r")\s*\)",
    re.IGNORECASE,
)
#: A provider's own word, and the word the catalog publishes. A routine that
#: contains both is mapping one onto the other.
_PROVIDER_WORD = re.compile(r"'(?:us|nation|state|county|place|agency)'", re.IGNORECASE)
_PUBLISHED_WORD = re.compile(r"'(?:NATIONAL|STATE|COUNTY|PLACE|AGENCY)'")
_VOCABULARY_CALL = "gold_glossary.geo_grain("

_PROCEDURE_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+(?P<name>[a-z_]+\.[a-z_]+)\s*\((?P<body>.*?)\n\$\$;",
    re.IGNORECASE | re.DOTALL,
)


def _effective_routines() -> dict[str, tuple[str, Path]]:
    """Each view's and procedure's last definition in bootstrap order.

    Both, because the vocabulary was copied into both: publisher and fact
    views spelled it, and so did `gold_glossary.refresh_dim_geo_latest`.
    """
    definitions: dict[str, tuple[str, Path]] = {}
    for path in _bootstrap_sql():
        source = _without_comments(path.read_text(encoding="utf-8"))
        for pattern in (_VIEW_PATTERN, _PROCEDURE_PATTERN):
            for match in pattern.finditer(source):
                definitions[match.group("name").lower()] = (match.group("body"), path)
    return definitions


def test_the_grain_vocabulary_is_called_and_never_copied() -> None:
    """Covers: DB-037 — one mapping, and every routine that spells it calls it.

    Migration 018 created `gold_glossary.geo_grain` and said why: "The mapping
    lives here once. Publisher views call it to say what they publish; the
    API's dispatch entries call it to say what they serve. A mapping written
    in five places is how this defect happened." It then routed two publishers
    through the function and left five, plus three serving routines, spelling
    the vocabulary themselves — `UPPER(fact.geo_level)`, `UPPER(fact.subject_type)`,
    and two `CASE ... = 'us' THEN 'NATIONAL'` copies. None of them was wrong;
    the structure that produced the USDA NASS defect was simply still
    standing in eight more places.

    Read from the routines the bootstrap actually leaves behind, so a ninth
    copy fails here rather than in a client. A routine that *infers* a grain
    from a row's identity (`geo_id LIKE 'state:%|county:%'`) is a different
    rule and stays; what it may not do is map a provider's grain word itself.
    """
    offenders: list[str] = []
    checked = 0
    for name, (body, path) in sorted(_effective_routines().items()):
        checked += 1
        upper_cased = sorted(
            {match.group(0) for match in _UPPER_CASED_GRAIN.finditer(body)}
        )
        if upper_cased:
            offenders.append(
                f"{name} ({path.name}) upper-cases a grain column itself: {upper_cased}"
            )
            continue
        transcribes = bool(_PROVIDER_WORD.search(body)) and bool(
            _PUBLISHED_WORD.search(body)
        )
        if transcribes and _VOCABULARY_CALL not in body:
            offenders.append(
                f"{name} ({path.name}) maps a provider grain word onto a "
                "published one without calling gold_glossary.geo_grain"
            )
    assert checked, "no routine was read from the bootstrap SQL"
    assert not offenders, (
        "these routines carry their own copy of the grain vocabulary: "
        + "; ".join(offenders)
    )
