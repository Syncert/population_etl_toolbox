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


# --------------------------------------------------------------------------
# A published grain is a fact about served rows, never a literal in the view.
# --------------------------------------------------------------------------

_GRAIN_ASSIGNMENT = re.compile(r"\bAS\s+valid_geo_grains\b", re.IGNORECASE)
_STRING_LITERAL = re.compile(r"'[^']*'")


def _publisher_bodies() -> dict[str, tuple[Path, str]]:
    """Each publishing schema's ``metric_publisher`` body, comments removed."""
    bodies: dict[str, tuple[Path, str]] = {}
    for path in _sql_files():
        source = _without_comments(path.read_text(encoding="utf-8"))
        for match in _PUBLISHER_PATTERN.finditer(source):
            start = match.start()
            end = source.find(";", start)
            bodies[match.group("schema").lower()] = (
                path,
                source[start : end if end != -1 else len(source)],
            )
    return bodies


def _grain_expression(body: str) -> str | None:
    """The select-item expression assigned to ``valid_geo_grains``.

    Walks back from the assignment to the comma that opens the select item,
    counting parentheses so a comma inside ``COALESCE(...)`` is not mistaken
    for the item boundary.
    """
    assignment = _GRAIN_ASSIGNMENT.search(body)
    if assignment is None:
        return None
    depth = 0
    index = assignment.start() - 1
    while index >= 0:
        character = body[index]
        if character == ")":
            depth += 1
        elif character == "(":
            depth -= 1
        elif character == "," and depth == 0:
            break
        index -= 1
    return body[index + 1 : assignment.start()].strip()


def test_every_publisher_reads_its_grains_from_rows() -> None:
    """Covers: ARC-006 — no publisher declares a geography grain.

    A declared grain is a claim about the future; a derived grain is a fact
    about the rows. Census ACS declared grains from its dataset code and
    advertised 2,487 metric/grain pairs nothing served; FRED declared
    ``ARRAY['NATIONAL']`` for every series, which was true only for as long as
    no regional series was configured; BLS mapped a configured series
    attribute through a ``CASE`` whose ``ELSE`` made an unrecognised level
    national.

    The rule is mechanical, so a seventh source cannot reintroduce it: the
    expression a publisher assigns to ``valid_geo_grains`` carries no string
    literal. A grain spelled in the view is a grain nothing has to serve.
    """
    bodies = _publisher_bodies()
    assert bodies, "no metric_publisher view was found to check"

    declared: list[str] = []
    checked: list[str] = []
    for schema, (path, body) in sorted(bodies.items()):
        expression = _grain_expression(body)
        assert expression, (
            f"{schema}.metric_publisher publishes no valid_geo_grains column; "
            "the harvest contract requires one"
        )
        checked.append(schema)
        literals = _STRING_LITERAL.findall(expression)
        if literals:
            declared.append(
                f"{path.relative_to(REPOSITORY_ROOT).as_posix()}: "
                f"{schema}.metric_publisher declares {', '.join(literals)} "
                f"in `{' '.join(expression.split())}`"
            )

    assert not declared, "\n".join(declared)
    assert len(checked) >= 6, f"only {checked} were checked; a publisher went missing"


# --------------------------------------------------------------------------
# The lineage a publisher declares is the one the registry reads rows by.
# --------------------------------------------------------------------------

_LINEAGE_OBJECT = re.compile(
    r"jsonb_build_object\((?P<body>.*?)\)\s*AS\s+physical_lineage",
    re.IGNORECASE | re.DOTALL,
)
_LINEAGE_KEY = re.compile(r"'(?P<name>[A-Za-z_][A-Za-z0-9_]*)'\s*,")
_LINEAGE_LITERAL = re.compile(
    r"'(?P<name>schema|relation)'\s*,\s*'(?P<value>[^']*)'", re.IGNORECASE
)


def _declared_lineage(body: str) -> tuple[dict[str, str], set[str]]:
    """The ``schema``/``relation`` literals and every key name a body declares."""
    match = _LINEAGE_OBJECT.search(body)
    if match is None:
        return {}, set()
    arguments = match.group("body")
    literals = {
        entry.group("name").lower(): entry.group("value")
        for entry in _LINEAGE_LITERAL.finditer(arguments)
    }
    # `jsonb_build_object` alternates key, value; the keys are the quoted
    # names that are followed by a comma at the top level of the call.
    names = {entry.group("name") for entry in _LINEAGE_KEY.finditer(arguments)}
    return literals, names - {"schema", "relation"} - set(literals.values())


def _dispatch_by_schema() -> dict[str, object]:
    """Each publishing schema's reviewed dispatch entry, where one exists."""
    published = _publisher_source_codes()
    by_code = {entry.source_code: entry for entry in OBSERVATION_DISPATCH.values()}
    return {
        schema: by_code[source_code]
        for schema, source_code in published.items()
        if source_code in by_code
    }


def test_every_publisher_declares_the_lineage_its_dispatch_entry_reads() -> None:
    """Covers: ARC-007 — the relation a publisher names is the one the API reads.

    A metric's serving rows are found through ``physical_lineage``. Before
    reading any, the neutral resource requires the lineage's declared
    ``schema``/``relation`` to equal the registry's, "so a publication/registry
    disagreement fails loudly instead of reading the wrong rows" -- and loudly
    means a sanitized 503 on every request for that source. The API is right
    to refuse; what was missing is anything that notices before a deployment
    does.

    ARC-005 attributes composed ``metric_code`` prefixes to their publishers,
    which is identity's front half. This is the half that finds the rows.
    """
    disagreements: list[str] = []
    bodies = _publisher_bodies()
    for schema, dispatch in sorted(_dispatch_by_schema().items()):
        path, body = bodies[schema]
        literals, _ = _declared_lineage(body)
        published = (literals.get("schema"), literals.get("relation"))
        declared = (dispatch.lineage_schema, dispatch.lineage_relation)
        if published != declared:
            disagreements.append(
                f"{path.name}: {schema}.metric_publisher publishes lineage "
                f"{published[0]}.{published[1]} but the dispatch entry for "
                f"{dispatch.source_code} declares {declared[0]}.{declared[1]}"
            )
    assert not disagreements, "\n".join(disagreements)


def test_every_publisher_declares_the_identity_its_dispatch_entry_binds() -> None:
    """Covers: ARC-007 — the keys the API binds are the keys the publisher writes.

    An ``identity_columns`` entry binds ``lineage.get(field)`` for each
    declared column, and a lineage that publishes no such key is refused with
    "publishes no '<field>', so its serving rows cannot be identified". A
    lineage-key entry needs ``key``. Source-agnostic: a source added later is
    checked without an edit here.
    """
    missing: list[str] = []
    bodies = _publisher_bodies()
    for schema, dispatch in sorted(_dispatch_by_schema().items()):
        path, body = bodies[schema]
        _, names = _declared_lineage(body)
        required = (
            set(dispatch.identity_columns)
            if dispatch.identity_columns
            else {"key"}
            if dispatch.lineage_key_column
            else set()
        )
        absent = sorted(required - names)
        if absent:
            missing.append(
                f"{path.name}: {schema}.metric_publisher publishes lineage keys "
                f"{sorted(names)}, which do not include {absent} that the "
                f"dispatch entry for {dispatch.source_code} binds"
            )
    assert not missing, "\n".join(missing)


def test_a_schema_without_a_dispatch_entry_is_not_judged() -> None:
    """Covers: ARC-007 — the registry lists what the API serves, not what exists.

    A publishing schema the API has not declared a dispatch entry for is
    outside these checks rather than a failure: the catalog can carry a source
    the observation resource has not yet been taught to reach, and
    ``get_metric_capability`` says exactly that about it.
    """
    judged = set(_dispatch_by_schema())
    published = set(_publisher_source_codes())
    assert judged <= published
    # And the checks above are not vacuous: every dispatch entry whose source
    # publishes through a `metric_publisher` view is judged.
    served = {entry.source_code for entry in OBSERVATION_DISPATCH.values()}
    publishing = {code for code in _publisher_source_codes().values() if code in served}
    assert len(judged) == len(publishing) >= 4, (judged, publishing)
