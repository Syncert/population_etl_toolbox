"""An `enforced` quality rule is one the warehouse really does refuse.

DQ-012 gave every declared rule an automation state, and `unimplemented` --
"nothing runs it or stands in for it" -- was applied to 44 of the 64,
including the eight uniqueness rules whose notes said, in prose, that the
grain is carried by a constraint. DQ-REF-001's note wrote down what was
missing: "An executor would still be needed to prove the constraints are the
declared grains."

This is that proof, and it is what lets those rules stop claiming nothing
covers them. For every relation an `enforced` rule declares, the warehouse
must carry a unique constraint or unique index whose key is exactly the
declared columns -- so a migration that drops a key, widens it, or narrows it
fails here rather than in a report nobody runs.

The one rule that does not become enforced is the reason this reads the
database rather than the notes. DQ-PEP-001 claimed the capture grain *and*
the natural key were "carried by unique constraints on the PEP relations";
only the capture grain is, `pep_fact_natural_key_idx` is a lookup index
despite its name, and a unique one would refuse a second capture of the same
vintage -- which `gold_pep.population_estimate_revision` exists to resolve.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.inventory import ALL_RULES

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: Every unique constraint and unique index on one relation, as the column
#: names each key element refers to.
#:
#: `pg_index.indkey` carries a 0 for an expression, and the expressions in
#: this warehouse are all `COALESCE(column, <constant>)` -- a serving
#: relation's grain over a nullable key column, so that rows without one are
#: still deduped rather than all distinct. That is a NULL-handling detail of
#: the same grain, so the element resolves to the column it wraps; anything
#: else is refused below rather than quietly treated as a match.
_UNIQUE_KEYS = """
    SELECT i.indexrelid::regclass::text AS index_name,
           pg_get_expr(i.indexprs, i.indrelid) AS expressions,
           i.indkey::int2[] AS key_columns,
           ARRAY(
               SELECT a.attname::text
               FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
               LEFT JOIN pg_attribute a
                 ON a.attrelid = i.indrelid AND a.attnum = k.attnum
               ORDER BY k.ord
           ) AS column_names
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.indisunique AND n.nspname = %s AND c.relname = %s
"""


def _enforced_grains() -> list[tuple[str, str, tuple[str, ...]]]:
    """Every declared grain, whatever the rule's automation state.

    `enforced_grains` says what the warehouse refuses and `automation` says
    whether that covers the rule, so a partly-refused rule declares its grain
    too and the constraint behind it is checked the same way (DQ-015).
    """
    return [
        (rule.rule_id, grain.relation, grain.columns)
        for rule in ALL_RULES
        for grain in rule.enforced_grains
    ]


def _columns_of_expression(expression: str) -> str | None:
    """The single column a `COALESCE(column, constant)` key element wraps.

    Returns ``None`` for any other expression, which the caller reports as a
    key it will not accept: an index over `lower(name)` or over two columns
    combined is a different grain, and silently accepting it would let the
    declaration drift from what the database enforces.
    """
    text = expression.strip()
    if not text.upper().startswith("COALESCE("):
        return None
    inner = text[len("COALESCE(") : text.rindex(")")]
    first = inner.split(",", 1)[0].strip()
    if not first.replace("_", "").isalnum() or first[0].isdigit():
        return None
    return first


def _split_expressions(expressions: str | None) -> list[str]:
    """`pg_get_expr` renders the expression list as a comma-separated text.

    Split at top-level commas only: every expression here is a call with
    arguments of its own.
    """
    if not expressions:
        return []
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    quoted = False
    for character in expressions:
        if character == "'":
            quoted = not quoted
        if not quoted:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            elif character == "," and depth == 0:
                parts.append("".join(current))
                current = []
                continue
        current.append(character)
    parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


def _keys_of(cursor, relation: str) -> list[tuple[str, tuple[str, ...] | None]]:
    schema, name = relation.split(".", 1)
    cursor.execute(_UNIQUE_KEYS, (schema, name))
    keys: list[tuple[str, tuple[str, ...] | None]] = []
    for index_name, expressions, key_columns, column_names in cursor.fetchall():
        rendered = _split_expressions(expressions)
        resolved: list[str] = []
        unreadable = False
        expression_index = 0
        for attnum, column in zip(key_columns, column_names):
            if attnum != 0:
                resolved.append(column)
                continue
            if expression_index >= len(rendered):
                unreadable = True
                break
            wrapped = _columns_of_expression(rendered[expression_index])
            expression_index += 1
            if wrapped is None:
                unreadable = True
                break
            resolved.append(wrapped)
        keys.append((index_name, None if unreadable else tuple(resolved)))
    return keys


@pytest.mark.parametrize(
    ("rule_id", "relation", "columns"),
    _enforced_grains(),
    ids=[f"{rule_id}:{relation}" for rule_id, relation, _ in _enforced_grains()],
)
def test_a_declared_grain_is_a_unique_key_in_the_warehouse(
    postgres_connection_factory: Callable[[], connection],
    rule_id: str,
    relation: str,
    columns: tuple[str, ...],
) -> None:
    """Covers: DQ-013 — the constraint behind an `enforced` rule exists, here.

    Read from the bootstrapped warehouse, so what is asserted is the schema a
    deployment gets rather than the DDL text a reader hopes it got.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            keys = _keys_of(cursor, relation)
    finally:
        database_connection.close()

    assert keys, (
        f"{rule_id} declares {relation} enforced at {columns} and the relation "
        f"carries no unique constraint or index at all"
    )
    declared = set(columns)
    matches = [
        name
        for name, resolved in keys
        if resolved is not None and set(resolved) == declared
    ]
    assert matches, (
        f"{rule_id} declares {relation} unique at {sorted(declared)}; its "
        f"unique keys are "
        + ", ".join(
            f"{name}={sorted(resolved) if resolved is not None else 'an expression this rule will not read'}"
            for name, resolved in keys
        )
    )


def test_the_pep_natural_key_is_not_a_key_and_the_inventory_says_so() -> None:
    """Covers: DQ-013 — the rule that could not become enforced says why.

    The note is the deliverable here: a reader of the inventory who sees
    `unimplemented` on a BLOCK uniqueness rule should learn from the note
    which half of it the database refuses and which half it cannot, rather
    than the previous claim that both were constraints.
    """
    rule = next(rule for rule in ALL_RULES if rule.rule_id == "DQ-PEP-001")
    assert rule.automation == "unimplemented"
    assert rule.enforced_grains == ()
    note = rule.automation_note
    assert "capture grain is the fact table's primary key" in note
    assert "pep_fact_natural_key_idx" in note
    assert "population_estimate_revision" in note


def test_the_pep_natural_key_index_is_still_not_unique(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DQ-013 — and the warehouse agrees with the note.

    A later migration making this index unique would refuse a legitimate
    re-capture of a vintage, so this is a guard against the fix that looks
    obvious from the index's name.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT i.indisunique
                FROM pg_index i
                WHERE i.indexrelid = 'silver_pep.pep_fact_natural_key_idx'::regclass
                """
            )
            row = cursor.fetchone()
    finally:
        database_connection.close()
    assert row is not None, "pep_fact_natural_key_idx is gone; the rule read nothing"
    assert row[0] is False, (
        "pep_fact_natural_key_idx is unique, which refuses a second capture of "
        "the same PEP vintage -- the case "
        "gold_pep.population_estimate_revision resolves by capture recency"
    )


def test_the_evidence_relations_refuse_what_dq_shared_006_says_they_do(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DQ-015 — the two claims that are constraints, named and checked.

    The grain above covers the result relation's uniqueness. The run's other
    half is a CHECK rather than a key, so it is read here: a terminal run
    with no finish is what `data_quality_run_terminal_has_finish` refuses,
    and the note for the rule names it. The third claim -- that a stored
    result is never rewritten -- has no constraint and no audit column, which
    is why the rule stays unimplemented.
    """
    rule = next(rule for rule in ALL_RULES if rule.rule_id == "DQ-SHARED-006")
    assert rule.automation == "unimplemented"
    note = rule.automation_note
    assert "data_quality_run_terminal_has_finish" in note
    assert "data_quality_result_one_per_rule_object_partition" in note

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_get_constraintdef(c.oid)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = 'control'
                  AND t.relname = 'data_quality_run'
                  AND c.conname = 'data_quality_run_terminal_has_finish'
                """
            )
            row = cursor.fetchone()
            assert row is not None, (
                "DQ-SHARED-006's note says a terminal run with no finish is "
                "refused, and the constraint it names is gone"
            )
            definition = " ".join(row[0].split())
            assert "finished_at IS NOT NULL" in definition, definition

            cursor.execute(
                """
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = 'control'
                  AND table_name = 'data_quality_result'
                  AND column_name IN ('updated_at', 'mutated_at', 'revised_at')
                """
            )
            assert cursor.fetchone()[0] == 0, (
                "the result relation gained an audit column, which is the "
                "prerequisite DQ-SHARED-006's append-only claim waits on -- "
                "measure it rather than leaving the note"
            )
    finally:
        database_connection.close()


def test_the_bounds_dq_ref_004_names_are_the_bounds_the_warehouse_holds(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DQ-016 — a per-row range the note claims is one the warehouse refuses.

    DQ-REF-004's note said overlap weights were "recorded and never measured
    against a reviewed bound". Each weight's range is refused at write time;
    what is unmeasured is the hierarchy shape the rule also claims. The
    range is a CHECK rather than a key, so it cannot be an
    `EnforcedGrain` -- it is read here by the constraint the note names, so
    the note cannot outlive it.
    """
    rule = next(rule for rule in ALL_RULES if rule.rule_id == "DQ-REF-004")
    assert rule.automation == "unimplemented"
    assert "overlap_weight_check" in rule.automation_note

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.conname, pg_get_constraintdef(c.oid)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = 'silver_ref'
                  AND t.relname = 'bridge_geo_relationship_version'
                  AND c.contype = 'c'
                """
            )
            definitions = {
                name: " ".join(text.split()) for name, text in cursor.fetchall()
            }
    finally:
        database_connection.close()

    weight = definitions.get("bridge_geo_relationship_version_overlap_weight_check", "")
    assert weight, (
        "DQ-REF-004's note names this constraint as what refuses an "
        f"impossible weight, and it is gone: {sorted(definitions)}"
    )
    assert ">= (0)::numeric" in weight and "<= (1)::numeric" in weight, weight
    area = definitions.get("bridge_geo_relationship_version_overlap_area_m2_check", "")
    assert ">= (0)::numeric" in area, area
