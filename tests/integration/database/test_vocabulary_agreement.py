"""A closed vocabulary the code declares and the warehouse enumerates is one set.

Several closed value sets are written down twice: once as a Python constant
the service or the quality rules read, and once as a `CHECK (column IN (…))`
in shipped DDL. Nothing compared them, and the consequence of drift is
caller-visible in both directions -- a word the warehouse serves and the
constant omits is a value the route refuses while rows carry it, and a word
the constant offers and the warehouse refuses is a filter that can never
match.

ENV-017 holds the grain vocabulary across the language boundary for the same
reason. This is the other pairing: code against the database.

Each pair declares whether the two sets are *equal* or whether the code's is
a deliberate *subset*, and why. A subset is the interesting case and the one
a bare equality check would get wrong: `silver_cdc.observation_revision`
stores `geo_type = 'unsupported'` for a geography the reference could not
resolve, and that is not a grain a caller can ask for -- the serving views
exclude it (DB-035), so the request vocabulary is narrower than the storage
vocabulary on purpose.
"""

from __future__ import annotations

import re
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.sources import FBI_RESOLUTION_CONFIDENCE
from data_ingestion_toolbox.sql.cdc_queries import (
    ADJUSTMENT_STATUSES,
    GEOGRAPHY_TYPES,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import VALUE_STATUSES

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: `(label, declared words, relation, column, exact, why)`.
#:
#: `exact` is True where the two sets must be equal and False where the code's
#: is a subset the reason column explains. Nothing here is a list of words --
#: each declared set is read from the constant that is used in production, so
#: a word added to it reaches this guard without an edit.
VOCABULARY_PAIRS: tuple[tuple[str, frozenset[str], str, str, bool, str], ...] = (
    (
        "cdc_queries.ADJUSTMENT_STATUSES",
        frozenset(ADJUSTMENT_STATUSES),
        "silver_cdc.observation_revision",
        "adjustment_status",
        True,
        "every stored adjustment is one `/cdc/observations` accepts, and every "
        "word it accepts is one the rows can carry",
    ),
    (
        "cdc_queries.GEOGRAPHY_TYPES",
        frozenset(GEOGRAPHY_TYPES),
        "silver_cdc.observation_revision",
        "geo_type",
        False,
        "'unsupported' is a storage state for a geography the reference could "
        "not resolve, and the serving views exclude it (DB-035), so it is not "
        "a grain a request may name",
    ),
    (
        "silver_nass.values.VALUE_STATUSES",
        frozenset(VALUE_STATUSES),
        "silver_nass.fact_crop_observation",
        "value_status",
        True,
        "the value states `/usda-nass/observations?value_status=` refuses a "
        "word outside (API-124) are exactly the states a row can hold",
    ),
    (
        "quality.sources.FBI_RESOLUTION_CONFIDENCE keys",
        frozenset(FBI_RESOLUTION_CONFIDENCE),
        "silver_fbi.agency_geography_relationship",
        "resolution_method",
        True,
        "DQ-FBI-004 fails a resolved relationship whose method the mapping "
        "does not know, so a method the warehouse allows and the mapping "
        "omits is a rule that cannot pass",
    ),
    (
        "quality.sources.FBI_RESOLUTION_CONFIDENCE values",
        frozenset(FBI_RESOLUTION_CONFIDENCE.values()),
        "silver_fbi.agency_geography_relationship",
        "confidence_class",
        False,
        "'unresolved' is the class of a relationship with no resolution "
        "method at all, so no method maps to it",
    ),
)


def _enumerated_words(cursor, relation: str, column: str) -> frozenset[str]:
    """The words a `CHECK (column = ANY (ARRAY[…]))` allows for one column.

    The constraint may be written `column IS NULL OR column = ANY (…)` where
    the column is nullable, so the literals are read out of the rendered
    definition rather than parsed as an expression tree.
    """
    schema, name = relation.split(".", 1)
    cursor.execute(
        """
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE c.contype = 'c' AND n.nspname = %s AND t.relname = %s
        """,
        (schema, name),
    )
    for (definition,) in cursor.fetchall():
        if f"({column} = ANY (ARRAY[" not in " ".join(definition.split()):
            continue
        return frozenset(re.findall(r"'([^']+)'::text", definition))
    return frozenset()


@pytest.mark.parametrize(
    ("label", "declared", "relation", "column", "exact", "why"),
    VOCABULARY_PAIRS,
    ids=[pair[0] for pair in VOCABULARY_PAIRS],
)
def test_a_declared_vocabulary_agrees_with_the_column_that_stores_it(
    postgres_connection_factory: Callable[[], connection],
    label: str,
    declared: frozenset[str],
    relation: str,
    column: str,
    exact: bool,
    why: str,
) -> None:
    """Covers: DB-042 — one closed set, written twice, compared once."""
    assert declared, f"{label} declares no words; the rule read nothing"
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            enumerated = _enumerated_words(cursor, relation, column)
    finally:
        database_connection.close()

    assert enumerated, (
        f"{relation}.{column} carries no enumerated CHECK, so {label} is "
        f"compared against nothing"
    )
    unknown = sorted(declared - enumerated)
    assert not unknown, (
        f"{label} offers words {relation}.{column} refuses, so a filter or a "
        f"rule naming them can never match a row: {unknown}"
    )
    unclaimed = sorted(enumerated - declared)
    if exact:
        assert not unclaimed, (
            f"{relation}.{column} allows words {label} does not, so a row can "
            f"carry a value the code will not recognise: {unclaimed}. If that "
            f"is deliberate, say so in this pair's reason and make it a subset"
        )
    else:
        assert unclaimed, (
            f"{label} is declared a deliberate subset of {relation}.{column} "
            f"({why}) and the two now agree exactly -- make the pair exact"
        )
