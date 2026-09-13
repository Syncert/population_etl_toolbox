"""A page of observations has a total order, derived from the relation's key.

`ObservationDispatch.released_order` and `latest_order` are the orders the
served routes page by, and the rule they exist to satisfy is stated on
`ServingContract`: each is "the relation's own unique-index key with the
columns the query already pins removed, so no two rows of one response can tie
on the full list and two consecutive pages can neither repeat a row nor skip
one".

Nothing checked that against the relations. FRED's entry declared
`released_order=("observation_date", "geo_id", "as_of_date", "series_id")`
while `uq_rpt_fred_observations_nk` is `(observation_date, series_id,
metric_code, realtime_start, realtime_end)` -- so the realtime window, FRED's
own vintage identity, was missing from the order. It did not show, because the
fact view published `NULL` for the window and every served row collapsed onto
one sentinel value; the tie would have appeared the moment the window reached
gold (DB-040), which is what makes a declaration-versus-DDL check the right
guard rather than a query test.

Read from the DDL the bootstrap applies, so the next dispatch entry is checked
without editing this file.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from apps.api.registry import OBSERVATION_DISPATCH

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = json.loads(
    (REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json").read_text(
        encoding="utf-8"
    )
)

#: Every released query pins the metric, so it cannot tie on it and the order
#: does not need to carry it. This is the only pinned column: `geo_id`,
#: `state_fips` and the rest are optional filters a caller may omit.
PINNED_COLUMNS = frozenset({"metric_code"})

_UNIQUE_INDEX = re.compile(
    r"CREATE\s+UNIQUE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+\w+\s+ON\s+"
    r"(?P<relation>[a-z_]+\.[a-z_]+)\s*\((?P<columns>[^;]*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)


def _bootstrap_sql() -> list[Path]:
    return [
        REPOSITORY_ROOT / asset["path"]
        for asset in MANIFEST["assets"]
        if asset["path"].endswith(".sql")
    ]


def _column_name(expression: str) -> str:
    """The column an index expression keys on.

    A key column may be wrapped to make a nullable column comparable --
    `COALESCE(metric_code, '')`, `COALESCE(realtime_start, '0001-01-01'::DATE)`
    -- and the order has to name the column, not the wrapper.
    """
    expression = expression.strip()
    match = re.match(r"COALESCE\s*\(\s*([a-z_]+)", expression, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return re.split(r"[\s:(]", expression, maxsplit=1)[0].strip().lower()


def _split_columns(columns: str) -> list[str]:
    """Split an index column list on its own commas, not a function's.

    `COALESCE(metric_code, '')` is one key column, and splitting on every
    comma turned it into two -- one of which was `'')`.
    """
    parts: list[str] = []
    depth = 0
    current = ""
    for character in columns:
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        if character == "," and depth == 0:
            parts.append(current)
            current = ""
            continue
        current += character
    parts.append(current)
    return [part for part in parts if part.strip()]


def _unique_keys() -> dict[str, list[tuple[str, ...]]]:
    """Each relation's unique-index keys, last definition winning per index."""
    keys: dict[str, list[tuple[str, ...]]] = {}
    for path in _bootstrap_sql():
        source = path.read_text(encoding="utf-8")
        for match in _UNIQUE_INDEX.finditer(source):
            relation = match.group("relation").lower()
            columns = tuple(
                _column_name(column)
                for column in _split_columns(match.group("columns"))
            )
            keys.setdefault(relation, [])
            if columns not in keys[relation]:
                keys[relation].append(columns)
    return keys


def test_the_ddl_scan_finds_the_keys_it_is_asked_about() -> None:
    """Covers: DB-040 — the rule below is not passing vacuously."""
    keys = _unique_keys()
    assert keys.get("gold_fred.rpt_fred_observations") == [
        (
            "observation_date",
            "series_id",
            "metric_code",
            "realtime_start",
            "realtime_end",
        )
    ], keys.get("gold_fred.rpt_fred_observations")
    assert ("geo_id", "observation_date", "series_id", "metric_code") in keys[
        "gold_bls.rpt_bls_observations"
    ]


@pytest.mark.parametrize(
    ("attribute", "relation_attribute"),
    [("released_order", "released_relation"), ("latest_order", "latest_relation")],
)
def test_every_paged_order_covers_its_relations_key(
    attribute: str, relation_attribute: str
) -> None:
    """Covers: DB-040 — a declared paging order is a total order.

    Only relations whose key the DDL states are checked: several sources
    serve through views, whose identity is declared on the dispatch entry
    itself rather than by an index. A relation with a key here and an order
    that does not cover it is the defect.
    """
    keys = _unique_keys()
    checked = 0
    offenders = []
    for dispatch in OBSERVATION_DISPATCH.values():
        relation = getattr(dispatch, relation_attribute, "")
        declared = getattr(dispatch, attribute) or ()
        candidates = keys.get(str(relation).lower())
        if not candidates:
            continue
        checked += 1
        # A relation may carry several unique indexes; the order has to cover
        # at least one of them, because covering one is what makes it total.
        covered = [
            key for key in candidates if set(key) - PINNED_COLUMNS <= set(declared)
        ]
        if not covered:
            offenders.append(
                f"{dispatch.source_code}.{attribute}={tuple(declared)} covers no "
                f"unique key of {relation}: {candidates}"
            )
    assert checked, f"no dispatch {attribute} was checked against a key"
    assert not offenders, (
        "these paged orders can tie, so two consecutive pages may repeat or "
        "skip a row: " + "; ".join(offenders)
    )
