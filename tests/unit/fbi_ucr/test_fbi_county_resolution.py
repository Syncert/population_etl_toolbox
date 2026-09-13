"""What a county label can and cannot establish about a county.

`AGENTS.md`: "Use authoritative geography codes/mappings; do not infer
identity from names when authoritative identifiers are required."
`silver_ref/geography_contract.py` builds identity "only from exact provider
codes, never names" and resolves "without fuzzy or name-based matching".
`silver_fbi/agency.py` says county labels are "retained as evidence, never
turned into a canonical county code here".

The FBI publishes no county identifier at all -- only a label -- so
`_load_county_relationships` joins that label to the reference's county name
after upper-casing it and stripping one legal suffix. The match is exact and
uniqueness-checked, and it is still a name. It used to be recorded as
`resolution_method = 'reviewed_county_name_crosswalk'`, `confidence_class =
'reviewed'`, which is the token the *place* path earns from
`silver_fbi.reviewed_place_crosswalk` -- a table with a reviewer, an evidence
URL and a review note. Nothing reviewed the county path, and
`gold_fbi.agency_observation_area_filter` published `reviewed` over it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from data_ingestion_toolbox.fbi_ucr.silver_fbi.agency import normalize_county_label
from data_ingestion_toolbox.fbi_ucr.silver_fbi.transform import COUNTY_SUFFIX_PATTERN

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
TRANSFORM = (
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fbi_ucr/silver_fbi/transform.py"
)


def _county_loader() -> str:
    """The body of `_load_county_relationships`, read from the module."""
    source = TRANSFORM.read_text(encoding="utf-8")
    start = source.index("def _load_county_relationships(")
    return source[start : source.index("\ndef ", start + 1)]


def _reference_side(county_name: str) -> str:
    """The reference's county name as the join compares it.

    A mirror of the join's own expression -- `BTRIM(REGEXP_REPLACE(
    UPPER(county.county_name), <pattern>, ''))` -- using the module's
    pattern rather than a transcription of it. PostgreSQL and Python agree on
    this pattern's syntax: an alternation of literals anchored with `$`.
    """
    return re.sub(COUNTY_SUFFIX_PATTERN, "", county_name.upper()).strip()


def test_the_join_compares_the_two_sides_the_way_this_mirror_does() -> None:
    """Covers: ETL-050 — the mirror below is the loader's own comparison."""
    loader = _county_loader()
    assert "UPPER(county.county_name)" in loader
    assert "REGEXP_REPLACE(" in loader
    assert "%(suffix)s" in loader
    assert "labels.label" in loader
    # The label side is normalised in Python, by the parser, and is compared
    # as stored -- so the mirror must not normalise it again.
    assert "UPPER(labels.label)" not in loader


@pytest.mark.parametrize(
    ("county_name", "provider_label"),
    [
        # The reference spells the county with a diacritic; the provider does
        # not. Neither side folds accents, and folding them is name-based
        # matching, which the contract forbids.
        ("Doña Ana County", "DONA ANA"),
        # The two sources disagree about an internal space. The parser
        # collapses runs of whitespace on the label side and the reference
        # side is not touched, so "LA SALLE" and "LASALLE" stay different
        # names -- and making them equal would also make genuinely different
        # names equal.
        ("La Salle Parish", "LASALLE"),
    ],
)
def test_a_label_the_normalisation_does_not_reach_resolves_nothing(
    county_name: str, provider_label: str
) -> None:
    """Covers: ETL-050 — an unreachable label is a miss, never a guess."""
    assert normalize_county_label(provider_label) == provider_label
    assert _reference_side(county_name) != provider_label


@pytest.mark.parametrize(
    ("county_name", "provider_label"),
    [
        ("Dane County", "DANE"),
        ("Kusilvak Census Area", "KUSILVAK"),
        ("Anchorage Municipality", "ANCHORAGE"),
        ("St. Louis City", "ST. LOUIS"),
    ],
)
def test_a_label_the_normalisation_reaches_matches_exactly(
    county_name: str, provider_label: str
) -> None:
    """Covers: ETL-050 — the suffix rule is exact, not approximate."""
    assert _reference_side(county_name) == normalize_county_label(provider_label)


def test_a_county_from_a_label_is_derived_and_never_reviewed() -> None:
    """Covers: ETL-050 — the token says how the county was established."""
    loader = _county_loader()
    assert "'county_label_match'" in loader
    assert "'derived'" in loader
    assert "'reviewed'" not in loader, (
        "the county path claims a token the place path earns from "
        "silver_fbi.reviewed_place_crosswalk; nothing reviewed this match"
    )
    assert "reviewed_county_name_crosswalk" not in loader


def test_a_label_that_resolves_nothing_says_which_way_it_failed() -> None:
    """Covers: ETL-050 — the reason states what happened, not what is true.

    `canonical_county_absent` asserts the reference does not hold the county.
    A zero-match join cannot support that: the county may be there under a
    spelling this normalisation does not reach. The reason now says the label
    did not match, which is what the query actually established.
    """
    loader = _county_loader()
    assert "'county_label_unmatched'" in loader
    assert "canonical_county_absent" not in loader
    assert "'ambiguous_county_name'" in loader


def test_an_unresolved_county_label_is_not_the_same_as_no_label() -> None:
    """Covers: ETL-050 — the agency status separates a gap from a source fact.

    `agency_only` says the provider associated the agency with no county.
    An agency whose label failed to resolve fell into the same bucket, so the
    one state an operator can act on was indistinguishable from the one they
    cannot.
    """
    source = TRANSFORM.read_text(encoding="utf-8")
    status_cte = source[
        source.index("_AGENCY_STATUS_CTE = ") : source.index("_GEOGRAPHY_STATUS_SQL = ")
    ]
    assert "'agency_county_unresolved'" in status_cte
    assert "resolution_status = 'unresolved'" in status_cte
    # Ordering matters: a resolved or ambiguous county must win over it.
    assert status_cte.index("'agency_place_bridged'") < status_cte.index(
        "'agency_county_bridged'"
    )
    assert status_cte.index("'agency_county_bridged'") < status_cte.index("'ambiguous'")
    assert status_cte.index("'ambiguous'") < status_cte.index(
        "'agency_county_unresolved'"
    )
    assert status_cte.index("'agency_county_unresolved'") < status_cte.index(
        "'agency_only'"
    )
