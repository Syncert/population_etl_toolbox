"""Every source's fact can name the response it came from, and say what it is.

Covers: DB-055 -- five of seven silver facts carry `capture_id` and a
        `value_status`. The two largest -- ACS and BLS -- carried neither, so
        a value the provider withheld was dropped between the revision, which
        records the withholding, and the fact, which keeps only a number. A
        consumer cannot tell a cell the provider suppressed from one it never
        published, and `AGENTS.md`'s rule against silently converting a
        suppressed value is about exactly that loss of meaning.

Every source is checked, against its own vocabulary rather than a single
spelling: FBI says `reported`/`not_reported` where the others say
`valid`/`missing`, and both are the provider's distinction rather than this
repository's preference.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRC = REPOSITORY_ROOT / "src/data_ingestion_toolbox"


@dataclass(frozen=True)
class FactContract:
    """One source's silver fact, and what its own vocabulary calls a value."""

    source: str
    ddl: Path
    relation: str
    #: The token that means "the provider published a number here". The check
    #: tying it to a non-null `value` is what this module grades.
    published_token: str
    #: PEP is the one source with nothing to withhold: its fact declares
    #: `value NUMERIC NOT NULL`, because an estimate the Bureau did not
    #: publish is a row it does not publish at all. A status column there
    #: would have exactly one value.
    can_withhold: bool = True
    #: The column the status is about. ACS calls its number `estimate_value`
    #: and reserves `value` for the served contract, so a guard that assumed
    #: one name would ask ACS for a constraint on a column its fact does not
    #: have.
    value_column: str = "value"


FACTS = [
    FactContract(
        "BLS",
        SRC / "bls/DDL/silver_bls.sql",
        "silver_bls.fact_labor_statistics",
        "valid",
    ),
    FactContract(
        "CDC",
        SRC / "cdc/DDL/silver_cdc.sql",
        "silver_cdc.fact_health_observation",
        "valid",
    ),
    FactContract(
        "CENSUS_ACS",
        SRC / "census_acs/DDL/silver_census.sql",
        "silver_census.fact_demographics",
        "valid",
        value_column="estimate_value",
    ),
    FactContract(
        "CENSUS_PEP",
        SRC / "census_pep/DDL/silver_pep.sql",
        "silver_pep.fact_population_estimate",
        "valid",
        can_withhold=False,
    ),
    FactContract(
        "FBI_UCR",
        SRC / "fbi_ucr/DDL/silver_fbi.sql",
        "silver_fbi.fact_crime_observation",
        "reported",
    ),
    FactContract(
        "FRED",
        SRC / "fred/DDL/silver_fred.sql",
        "silver_fred.fact_economic_indicators",
        "valid",
    ),
    FactContract(
        "USDA_NASS",
        SRC / "usda_nass/DDL/silver_nass.sql",
        "silver_nass.fact_crop_observation",
        "valid",
    ),
]

IDS = [contract.source for contract in FACTS]
WITHHOLDING = [contract for contract in FACTS if contract.can_withhold]
WITHHOLDING_IDS = [contract.source for contract in WITHHOLDING]


def _create_table_body(ddl: Path, relation: str) -> str:
    """The parenthesised body of one `CREATE TABLE`, and nothing else's."""
    text = ddl.read_text(encoding="utf-8")
    start = text.index(f"CREATE TABLE IF NOT EXISTS {relation} (")
    depth = 0
    for offset in range(text.index("(", start), len(text)):
        if text[offset] == "(":
            depth += 1
        elif text[offset] == ")":
            depth -= 1
            if depth == 0:
                return re.sub(r"\s+", " ", text[start : offset + 1])
    raise AssertionError(f"{relation} in {ddl.name} has an unbalanced body")


def test_every_source_package_declares_one_fact_here() -> None:
    """Covers: DB-055 — the table below is the seven sources, not a subset.

    A parametrised guard is only as complete as its list, and a source added
    without an entry would be silently exempt from every check in this module.
    The list is compared against the packages that own a `gold_*` subpackage,
    which is the same definition `test_warehouse_manifest.py` uses.
    """
    packages = {gold.parent.name for gold in SRC.glob("*/gold_*") if gold.is_dir()}
    declared = {contract.ddl.parent.parent.name for contract in FACTS}
    assert declared == packages, (
        f"these source packages declare no fact contract here, so nothing in "
        f"this module checks them: {sorted(packages - declared)}"
    )


@pytest.mark.parametrize("contract", FACTS, ids=IDS)
def test_the_fact_names_the_capture_it_was_parsed_from(contract: FactContract) -> None:
    """Covers: DB-055 — a served row can be traced to a verifiable response.

    `DQ-SHARED-001` verifies a capture's checksum. A fact with no `capture_id`
    cannot take part in that: nothing connects the number to the bytes it was
    read from, so "this row is what the provider sent" is not a claim the
    warehouse can answer.

    The reference may be direct or through the source's own revision relation
    -- PEP keys its fact on the revision's grain and inherits the capture
    reference from it, which is the stronger of the two because the fact
    cannot name a capture the revision did not parse.
    """
    body = _create_table_body(contract.ddl, contract.relation)
    assert re.search(r"\bcapture_id\b", body), (
        f"{contract.relation} carries no capture_id, so a served row cannot "
        f"be traced to the response it was parsed from"
    )
    direct = "raw_capture.response_capture(capture_id)" in body
    through_revision = "observation_revision(capture_id" in body
    assert direct or through_revision, (
        f"{contract.relation}'s capture_id is a bare column: it references "
        f"neither raw_capture.response_capture nor the source's revision, so "
        f"it can name a capture that is not there"
    )


@pytest.mark.parametrize("contract", WITHHOLDING, ids=WITHHOLDING_IDS)
def test_the_fact_says_why_a_value_is_absent(contract: FactContract) -> None:
    """Covers: DB-055 — a missing number is a status, not an empty column."""
    body = _create_table_body(contract.ddl, contract.relation)
    assert re.search(r"\bvalue_status\b", body), (
        f"{contract.relation} carries no value_status, so a value the "
        f"provider withheld is indistinguishable from one it never published"
    )


@pytest.mark.parametrize("contract", WITHHOLDING, ids=WITHHOLDING_IDS)
def test_a_published_value_cannot_be_absent(contract: FactContract) -> None:
    """Covers: DB-055 — the status and the number cannot disagree.

    This is the half that makes the status mean anything. A nullable `value`
    beside an unconstrained status lets a row claim the provider published a
    number while carrying none, which is worse than no status at all: a
    consumer reading the status believes it.

    `silver_fred.fact_economic_indicators` had the status and not the check.
    Its `value_status` even defaults to `valid`, so a writer that set no
    status at all produced a row asserting a published value it did not have.
    """
    body = _create_table_body(contract.ddl, contract.relation)
    expected = (
        f"CHECK (value_status <> '{contract.published_token}' "
        f"OR {contract.value_column} IS NOT NULL)"
    )
    assert expected in body, (
        f"{contract.relation} does not require a '{contract.published_token}' "
        f"row to carry a value, so the status and the number can disagree: "
        f"expected {expected!r}"
    )


@pytest.mark.parametrize("contract", WITHHOLDING, ids=WITHHOLDING_IDS)
def test_the_fact_keeps_the_token_the_provider_sent(contract: FactContract) -> None:
    """Covers: DB-055 — the provider's own spelling survives typing.

    A status is this pipeline's reading of a token. Keeping the token beside
    it means a reading that turns out to be wrong can be re-derived from the
    fact rather than only from the capture. Both spellings the repository
    already uses are accepted -- FRED and ACS say `source_value`, FBI and NASS
    say `value_source` -- because which word a source chose is not what this
    checks.
    """
    body = _create_table_body(contract.ddl, contract.relation)
    assert re.search(r"\bsource_value\b|\bvalue_source\b", body), (
        f"{contract.relation} keeps no copy of the provider's own token, so a "
        f"status this pipeline assigned cannot be re-derived from the fact"
    )
