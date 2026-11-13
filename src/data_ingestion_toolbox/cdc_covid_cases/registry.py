"""The registered CDC case surveillance groupings and the counts read from them.

Each state-month request asks Socrata for ``count(*)`` of CDC's published
case records grouped by ``county_fips_code``, ``age_group``, ``hosp_yn`` and
``death_yn``. Race and ethnicity are never requested. Every group is stored
as CDC's API returned it, then summed into five counts per area, month and
age group:

- ``case_records``: every published record;
- ``hospitalized_records``: records with ``hosp_yn`` = ``Yes``;
- ``hospitalization_unknown_records``: records whose ``hosp_yn`` is neither
  ``Yes`` nor ``No`` (CDC's ``Unknown``, ``Missing``, ``NA`` or empty);
- ``death_records``: records with ``death_yn`` = ``Yes``;
- ``death_unknown_records``: records whose ``death_yn`` is neither.

These are counts of the records CDC published, not of all cases: CDC
withholds the county of residence where few people live, and states sent
CDC records with different completeness.
"""

from __future__ import annotations

from dataclasses import dataclass

PARSER_CONTRACT_VERSION = "cdc_n8mc_b4w4_grouped_json:v1"

#: The columns each state-month request groups by, in order.
GROUP_COLUMNS: tuple[str, ...] = (
    "county_fips_code",
    "age_group",
    "hosp_yn",
    "death_yn",
)

#: The columns of the slice index request (records per state and month).
INDEX_COLUMNS: tuple[str, ...] = ("res_state", "case_month")


@dataclass(frozen=True)
class AgeGroup:
    age_key: str
    label: str
    source_labels: tuple[str, ...]


AGE_GROUPS: tuple[AgeGroup, ...] = (
    AgeGroup("age_0_17", "Ages 0 to 17", ("0 - 17 years",)),
    AgeGroup("age_18_49", "Ages 18 to 49", ("18 to 49 years",)),
    AgeGroup("age_50_64", "Ages 50 to 64", ("50 to 64 years",)),
    AgeGroup("age_65_plus", "Ages 65 and over", ("65+ years",)),
    AgeGroup("age_unknown", "Age not published", ("NA", "Missing", "Unknown", "")),
)

#: Every age group together.
ALL_AGES = AgeGroup("all_ages", "All ages", ())

AGE_KEY_OF_LABEL: dict[str, str] = {
    label: group.age_key for group in AGE_GROUPS for label in group.source_labels
}


@dataclass(frozen=True)
class Measure:
    measure_id: str
    label: str
    basis: str


MEASURES: tuple[Measure, ...] = (
    Measure(
        "case_records",
        "COVID-19 case records",
        "case records CDC published for the county of residence and case month",
    ),
    Measure(
        "hospitalized_records",
        "COVID-19 case records, hospitalised",
        "published case records whose hospitalisation field is Yes",
    ),
    Measure(
        "hospitalization_unknown_records",
        "COVID-19 case records, hospitalisation not known",
        "published case records whose hospitalisation field is Unknown, Missing, NA or empty",
    ),
    Measure(
        "death_records",
        "COVID-19 case records, died",
        "published case records whose death field is Yes",
    ),
    Measure(
        "death_unknown_records",
        "COVID-19 case records, death not known",
        "published case records whose death field is Unknown, Missing, NA or empty",
    ),
)

MEASURE_IDS = frozenset(measure.measure_id for measure in MEASURES)

#: The age keys a count is served under, ``all_ages`` first.
AGE_KEYS: tuple[str, ...] = (ALL_AGES.age_key, *(group.age_key for group in AGE_GROUPS))


def age_key(label: str | None) -> str | None:
    """The registered age key of CDC's age-group text, or None when unregistered."""
    return AGE_KEY_OF_LABEL.get((label or "").strip())


def answer_kind(value: str | None) -> str:
    """``yes``, ``no`` or ``unknown`` for CDC's text in a yes/no field."""
    text = (value or "").strip()
    if text == "Yes":
        return "yes"
    if text == "No":
        return "no"
    return "unknown"
