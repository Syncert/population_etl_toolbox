"""FBI county codes to Census counties, from the file itself.

The Return A file numbers each state's counties 1, 2, 3 ... (Dane County,
Wisconsin, is 013; its FIPS code is 025). No county list ships with the file,
so each code is resolved from evidence in the same year's file:

1. **County agency name.** A sheriff's office or county police department
   (population group 8 or 9, not state police) is named after its county:
   ``DANE`` in county 013. A code whose county agencies all name the same
   Census county resolves to it (``county_agency_name``).
2. **FIPS order.** The FBI's numbering follows the order FIPS codes were
   first assigned, so in most states code *n* is the *n*-th county by FIPS
   code. A code no county agency names takes that county
   (``fips_order_rank``), but only in a state where every name-resolved code
   agrees with the order. Where one disagrees (a county created later, a
   Virginia independent city), the order cannot be trusted there and the
   code stays unresolved.

An unresolved code is recorded with its reason, and its agencies are left out
of county views; a county is never guessed.
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from ..registry import STATE_CODE_CONTRACT
from .layout import ReturnARecord

_SUFFIX = re.compile(
    r"\s+(CITY AND BOROUGH|CENSUS AREA|MUNICIPALITY|MUNICIPIO|BOROUGH|PARISH"
    r"|COUNTY|CITY)$"
)
#: Words an FBI county agency name adds to its county's name.
_AGENCY_WORDS = re.compile(
    r"\s+(COUNTY POLICE DEPARTMENT|COUNTY POLICE DEPT|COUNTY POLICE|CO POLICE"
    r"|COUNTY SHERIFF|SHERIFF'?S? OFFICE|SHERIFF|COUNTY PD|COUNTY|CO|PARISH"
    r"|POLICE DEPARTMENT|PD)$"
)


@dataclass(frozen=True)
class CountyRef:
    state_fips: str
    county_fips: str
    name: str
    geo_id: str
    geo_sk: int | None


@dataclass(frozen=True)
class CrosswalkRow:
    state_code: str
    fbi_county_code: int
    geo_id: str | None
    geo_sk: int | None
    method: str | None
    status: str
    reason_code: str | None
    evidence: str | None


def normalize(name: str) -> str:
    """A county or agency name reduced to what both spellings share."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    text = ascii_name.upper().strip()
    text = _SUFFIX.sub("", text)
    text = re.sub(r"\bSAINT\b", "ST", text)
    text = re.sub(r"\bSTE\b", "ST", text)
    return re.sub(r"[^A-Z0-9]", "", text)


def _agency_county_name(record: ReturnARecord) -> str | None:
    group = record.population_group
    if not group or group[0] not in "89" or group.endswith("E"):
        return None
    if record.ori7.endswith("SP") or len(record.county_slices) != 1:
        return None
    name = record.agency_name.upper().strip()
    previous = None
    while previous != name:
        previous = name
        name = _AGENCY_WORDS.sub("", name).strip()
    return normalize(name) or None


def build_crosswalk(
    records: Iterable[ReturnARecord],
    counties: Iterable[CountyRef],
) -> list[CrosswalkRow]:
    """One row for every (state, FBI county code) the year's records use."""
    by_state: Mapping[str, list[CountyRef]] = defaultdict(list)
    for county in counties:
        by_state[county.state_fips].append(county)  # type: ignore[index]
    codes: dict[str, set[int]] = defaultdict(set)
    named: dict[tuple[str, int], set[str]] = defaultdict(set)
    for record in records:
        for item in record.county_slices:
            codes[record.state_code].add(item.fbi_county_code)
        name = _agency_county_name(record)
        if name is not None:
            named[(record.state_code, record.county_slices[0].fbi_county_code)].add(
                name
            )

    rows: list[CrosswalkRow] = []
    for state_code in sorted(codes):
        contract = STATE_CODE_CONTRACT.get(state_code)
        state_counties = sorted(
            by_state.get(contract[1], []) if contract else [],
            key=lambda county: county.county_fips,
        )
        by_name: dict[str, list[CountyRef]] = defaultdict(list)
        for county in state_counties:
            by_name[normalize(county.name)].append(county)

        resolved: dict[int, tuple[CountyRef, str]] = {}
        failed: dict[int, tuple[str, str]] = {}
        for code in sorted(codes[state_code]):
            names = named.get((state_code, code), set())
            matches = {
                county.geo_id: county
                for name in names
                for county in by_name.get(name, [])
            }
            if len(matches) == 1:
                resolved[code] = (next(iter(matches.values())), ",".join(sorted(names)))
            elif len(matches) > 1:
                failed[code] = (
                    "conflicting_county_agency_names",
                    ",".join(sorted(names)),
                )

        ranked = {index + 1: county for index, county in enumerate(state_counties)}
        order_holds = bool(resolved) and all(
            ranked.get(code) is not None and ranked[code].geo_id == county.geo_id
            for code, (county, _) in resolved.items()
        )

        for code in sorted(codes[state_code]):
            if code in resolved:
                county, evidence = resolved[code]
                rows.append(
                    CrosswalkRow(
                        state_code,
                        code,
                        county.geo_id,
                        county.geo_sk,
                        "county_agency_name",
                        "resolved",
                        None,
                        evidence,
                    )
                )
            elif code in failed:
                reason, evidence = failed[code]
                rows.append(
                    CrosswalkRow(
                        state_code,
                        code,
                        None,
                        None,
                        None,
                        "unresolved",
                        reason,
                        evidence,
                    )
                )
            elif contract is None:
                rows.append(
                    CrosswalkRow(
                        state_code,
                        code,
                        None,
                        None,
                        None,
                        "unresolved",
                        "state_without_census_code",
                        None,
                    )
                )
            elif order_holds and code in ranked:
                county = ranked[code]
                rows.append(
                    CrosswalkRow(
                        state_code,
                        code,
                        county.geo_id,
                        county.geo_sk,
                        "fips_order_rank",
                        "resolved",
                        None,
                        f"county {code} of {len(ranked)} by FIPS",
                    )
                )
            else:
                rows.append(
                    CrosswalkRow(
                        state_code,
                        code,
                        None,
                        None,
                        None,
                        "unresolved",
                        "fips_order_disagrees_in_state"
                        if resolved
                        else "no_county_agency_in_state",
                        None,
                    )
                )
    return rows
