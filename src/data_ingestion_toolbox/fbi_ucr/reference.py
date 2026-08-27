"""Reviewed, versioned agency-to-place crosswalk for FBI UCR agencies.

An Originating Agency Identifier is not a Census place code, and the CDE
Agency resource publishes no place geography. A place association therefore
exists only where a human reviewer established it against an authoritative
Census place identifier for a stated period. Nothing in this module is derived
at runtime from an agency name, a mailing city, or a coordinate.

Countywide sheriffs, state police, tribal, university, transit,
multi-jurisdiction, and federal agencies deliberately have no entry: their
jurisdictions are not incorporated places, so bridging them to a place would
misrepresent the observation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

#: Contract version for the reviewed crosswalk content below. Bump this when
#: an entry is added, retired, or corrected so the bridge can be re-derived.
CROSSWALK_VERSION = "fbi-agency-place-crosswalk-v1"


@dataclass(frozen=True)
class ReviewedPlaceMapping:
    """One reviewed, effective-dated ORI-to-Census-place relationship."""

    ori: str
    agency_name: str
    state_fips: str
    place_fips: str
    place_name: str
    geography_vintage: int
    effective_start: date
    effective_end: date | None
    evidence_url: str
    review_note: str

    @property
    def place_geo_id(self) -> str:
        return f"state:{self.state_fips}|place:{self.place_fips}"

    def covers(self, period_start: date, period_end: date) -> bool:
        """Return True when the mapping is effective across the whole period."""
        if period_start < self.effective_start:
            return False
        return self.effective_end is None or period_end <= self.effective_end


_GAZETTEER_2023 = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2023_Gazetteer/2023_Gaz_place_national.zip"
)

REVIEWED_PLACE_MAPPINGS: tuple[ReviewedPlaceMapping, ...] = (
    ReviewedPlaceMapping(
        ori="WI0137000",
        agency_name="Fitchburg Police Department",
        state_fips="55",
        place_fips="25950",
        place_name="Fitchburg city",
        geography_vintage=2023,
        effective_start=date(2023, 1, 1),
        effective_end=None,
        evidence_url=_GAZETTEER_2023,
        review_note=(
            "Municipal police department of a single incorporated place; the "
            "2023 Census place gazetteer publishes GEOID 5525950 for "
            "Fitchburg city, Wisconsin."
        ),
    ),
    ReviewedPlaceMapping(
        ori="WI0540300",
        agency_name="Edgerton Police Department",
        state_fips="55",
        place_fips="22575",
        place_name="Edgerton city",
        geography_vintage=2023,
        effective_start=date(2023, 1, 1),
        effective_end=None,
        evidence_url=_GAZETTEER_2023,
        review_note=(
            "Municipal police department of a single incorporated place that "
            "spans two counties; the 2023 Census place gazetteer publishes "
            "GEOID 5522575 for Edgerton city, Wisconsin. The place mapping "
            "does not collapse the agency's two county associations."
        ),
    ),
)

_BY_ORI: dict[str, tuple[ReviewedPlaceMapping, ...]] = {
    ori: tuple(mapping for mapping in REVIEWED_PLACE_MAPPINGS if mapping.ori == ori)
    for ori in {mapping.ori for mapping in REVIEWED_PLACE_MAPPINGS}
}


def reviewed_place_mapping(
    ori: str, *, period_start: date, period_end: date
) -> ReviewedPlaceMapping | None:
    """Return the reviewed place mapping effective for the whole period.

    Returns ``None`` when no reviewed mapping exists or when the available
    mapping does not cover the requested period. A partially covered period is
    deliberately not bridged, because half a period is not the relationship the
    reviewer approved.
    """
    candidates = [
        mapping
        for mapping in _BY_ORI.get(ori, ())
        if mapping.covers(period_start, period_end)
    ]
    if len(candidates) != 1:
        return None
    return candidates[0]
