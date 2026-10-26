"""Finding a place by name, and the places that contain a point (A-1)."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel

MatchKind = Literal["exact", "prefix", "word", "sounds_like", "contains_a_match"]


class PlaceMatch(BaseModel):
    """One geography a search found, with what it is and where it sits.

    ``population`` is the newest ACS 5-year total population (B01003) the
    warehouse serves for the geography, used only to rank; ``null`` where none
    is served (metros before their own ACS figures load, ZCTAs). ``county_*``
    names the county that contains the geography's internal point, for a
    city or tract; ``metro_*`` the metro whose counties include it.
    """

    geo_id: str
    geo_level: str
    name: str
    state_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_geo_id: Optional[str] = None
    county_name: Optional[str] = None
    metro_geo_id: Optional[str] = None
    metro_name: Optional[str] = None
    population: Optional[int] = None
    match: MatchKind
    #: For `contains_a_match`: the matched geography this one contains.
    contains_geo_id: Optional[str] = None


class PlaceSearchResponse(BaseModel):
    query: str
    levels: list[str]
    items: list[PlaceMatch]


class LocatedPlace(BaseModel):
    geo_id: str
    geo_level: str
    name: str
    state_fips: Optional[str] = None


class LocateResponse(BaseModel):
    """The geographies that contain a point, smallest first.

    A point outside every served boundary (the sea, a territory the reference
    does not draw) answers an empty list rather than the nearest place.
    """

    latitude: float
    longitude: float
    items: list[LocatedPlace]
