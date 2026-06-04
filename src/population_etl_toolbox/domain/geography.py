from enum import Enum


class GeoLevel(str, Enum):
    national = "national"
    state = "state"
    county = "county"
    cbsa = "cbsa"
    tract = "tract"
