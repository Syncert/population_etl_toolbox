from enum import Enum


class Source(str, Enum):
    ACS = "ACS"
    BLS = "BLS"
    FRED = "FRED"
    CENSUS = "CENSUS"
    DERIVED = "DERIVED"
