from __future__ import annotations

import logging
import re
from typing import Dict, Optional

from data_ingestion_toolbox.bls.geography import parse_laus_series_id

logger = logging.getLogger(__name__)

BLS_SERIES_DOC = "https://www.bls.gov/help/hlpforma.htm"


def parse_bls_geography(series_id: str, program: str) -> Dict[str, Optional[str]]:
    """
    Parse BLS series ID into geo_level and FIPS components.
    """
    series_id = series_id or ""
    program = (program or "").lower()

    if program == "la":
        parsed = parse_laus_series_id(series_id)
        return {
            "geo_level": parsed["geo_level"],
            "geo_id": parsed["geo_id"],
            "state_fips": parsed["state_fips"],
            "county_fips": parsed["county_fips"],
        }

    if series_id.startswith("LNS"):
        return {
            "geo_level": "us",
            "geo_id": "us:1",
            "state_fips": None,
            "county_fips": None,
        }

    if program in {"cu", "ap"}:
        return parse_bls_price_area(series_id, program)

    if program == "cx":
        return parse_bls_expenditure_area(series_id)

    # Default national geography for non-LAUS programs.
    if program in {"ln", "ce", "jt"}:
        return {
            "geo_level": "us",
            "geo_id": "us:1",
            "state_fips": None,
            "county_fips": None,
        }

    logger.warning(
        "Unrecognized BLS series_id '%s' for program '%s'. See %s",
        series_id,
        program,
        BLS_SERIES_DOC,
    )
    return {"geo_level": None, "geo_id": None, "state_fips": None, "county_fips": None}


#: A current BLS CPI metropolitan area (grocery-and-gasoline-prices). The
#: discontinued `A` areas and the size classes are not places.
_CPI_METRO = re.compile(r"^S[1-4][0-9][A-Z]$")


def bls_price_area_code(series_id: str, program: str) -> Optional[str]:
    """The four-character area code inside a CPI (`cu`) or average-price (`ap`) id.

    ``CUUR0000SA0``: `CU`, seasonal, periodicity, then the area.
    ``APU0000708111``: `AP`, seasonal, then the area.
    """
    sid = (series_id or "").strip().upper()
    if program == "cu" and sid.startswith(("CU", "CW")) and len(sid) > 8:
        return sid[4:8]
    if program == "ap" and sid.startswith("AP") and len(sid) > 7:
        return sid[3:7]
    return None


def parse_bls_price_area(series_id: str, program: str) -> Dict[str, Optional[str]]:
    """Map a CPI or average-price area code to the shared reference, by code.

    `0000` is the nation; `0R00` is Census region R; `0RD0` is Census
    division D (BLS numbers its divisions as the Census Bureau does); a
    current metro area is BLS's own provider area. Anything else -- a size
    class, a discontinued area -- has no geography and is refused, never
    guessed.
    """
    unresolved: Dict[str, Optional[str]] = {
        "geo_level": None,
        "geo_id": None,
        "state_fips": None,
        "county_fips": None,
    }
    code = bls_price_area_code(series_id, program)
    if code is None:
        return unresolved
    if code == "0000":
        return {**unresolved, "geo_level": "us", "geo_id": "us:1"}
    if re.fullmatch(r"0[1-4]00", code):
        return {
            **unresolved,
            "geo_level": "census_region",
            "geo_id": f"region:{code[1]}",
        }
    if re.fullmatch(r"0[1-4][1-9]0", code):
        return {
            **unresolved,
            "geo_level": "census_division",
            "geo_id": f"division:{code[2]}",
        }
    if _CPI_METRO.fullmatch(code):
        return {
            **unresolved,
            "geo_level": "provider_area",
            "geo_id": f"area:bls_cpi:{code}",
        }
    logger.warning(
        "BLS %s series %s names area %s, which is not a geography",
        program,
        series_id,
        code,
    )
    return unresolved


#: A Consumer Expenditure series: item, then a demographic table `LBnn` and
#: its characteristic (``CXUFOODHOMELB1103M``: food at home, region table,
#: Midwest, means).
_CX_SERIES = re.compile(
    r"^CXU(?P<item>[A-Z0-9]+?)(?P<table>LB\d{2})(?P<characteristic>[0-9A-Z]{2})M$"
)

#: The region table's characteristics in BLS's order (`cx.characteristics`):
#: 02 Northeast, 03 Midwest, 04 South, 05 West -- Census regions 1 to 4.
_CX_REGIONS = {"02": "1", "03": "2", "04": "3", "05": "4"}


def parse_bls_expenditure_area(series_id: str) -> Dict[str, Optional[str]]:
    """The area a Consumer Expenditure (`cx`) mean describes.

    The region table (`LB11`) names a Census region by its characteristic,
    or all consumer units (`01`), the nation. Every other demographic table
    -- household size, income, age -- describes national consumer units of
    one kind, so it is the nation. A series of another shape is refused.
    """
    unresolved: Dict[str, Optional[str]] = {
        "geo_level": None,
        "geo_id": None,
        "state_fips": None,
        "county_fips": None,
    }
    match = _CX_SERIES.fullmatch((series_id or "").strip().upper())
    if not match:
        logger.warning("BLS cx series %s is not a demographic-table mean", series_id)
        return unresolved
    if match["table"] == "LB11" and match["characteristic"] != "01":
        region = _CX_REGIONS.get(match["characteristic"])
        if region is None:
            logger.warning("BLS cx series %s names no Census region", series_id)
            return unresolved
        return {
            **unresolved,
            "geo_level": "census_region",
            "geo_id": f"region:{region}",
        }
    return {**unresolved, "geo_level": "us", "geo_id": "us:1"}
