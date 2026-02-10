from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

BLS_SERIES_DOC = "https://www.bls.gov/help/hlpforma.htm"


def parse_bls_geography(series_id: str, program: str) -> Dict[str, Optional[str]]:
    """
    Parse BLS series ID into geo_level and FIPS components.
    """
    series_id = series_id or ""
    program = (program or "").lower()

    if series_id.startswith("LNS"):
        return {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None}

    if series_id.startswith("LASST"):
        state_fips = series_id[5:7]
        if state_fips == "00":
            return {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None}
        if len(state_fips) == 2 and state_fips.isdigit():
            return {
                "geo_level": "state",
                "geo_id": f"state:{state_fips}",
                "state_fips": state_fips,
                "county_fips": None,
            }

    if series_id.startswith("LAUCN") and len(series_id) >= 10:
        state_fips = series_id[5:7]
        county_fips = series_id[7:10]
        if state_fips.isdigit() and county_fips.isdigit():
            return {
                "geo_level": "county",
                "geo_id": f"state:{state_fips}|county:{county_fips}",
                "state_fips": state_fips,
                "county_fips": county_fips,
            }

    if program in {"la", "ln", "ce", "cu", "jt"}:
        return {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None}

    logger.warning(
        "Unrecognized BLS series_id '%s' for program '%s'. See %s",
        series_id,
        program,
        BLS_SERIES_DOC,
    )
    return {"geo_level": None, "geo_id": None, "state_fips": None, "county_fips": None}
