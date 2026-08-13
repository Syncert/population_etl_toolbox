from __future__ import annotations

import logging
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

    # Default national geography for non-LAUS programs.
    if program in {"ln", "ce", "cu", "jt"}:
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
