from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

GEO_DOC = "https://www.census.gov/programs-surveys/geography/guidance/hierarchy.html"
FIPS_DOC = "https://www.census.gov/library/reference/code-lists/ansi.html"

_STATE_RE = re.compile(r"^[0-9]{2}$")
_COUNTY_RE = re.compile(r"^[0-9]{3}$")


def map_census_geography(
    geo_level: str,
    state_fips: Optional[str],
    county_fips: Optional[str],
) -> Optional[str]:
    """
    Map Census geo inputs to silver_ref geo_id format.
    """
    geo_level = (geo_level or "").lower()

    if geo_level == "us":
        return "us:1"

    if geo_level == "state":
        if not state_fips or not _STATE_RE.match(state_fips):
            logger.warning(
                "Invalid state_fips '%s' for geo_level=state. See %s",
                state_fips,
                FIPS_DOC,
            )
            return None
        return f"state:{state_fips}"

    if geo_level == "county":
        if not state_fips or not _STATE_RE.match(state_fips):
            logger.warning(
                "Invalid state_fips '%s' for geo_level=county. See %s",
                state_fips,
                FIPS_DOC,
            )
            return None
        if not county_fips or not _COUNTY_RE.match(county_fips):
            logger.warning(
                "Invalid county_fips '%s' for geo_level=county. See %s",
                county_fips,
                FIPS_DOC,
            )
            return None
        return f"state:{state_fips}|county:{county_fips}"

    logger.warning(
        "Unknown Census geo_level '%s'. See %s",
        geo_level,
        GEO_DOC,
    )
    return None
