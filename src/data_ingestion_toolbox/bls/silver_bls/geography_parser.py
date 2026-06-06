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

    # ------------------------------------------------------------------
    # LAUS (program='la') in this repo is generated as:
    #   series_id = f"LA{seasonal}{area_code}{measure_code}"
    # which yields prefixes like:
    #   - LAU... (not seasonally adjusted)
    #   - LAS... (seasonally adjusted)
    # and embeds an area_code that starts with ST/CN/MT/CI or is all zeros.
    # ------------------------------------------------------------------
    if series_id.startswith("LA") and len(series_id) >= 6 and series_id[-2:].isdigit():
        seasonal = series_id[2]
        area_code = series_id[3:-2]

        if seasonal in {"S", "U"} and area_code:
            # US national: area_code is all zeros (length varies in upstream metadata)
            if area_code.strip("0") == "":
                return {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None}

            prefix = area_code[:2]
            if prefix == "ST" and len(area_code) >= 4:
                state_fips = area_code[2:4]
                if state_fips.isdigit():
                    return {
                        "geo_level": "state",
                        "geo_id": f"state:{state_fips}",
                        "state_fips": state_fips,
                        "county_fips": None,
                    }

            if prefix == "CN" and len(area_code) >= 9:
                state_fips = area_code[2:4]
                county_fips_full = area_code[4:9]
                if state_fips.isdigit() and county_fips_full.isdigit():
                    county_fips = county_fips_full[-3:]
                    return {
                        "geo_level": "county",
                        "geo_id": f"state:{state_fips}|county:{county_fips}",
                        "state_fips": state_fips,
                        "county_fips": county_fips,
                    }

            # Metro/city are not currently supported by silver_ref.dim_geo.
            # Return None so transform can log/drop rather than silently treating as US.
            return {"geo_level": None, "geo_id": None, "state_fips": None, "county_fips": None}

    if series_id.startswith("LNS"):
        return {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None}

    # Legacy patterns (kept for backward compatibility with any older LAUS IDs)
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

    # Default national geography for non-LAUS programs.
    if program in {"ln", "ce", "cu", "jt"}:
        return {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None}

    logger.warning(
        "Unrecognized BLS series_id '%s' for program '%s'. See %s",
        series_id,
        program,
        BLS_SERIES_DOC,
    )
    return {"geo_level": None, "geo_id": None, "state_fips": None, "county_fips": None}
