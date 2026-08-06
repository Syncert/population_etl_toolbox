"""Smoke-test the live Census, BLS, and FRED source APIs.

The script reads API keys from the environment, never prints them, and exits
non-zero unless each provider returns at least one usable observation.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TIMEOUT_SECONDS = 30


def _json_request(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> Any:
    if params:
        url = f"{url}?{urlencode(params)}"
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "population-etl-toolbox/source-api-validator",
        },
        method="POST" if body is not None else "GET",
    )
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def validate_census() -> dict[str, Any]:
    params = {
        "get": "NAME,B01003_001E",
        "for": "us:*",
    }
    api_key = os.environ.get("CENSUS_API_KEY", "").strip()
    if api_key:
        params["key"] = api_key
    payload = _json_request("https://api.census.gov/data/2023/acs/acs5", params=params)
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Census returned no ACS population rows")
    headings, first = payload[0], payload[1]
    population_index = headings.index("B01003_001E")
    population = int(first[population_index])
    if population <= 0:
        raise ValueError("Census returned a non-positive population")
    return {
        "status": "ok",
        "dataset": "2023 ACS 5-year",
        "records": len(payload) - 1,
        "sample": {"name": first[headings.index("NAME")], "population": population},
    }


def validate_bls() -> dict[str, Any]:
    current_year = date.today().year
    request_payload: dict[str, Any] = {
        "seriesid": ["LNS14000000"],
        "startyear": str(current_year - 1),
        "endyear": str(current_year),
    }
    api_key = os.environ.get("BLS_API_KEY", "").strip()
    if api_key:
        request_payload["registrationkey"] = api_key
    payload = _json_request(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        payload=request_payload,
    )
    if payload.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(
            f"BLS request failed: {payload.get('message') or payload.get('status')}"
        )
    series = payload.get("Results", {}).get("series", [])
    observations = series[0].get("data", []) if series else []
    usable = [item for item in observations if item.get("value") not in {None, "", "-"}]
    if not usable:
        raise ValueError("BLS returned no usable unemployment observations")
    sample = usable[0]
    return {
        "status": "ok",
        "series": "LNS14000000",
        "records": len(usable),
        "sample": {
            "period": f"{sample.get('year')} {sample.get('periodName')}",
            "value": sample.get("value"),
        },
    }


def validate_fred() -> dict[str, Any]:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        raise ValueError("FRED_API_KEY is required for the official FRED API")
    payload = _json_request(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": "CPIAUCSL",
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,
        },
    )
    observations = [
        item
        for item in payload.get("observations", [])
        if item.get("value") not in {None, "", "."}
    ]
    if not observations:
        raise ValueError("FRED returned no usable CPI observations")
    sample = observations[0]
    return {
        "status": "ok",
        "series": "CPIAUCSL",
        "records": len(observations),
        "sample": {"date": sample.get("date"), "value": sample.get("value")},
    }


def main() -> int:
    results: dict[str, Any] = {}
    failed = False
    for name, validator in (
        ("census", validate_census),
        ("bls", validate_bls),
        ("fred", validate_fred),
    ):
        try:
            results[name] = validator()
        except (
            HTTPError,
            URLError,
            TimeoutError,
            ValueError,
            KeyError,
            TypeError,
        ) as exc:
            failed = True
            results[name] = {
                "status": "failed",
                "error": str(exc)[:300],
            }
    print(json.dumps(results, indent=2, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
