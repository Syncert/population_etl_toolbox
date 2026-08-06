"""Manual BLS API diagnostic; this is not an automated test."""

import httpx
import json
import os

# Test BLS API directly
url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Try official LAUS examples and the CPS national series.
series_ids = [
    "LAUST010000000000003",  # Alabama unemployment rate
    "LAUCN010010000000003",  # Autauga County unemployment rate
    "LNS14000000",  # CPS national unemployment rate series
]

payload = {
    "seriesid": series_ids,
    "startyear": "2023",
    "endyear": "2023",
    "registrationkey": os.environ.get("BLS_API_KEY", ""),
}

print("Requesting data from BLS API...")
print(f"Series IDs: {series_ids}")

with httpx.Client(timeout=30.0) as client:
    resp = client.post(url, json=payload)
    print(f"\nResponse status: {resp.status_code}")

    data = resp.json()
    print("\nResponse structure:")
    print(json.dumps(data, indent=2))
