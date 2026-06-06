import httpx
import json
import os

# Test BLS API directly
url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Try the US-level series
series_ids = [
    "LAU00000000000000003",  # Our generated format
    "LNS14000000",           # Known national unemployment rate series
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
    print(f"\nResponse structure:")
    print(json.dumps(data, indent=2))
