import json
import urllib.request

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "population-etl-toolbox/0.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

d = get("https://data.cdc.gov/api/views/hksd-2xuw.json")
print("=== CDI EXACT COLUMNS (fieldName | dataTypeName | name) ===")
for c in d["columns"]:
    print(f"{c['fieldName']} | {c['dataTypeName']} | {c['name']}")
print("ROWS_COUNT:", d.get("rowsCount"))
print("META:", {k: d.get(k) for k in ("updatedAt", "createdAt", "rowsUpdatedAt", "publicationStage", "publicationAppend", "view", "name")})

# A few sample rows for realistic fixtures (US + state + county-like, with/without suppression)
rows = get("https://data.cdc.gov/api/views/hksd-2xuw/rows.json?$limit=3&$onlyData=true")
print("=== SAMPLE ROWS ===")
for row in rows:
    print(json.dumps(row))
