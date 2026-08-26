import json
import urllib.request

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "population-etl-toolbox/0.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

# Fetch a broader slice: US national rows + a suppressed one + a county-level one.
# CDI supports $where. Grab 5 rows of interest by topic to see units/suppression.
cols = [c["fieldName"] for c in get("https://data.cdc.gov/api/views/hksd-2xuw.json")["columns"]]
print("COLUMNS:", cols)

def fetch(where, limit=5):
    url = f"https://data.cdc.gov/api/views/hksd-2xuw/rows.json?$limit={limit}&$where={where}"
    d = get(url)
    return d["data"]

print("=== NATIONAL (locationid=US) ===")
for row in fetch("locationid='US'", 4):
    print(json.dumps(dict(zip(cols, row))))

print("=== A STATE (California) ===")
for row in fetch("locationid='06'", 2):
    print(json.dumps(dict(zip(cols, row))))

print("=== SUPPRESSED / footnote present ===")
for row in fetch("datavalue is null and locationid='US'", 3):
    print(json.dumps(dict(zip(cols, row))))

print("=== COUNTY-level locationid sample ===")
# locationid format for county is 2-digit state + 3-digit county, e.g. '06037'
for row in fetch("length(locationid)=5", 2):
    print(json.dumps(dict(zip(cols, row))))
