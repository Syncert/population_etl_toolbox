import json
import socket
import sys
import urllib.request

def get(url, timeout=25):
    print("FETCH", url, flush=True)
    req = urllib.request.Request(url, headers={"User-Agent": "population-etl-toolbox/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)

try:
    d = get("https://data.cdc.gov/api/views/hksd-2xuw/rows.json?$limit=2")
    cols = [c for c in d["meta"]["view"]["columns"]] if "view" in d["meta"] else None
    print("KEYS", list(d.keys()), flush=True)
    print("META_KEYS", list(d["meta"].keys()), flush=True)
    print("ROWS", d.get("data")[:2], flush=True)
except Exception as e:
    print("ERR", type(e).__name__, str(e)[:300], flush=True)
