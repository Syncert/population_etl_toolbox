#!/usr/bin/env python3
"""Time the tiles a state's city map opens on, with and without ``?state=``.

Read-only. A city map (``/map/<metric>?grain=PLACE&state=48``) opens on the
state's internal point at the place layer's minzoom and asks the tile server
for every tile under its canvas. This script asks for exactly those tiles,
through the same ``/tiles`` proxy the browser uses and at most six at a time
(a browser's connection limit to one host over HTTP/1.1), twice:

* ``all_states``: ``?vintage=YYYY`` only, which is what the page asked for
  before the place tile took a state; every state's places in each tile.
* ``one_state``: ``?vintage=YYYY&state=NN``, what the page asks for now.

For each it prints the tiles, bytes, slowest and median tile, and the wall
time for the whole set, which is the part of the page's wait that is tiles.
Each run adds a throwaway ``_m=<run>`` argument, which ``gold.tile_places``
ignores, so the tile server's own cache cannot answer either set and both are
measured cold. ``--warm`` leaves it off to measure a cached second visit.

Usage (OFFICE_LAB, stack up)::

    python scripts/measure_state_tile_wait.py --tiles-url http://localhost:3001/tiles \\
        --state-fips 48 --lon -99.3 --lat 31.2 --vintage 2025 > tx-tile-wait.json

The defaults are Texas on the 2025 place boundaries in the map's 1100 x 360
canvas. Prints one JSON document.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

#: MapLibre draws a vector source in 512-pixel tiles, so a tile at zoom z
#: covers 512 CSS pixels of a map at zoom z.
TILE_PIXELS = 512

#: What a browser opens to one host over HTTP/1.1.
BROWSER_CONNECTIONS = 6


def world_pixel(longitude: float, latitude: float, zoom: int) -> tuple[float, float]:
    """A point's Web Mercator position, in CSS pixels of the world at ``zoom``."""
    size = TILE_PIXELS * 2**zoom
    x = (longitude + 180.0) / 360.0 * size
    sin = math.sin(math.radians(latitude))
    y = (0.5 - math.log((1 + sin) / (1 - sin)) / (4 * math.pi)) * size
    return x, y


def viewport_tiles(
    longitude: float, latitude: float, zoom: int, width: int, height: int
) -> list[tuple[int, int, int]]:
    """The tiles under a ``width`` x ``height`` canvas centred on the point."""
    cx, cy = world_pixel(longitude, latitude, zoom)
    last = 2**zoom - 1
    x0 = max(0, math.floor((cx - width / 2) / TILE_PIXELS))
    x1 = min(last, math.floor((cx + width / 2) / TILE_PIXELS))
    y0 = max(0, math.floor((cy - height / 2) / TILE_PIXELS))
    y1 = min(last, math.floor((cy + height / 2) / TILE_PIXELS))
    return [(zoom, x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)]


def tile_url(
    tiles_url: str,
    tile: tuple[int, int, int],
    vintage: int,
    state_fips: str | None,
    run: str | None,
) -> str:
    z, x, y = tile
    query = [f"vintage={vintage}"]
    if state_fips:
        query.append(f"state={state_fips}")
    if run:
        query.append(f"_m={run}")
    return f"{tiles_url.rstrip('/')}/places/{z}/{x}/{y}?{'&'.join(query)}"


def _fetch(url: str) -> dict:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            body = response.read()
            status = response.status
            cache_control = response.headers.get("Cache-Control")
    except urllib.error.HTTPError as error:
        body, status, cache_control = b"", error.code, None
    return {
        "url": url,
        "status": status,
        "bytes": len(body),
        "ms": round((time.perf_counter() - started) * 1000, 1),
        "cache_control": cache_control,
    }


def summarise(fetched: list[dict], wall_ms: float) -> dict:
    times = [row["ms"] for row in fetched]
    return {
        "tiles": len(fetched),
        "bytes": sum(row["bytes"] for row in fetched),
        "largest_tile_bytes": max((row["bytes"] for row in fetched), default=0),
        "slowest_tile_ms": max(times, default=0.0),
        "median_tile_ms": round(statistics.median(times), 1) if times else 0.0,
        "wall_ms": round(wall_ms, 1),
        "failed": [row["url"] for row in fetched if row["status"] not in (200, 204)],
    }


def measure_set(urls: list[str], connections: int) -> tuple[list[dict], float]:
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=connections) as pool:
        fetched = list(pool.map(_fetch, urls))
    return fetched, (time.perf_counter() - started) * 1000


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tiles-url", default="http://localhost:3001/tiles")
    parser.add_argument("--state-fips", default="48")
    parser.add_argument("--lon", type=float, default=-99.3)
    parser.add_argument("--lat", type=float, default=31.2)
    parser.add_argument("--vintage", type=int, default=2025)
    parser.add_argument("--zoom", type=int, default=6, help="the place layer's minzoom")
    parser.add_argument("--width", type=int, default=1100)
    parser.add_argument("--height", type=int, default=360)
    parser.add_argument("--connections", type=int, default=BROWSER_CONNECTIONS)
    parser.add_argument(
        "--warm", action="store_true", help="let the tile server's cache answer"
    )
    args = parser.parse_args(argv)

    tiles = viewport_tiles(args.lon, args.lat, args.zoom, args.width, args.height)
    run = None if args.warm else uuid.uuid4().hex[:8]
    report: dict = {
        "state_fips": args.state_fips,
        "vintage": args.vintage,
        "zoom": args.zoom,
        "canvas": [args.width, args.height],
        "tiles": [f"{z}/{x}/{y}" for z, x, y in tiles],
        "cold": not args.warm,
    }
    for label, state in (("all_states", None), ("one_state", args.state_fips)):
        urls = [
            tile_url(args.tiles_url, tile, args.vintage, state, run) for tile in tiles
        ]
        fetched, wall_ms = measure_set(urls, args.connections)
        report[label] = summarise(fetched, wall_ms)
        report[label]["per_tile"] = [
            {key: row[key] for key in ("url", "status", "bytes", "ms")}
            for row in fetched
        ]
        report[label]["cache_control"] = sorted(
            {row["cache_control"] or "" for row in fetched}
        )
    before, after = report["all_states"], report["one_state"]
    report["saved"] = {
        "bytes": before["bytes"] - after["bytes"],
        "wall_ms": round(before["wall_ms"] - after["wall_ms"], 1),
    }
    json.dump(report, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 1 if before["failed"] or after["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
