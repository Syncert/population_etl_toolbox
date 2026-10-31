#!/usr/bin/env python3
"""Measure the place and metro tile layers on a loaded stack (tiles-places-and-metros).

Read-only. Two questions, both of which only a loaded warehouse can answer:

1. **Size.** Fetches fixed sample tiles from each layer at its default
   vintage (dense metros at low and middle zooms, where a tile is largest) and
   records bytes and features per tile against ``--budget-bytes``. A tile over
   budget fails the run: it is a tile a browser would be sent.
2. **Coverage.** For each versioned grain, how many geographies with a
   published ACS population figure have a polygon in the default vintage, and
   how many polygons have no published figure. The first number is what a map
   could not draw at all; the second is what it leaves uncoloured, never drawn
   as zero. This is the precondition the D-8 paint check grades.

Usage (OFFICE_LAB, stack up)::

    python scripts/measure_tile_layers.py \
        --tiles-url http://localhost:3001/tiles \
        --dsn "postgresql://...@localhost:5432/population_etl" > tile-measure.json

``--dsn`` may be omitted to measure size only. Prints one JSON document.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass

#: MapLibre's own guidance keeps a vector tile under about 500 KB; the county
#: layer measured 516 KB at z4 after DB-053, so this is the same ceiling.
DEFAULT_BUDGET_BYTES = 512 * 1024

#: (label, longitude, latitude) of the densest places to cut a sample tile.
DENSE = (
    ("new-york", -74.006, 40.713),
    ("los-angeles", -118.244, 34.052),
    ("austin", -97.743, 30.267),
)


@dataclass(frozen=True)
class Sample:
    layer: str
    label: str
    z: int
    x: int
    y: int


def tile_for(longitude: float, latitude: float, zoom: int) -> tuple[int, int]:
    scale = 2**zoom
    x = int((longitude + 180.0) / 360.0 * scale)
    y = int(
        (1.0 - math.asinh(math.tan(math.radians(latitude))) / math.pi) / 2.0 * scale
    )
    return x, y


def samples() -> list[Sample]:
    """The tiles measured: each layer's lowest zooms over the densest areas."""
    out = [Sample("metros", "world", 0, 0, 0)]
    for layer, zooms in (("places", (6, 8, 10)), ("metros", (4, 8))):
        for label, longitude, latitude in DENSE:
            for zoom in zooms:
                x, y = tile_for(longitude, latitude, zoom)
                out.append(Sample(layer, label, zoom, x, y))
    return out


def over_budget(measured: list[dict], budget_bytes: int) -> list[dict]:
    return [row for row in measured if row["bytes"] > budget_bytes]


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, b""


def measure_sizes(tiles_url: str) -> tuple[dict, list[dict]]:
    catalog: dict = {}
    for layer in ("places", "metros"):
        status, body = _get(f"{tiles_url}/{layer}")
        catalog[layer] = json.loads(body) if status == 200 else {"status": status}
    measured = []
    for sample in samples():
        status, body = _get(
            f"{tiles_url}/{sample.layer}/{sample.z}/{sample.x}/{sample.y}"
        )
        features = None
        if body:
            try:
                from mapbox_vector_tile import decode

                features = len(decode(body).get(sample.layer, {}).get("features", []))
            except ImportError:
                features = None
        measured.append(
            {
                "layer": sample.layer,
                "area": sample.label,
                "tile": f"{sample.z}/{sample.x}/{sample.y}",
                "status": status,
                "bytes": len(body),
                "features": features,
            }
        )
    return catalog, measured


COVERAGE_SQL = """
WITH default_vintage AS (
    SELECT geo_level, boundary_vintage
    FROM gold.tile_layer_vintage
    WHERE is_default
),
drawn AS (
    SELECT boundary.geo_id, boundary.geo_level
    FROM gold_glossary.tile_boundary_version AS boundary
    JOIN default_vintage USING (geo_level, boundary_vintage)
),
published AS (
    SELECT DISTINCT served.geo_id, served.geo_level
    FROM gold_census.mv_acs_latest AS served
    WHERE served.metric_code = 'CENSUS_ACS:acs5:B01003_001'
      AND served.value_status = 'valid'
      AND served.geo_level IN ('PLACE', 'METRO')
)
SELECT level.geo_level,
       (SELECT boundary_vintage FROM default_vintage d WHERE d.geo_level = level.geo_level),
       (SELECT COUNT(*) FROM drawn d WHERE d.geo_level = level.geo_level),
       (SELECT COUNT(*) FROM published p WHERE p.geo_level = level.geo_level),
       (SELECT COUNT(*) FROM published p
         WHERE p.geo_level = level.geo_level
           AND NOT EXISTS (SELECT 1 FROM drawn d WHERE d.geo_id = p.geo_id)),
       (SELECT COUNT(*) FROM drawn d
         WHERE d.geo_level = level.geo_level
           AND NOT EXISTS (SELECT 1 FROM published p WHERE p.geo_id = d.geo_id))
FROM (VALUES ('PLACE'), ('METRO')) AS level (geo_level)
"""


def measure_coverage(dsn: str) -> list[dict]:
    import psycopg2

    connection = psycopg2.connect(dsn)
    try:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute(COVERAGE_SQL)
            rows = cursor.fetchall()
    finally:
        connection.close()
    return [
        {
            "grain": grain,
            "default_vintage": vintage,
            "polygons": polygons,
            "published": published,
            "published_without_polygon": missing,
            "polygons_left_uncoloured": uncoloured,
        }
        for grain, vintage, polygons, published, missing, uncoloured in rows
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--tiles-url", default="http://localhost:3001/tiles")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--budget-bytes", type=int, default=DEFAULT_BUDGET_BYTES)
    args = parser.parse_args(argv)

    catalog, measured = measure_sizes(args.tiles_url.rstrip("/"))
    report = {
        "budget_bytes": args.budget_bytes,
        "tilejson": catalog,
        "tiles": measured,
        "over_budget": over_budget(measured, args.budget_bytes),
        "coverage": measure_coverage(args.dsn) if args.dsn else None,
    }
    json.dump(report, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 1 if report["over_budget"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
