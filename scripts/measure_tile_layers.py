#!/usr/bin/env python3
"""Measure the place and metro tile layers on a loaded stack (tiles-places-and-metros).

Read-only. Two questions, both of which only a loaded warehouse can answer:

1. **Size.** Fetches fixed sample tiles from each layer at its default
   vintage (dense metros at low and middle zooms, where a tile is largest) and
   records bytes and features per tile against ``--budget-bytes``. A tile over
   budget fails the run: it is a tile a browser would be sent.
2. **Coverage.** For each versioned grain, how many geographies that any
   served source publishes at that grain have a polygon in the default
   vintage, and how many polygons have nothing published. The first number is what a map
   could not draw at all; the second is what it leaves uncoloured, never drawn
   as zero. This is the precondition the D-8 paint check grades. Tiles within
   10% of the budget are listed under ``near_budget``.

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
from pathlib import Path

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


#: A tile above this share of the budget is reported as near it: New York's z6
#: places tile measured 514,278 B against 524,288 before simplification, and a
#: new vintage adds vertices, so headroom is reported before it is gone.
NEAR_BUDGET_FRACTION = 0.9


def over_budget(measured: list[dict], budget_bytes: int) -> list[dict]:
    return [row for row in measured if row["bytes"] > budget_bytes]


def near_budget(
    measured: list[dict], budget_bytes: int, fraction: float = NEAR_BUDGET_FRACTION
) -> list[dict]:
    """Tiles inside the budget but within ``1 - fraction`` of it."""
    return [
        row
        for row in measured
        if budget_bytes * fraction < row["bytes"] <= budget_bytes
    ]


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


#: The polygons each grain draws at its default vintage.
POLYGONS_SQL = """
SELECT boundary.geo_level, boundary.boundary_vintage, boundary.geo_id
FROM gold_glossary.tile_boundary_version AS boundary
JOIN gold.tile_layer_vintage AS layer
  ON layer.geo_level = boundary.geo_level
 AND layer.boundary_vintage = boundary.boundary_vintage
WHERE layer.is_default
"""

#: The geographies one serving relation publishes at one grain. Every source's
#: latest relation is asked, not only ACS population: on OFFICE_LAB (10-31)
#: ACS had served no metro rows yet while LAUS and BEA did, and an
#: ACS-population probe reported all 925 metros as having nothing published.
#: The relation and its grain and geo_id expressions come from the API's own
#: dispatch registry (`apps/api/registry.py`), so a source added there is
#: measured here without an edit.
PUBLISHED_SQL = (
    "SELECT DISTINCT {geo_id} FROM {relation} WHERE {geo_level} = %(geo_level)s"
)

GRAINS = ("PLACE", "METRO")

#: Each relation's read is bounded so one slow source cannot stall the run; a
#: source that times out is reported under ``skipped_sources``.
SOURCE_TIMEOUT_MS = 300_000


def published_sources() -> list[tuple[str, str, str]]:
    """(relation, geo_id expression, grain expression) for every served source."""
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from apps.api.registry import OBSERVATION_DISPATCH

    return sorted(
        {
            (
                dispatch.latest_relation,
                dispatch.geo_id_expression,
                dispatch.geo_level_expression,
            )
            for dispatch in OBSERVATION_DISPATCH.values()
        }
    )


def coverage_rows(
    polygons: dict[str, tuple[int | None, set[str]]],
    published: dict[str, set[str]],
    by_source: dict[str, dict[str, int]],
) -> list[dict]:
    """Compare each grain's polygons with the geographies any source publishes."""
    rows = []
    for grain in GRAINS:
        vintage, drawn = polygons.get(grain, (None, set()))
        served = published.get(grain, set())
        rows.append(
            {
                "grain": grain,
                "default_vintage": vintage,
                "polygons": len(drawn),
                "published": len(served),
                "published_without_polygon": len(served - drawn),
                "polygons_left_uncoloured": len(drawn - served),
                "published_by_source": by_source.get(grain, {}),
            }
        )
    return rows


def measure_coverage(dsn: str) -> tuple[list[dict], list[dict]]:
    import psycopg2

    polygons: dict[str, tuple[int | None, set[str]]] = {}
    published: dict[str, set[str]] = {grain: set() for grain in GRAINS}
    by_source: dict[str, dict[str, int]] = {grain: {} for grain in GRAINS}
    skipped: list[dict] = []
    connection = psycopg2.connect(dsn)
    try:
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(POLYGONS_SQL)
            for grain, vintage, geo_id in cursor.fetchall():
                _vintage, ids = polygons.setdefault(grain, (vintage, set()))
                ids.add(geo_id)
            cursor.execute("SET statement_timeout = %s", (SOURCE_TIMEOUT_MS,))
            for relation, geo_id, geo_level in published_sources():
                for grain in GRAINS:
                    try:
                        cursor.execute(
                            PUBLISHED_SQL.format(
                                geo_id=geo_id, relation=relation, geo_level=geo_level
                            ),
                            {"geo_level": grain},
                        )
                    except psycopg2.Error as exc:
                        skipped.append(
                            {
                                "relation": relation,
                                "grain": grain,
                                "error": str(exc).strip().splitlines()[0],
                            }
                        )
                        continue
                    ids = {row[0] for row in cursor.fetchall() if row[0]}
                    if ids:
                        by_source[grain][relation] = len(ids)
                        published[grain] |= ids
    finally:
        connection.close()
    return coverage_rows(polygons, published, by_source), skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--tiles-url", default="http://localhost:3001/tiles")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--budget-bytes", type=int, default=DEFAULT_BUDGET_BYTES)
    args = parser.parse_args(argv)

    catalog, measured = measure_sizes(args.tiles_url.rstrip("/"))
    coverage, skipped = measure_coverage(args.dsn) if args.dsn else (None, [])
    report = {
        "budget_bytes": args.budget_bytes,
        "tilejson": catalog,
        "tiles": measured,
        "over_budget": over_budget(measured, args.budget_bytes),
        "near_budget": near_budget(measured, args.budget_bytes),
        "coverage": coverage,
        "skipped_sources": skipped,
    }
    json.dump(report, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 1 if report["over_budget"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
