---
id: tiles-places-and-metros
depends_on:
  - sub-county-geography
  - grocery-and-gasoline-prices
  - api-place-search
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/martin tests/unit/api tests/unit/shared -q
  - python -m pytest tests/dags -m dag -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_tile_boundary_versions.py tests/integration/database/test_schema_snapshot.py -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and api and database" tests/integration/api/test_tile_layer_catalog_contract.py -q
  - RUN_MARTIN_TESTS=1 python -m pytest -m martin tests/integration/martin -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - ruff format --check . ; ruff check .
---

# Place and metro map tiles, versioned by vintage (v1.0 build-out item A-6)

## Status

In progress since 2026-10-31 on `claude/project-thread-odh0cy` (the build-out
names `feat/tiles-places-and-metros`), branched from `dev` at `24459ea`.
Written first, in the branch that implements it, as the
build-out prompt (`/mnt/project-files/prompts/v1.0-build-out-prompt.md`,
Wave A, item A-6) requires.

## Why

The tile server publishes two layers: `counties` (states and counties,
`gold.tile_boundary`) and `tracts` (`gold.tile_tract`). A city or a metro has
no polygon a map can draw, so the one-measure map, the explorer and the place
pages cannot show either, even though ACS publishes at both grains and W-7
gave metros published figures. This is gap 6 of the October web-readiness
review, and the server half of D-8 ("city, metro (and ZCTA) map layers").

Two things make it more than "add two table sources":

1. **Metros have no boundary.** The geography reference loads CBSA identities
   and county membership from the Population Estimates delineation file
   (`silver_ref/area_geography.py`) but no CBSA polygon. The Census Bureau
   publishes one, the cartographic boundary file for each delineation, so the
   metro layer draws that, not a union of member counties.
2. **Boundaries change between vintages.** Places incorporate, annex and
   disincorporate; OMB redelineates metros (2013, 2015, 2017, 2018, 2020,
   2023). `gold_glossary.dim_geo_latest` keeps only the newest geometry per
   geography, so a 2012 place figure drawn on 2024 boundaries colours the
   wrong shape, and a place that no longer exists cannot be drawn at all. A
   layer must say which boundary vintage it draws and serve the one asked for.

## Dependencies

- W-2 (decade geography, part of `acs-place-tract-history-backfill`): place
  identities and boundaries for every vintage. Merged into `dev`.
- W-7 (metro figures) and `grocery-and-gasoline-prices`: metro grain, CBSA
  identities, delineation vintage and county membership. Merged into `dev`.
- `sub-county-geography`: the tract layer this follows the shape of.
- `api-place-search`: the catalog route family this joins.
- W-8 (ZCTA grain) has **not** landed, so there is no ZCTA layer. ZCTA
  boundaries exist (2020 only); adding a `zctas` layer is a follow-up once a
  source publishes at ZCTA grain.

## Deliverables

1. **Metro boundaries (warehouse, silver).** `parse_boundary_capture`
   accepts `geo_type="metro"` and reads the CBSA code (`CBSAFP`, falling back
   to `GEOID`). `sync_area_geography` captures
   `cb_<delineation>_us_cbsa_500k.zip` for the release's delineation vintage
   and loads each polygon as a geometry version with
   `boundary_vintage = delineation_vintage`. A polygon whose CBSA the
   delineation file did not list (Puerto Rico's areas, which the estimates
   file omits) is recorded `unmapped` in `silver_ref.geography_resolution`,
   not loaded and not dropped silently.
2. **Boundaries by vintage (warehouse, gold).**
   `gold_glossary.tile_boundary_version`: one row per (geography, boundary
   vintage) for the place and metro grains, with the geometry in Web
   Mercator for the tile functions, the name the reference gave it at that
   vintage, and the geometry checksum. `gold_glossary.refresh_tile_boundaries()`
   brings it in line with `silver_ref` incrementally (a row is rewritten only
   when its checksum changed; a vintage the reference no longer holds is
   removed), and the glossary DAG runs it after `refresh_dim_geo_latest`.
   `gold.tile_layer_vintage` summarises it: per grain and vintage, the
   feature count and whether it is the default (newest).
3. **Tile layers (Martin).** Two function sources, `places` (zoom 6 to 14,
   like tracts) and `metros` (zoom 0 to 12, like counties):
   `gold.tile_places(z, x, y, query_params)` and `gold.tile_metros(...)`.
   `?vintage=<year>` selects a boundary vintage; without it the layer draws
   the newest. A vintage the layer does not hold is an empty tile, never a
   fallback to another vintage, and a vintage that is not a year is refused.
   Every feature carries `geo_id` (the join key), `geo_level`, its name, its
   state where it has one, and `boundary_vintage`. Features carry no value:
   colouring is the client's join, so the uncoloured-is-not-zero rule holds
   by construction (a geography with no published value gets no colour).
4. **API.** `GET /api/v1/catalog/tile-layers`: every tile layer, its grain,
   zoom range, join key, tile template, and for the versioned layers each
   boundary vintage held with its feature count and the default. It also
   states the rule: a geography without a published value is left uncoloured,
   not drawn as zero. The web reads layers here rather than guessing from
   Martin's catalog. Documented in `API_CONSUMER_GUIDE.md`; OpenAPI snapshot
   updated.
5. **Web contract helpers (no new drawing).** `apps/web/lib/tiles.ts` gains
   the typed tile-layer catalog read and `versionedTileTemplate(layer,
   vintage)`; a shared `uncolouredSentence(noun, total, coloured)` replaces the
   tract-only copy in `WithinCounty.tsx`. The drawable grains of the
   `counties` boundary do not change: drawing places and metros on the map
   pages is D-8, which reads this contract.
6. **Checks.** Martin config unit tests pin both function sources; the live
   Martin tier seeds two place vintages and one metro, decodes tiles, proves
   `?vintage=` selects and an unheld vintage is empty, and holds every seeded
   tile under a byte budget; the database tier proves the refresh is
   incremental and removes withdrawn vintages; the API tier pins the catalog
   route; `check:bundle` holds the web budgets. A read-only measurement
   script (`scripts/measure_tile_layers.py`) records bytes and feature counts
   per layer at fixed zooms against a budget on the loaded warehouse, and the
   coverage each layer gives the geographies with published values (the
   precondition the D-8 paint check grades).

## Acceptance criteria

- [x] Metro polygons load from the published CBSA boundary file with the
      delineation vintage; unlisted CBSAs are recorded, not dropped.
- [x] `places` and `metros` are served by Martin, select a vintage by
      `?vintage=`, default to the newest, and never substitute a vintage
      (database tier against real PostGIS; the live Martin tier runs in CI).
- [x] `/api/v1/catalog/tile-layers` lists every layer and its vintages.
- [x] Features carry no values; the rule is stated in the API and helpers.
- [x] Unit, DAG, database and API tiers pass; web unit, typecheck, lint,
      build and bundle budgets pass. Martin live tier: CI (`martin-integration`).
- [ ] On OFFICE_LAB: metro boundaries loaded, refresh run, real tiles
      measured against the budget, coverage recorded (see Hand-off).

## Implementation evidence (cloud session, 2026-10-31)

Run in a cloud session with a local PostgreSQL 16 + PostGIS 3 cluster for
the database and API tiers, and a Python 3.11 Airflow 2.9.3 venv for the DAG
tier. The pinned Martin image could not be pulled here (the registry's blob
host is not reachable from this session), so the live Martin tier is left to
`martin-integration` in CI.

| Command | Result |
|---|---|
| `python -m pytest tests/unit -q` | 2831 passed |
| `python -m pytest tests/unit/martin -q` | 37 passed |
| `python -m pytest tests/dags -m dag -q` (Airflow venv) | 296 passed, 5 skipped, 5 failed; the same 5 (`test_task_callables.py`, ACS backfill planning) fail identically on `dev` without this branch |
| `pytest -m "integration and database" tests/integration/database/test_tile_boundary_versions.py` | 4 passed |
| `pytest -m "integration and database" tests/integration/database/test_schema_snapshot.py` | 3 passed after `python -m tests.support.schema_snapshot --write` |
| `pytest -m "integration and api and database" tests/integration/api/test_tile_layer_catalog_contract.py` | 1 passed |
| `npm --prefix apps/web run test:unit` | 911 passed (68 files) |
| `npm --prefix apps/web run typecheck` / `lint` / `build` | clean |
| `npm --prefix apps/web run check:bundle` | every route within budget |
| `python -m ruff check .` / `python -m ruff format --check .` (0.15.16) | clean |
| `python -m tests.support.plan_environments --write` | regenerated |
| `python tests/support/regenerate_openapi_contract.py` | 63 operations |

Catalog rows added: ETL-105, DB-067, API-177, WEB-147, MARTIN-012,
MARTIN-013; total recounted from the ranges table (666).

### Decisions

- **Function sources, not tables.** A Martin table source takes no parameter;
  versioning needs one. `gold.tile_requested_vintage` is the only place the
  default (newest) is chosen, and only when no vintage is named.
- **Geometry stored in Web Mercator** in `gold_glossary.tile_boundary_version`
  so a tile request transforms nothing. The refresh compares checksums and
  attributes before reading any geometry.
- **Metro attributes come from the delineation file**, never the boundary
  file, so the boundary load adds no attribute version and renames nothing.
- **Locate is unchanged**: a point's metro still follows from its county's
  delineated membership (`GEO_LOCATE_GRAINS` excludes METRO), so loading
  metro polygons does not change `/geographies/locate`.
- **Places start at zoom 6** like tracts; metros at zoom 0 like counties.
  The real-tile measurement on OFFICE_LAB decides whether that holds.

## Hand-off: checks that need the loaded warehouse (OFFICE_LAB)

Not run: a cloud session cannot reach the warehouse or the running Martin.

1. Apply the DDL that the ledger lacks (`gold_glossary.tile_boundary_version`,
   `gold_glossary.refresh_tile_boundaries`, `gold.tile_layer_vintage`,
   `gold.tile_requested_vintage`, `gold.tile_places`, `gold.tile_metros`)
   **before** restarting Martin: Martin refuses to start when a declared
   function is missing.
2. Run the area geography sync (`silver_ref` DAG, area task) so the 2023
   CBSA cartographic boundary is captured and loaded. Record the count of
   loaded metro polygons (expect about 925) and of `boundary_not_in_delineation`
   rows in `silver_ref.geography_resolution` (expect Puerto Rico's CBSAs).
3. `CALL gold_glossary.refresh_tile_boundaries();` and record wall time,
   then `SELECT * FROM gold.tile_layer_vintage ORDER BY 1, 2;`. Run it a
   second time and record that it is fast and rewrites nothing.
4. Restart Martin (`docker-web-1`/`docker-api-1` images rebuilt from the
   branch) and check `http://localhost:3001/tiles/places`,
   `http://localhost:3001/tiles/metros` (TileJSON with `vector_layers`) and
   `http://localhost:3001/api/v1/catalog/tile-layers`.
5. `python scripts/measure_tile_layers.py --tiles-url http://localhost:3001/tiles --dsn <warehouse DSN>`
   and paste the JSON summary here: bytes per sample tile against the 512 KB
   budget (exit code 0), and per grain the published geographies with no
   polygon and the polygons left uncoloured.
6. Spot check: `http://localhost:3001/tiles/places/10/233/421?vintage=2010`
   against `?vintage=2024` (Austin area) decode to different Austin
   boundaries, and `?vintage=1999` is empty.

### Results on OFFICE_LAB (2026-10-31, dev at 0f71e5c)

1. `scripts/apply_warehouse_manifest.py --only-flagged`: applied and recorded
   `glossary-schema` and `contract-views` before Martin restarted.
2. Area sync: `metro_boundaries` 925, `unlisted_metro_boundaries` 10; 10
   `boundary_not_in_delineation` rows in `silver_ref.geography_resolution`
   (Puerto Rico's CBSAs). 5 s.
3. `CALL gold_glossary.refresh_tile_boundaries();` 4.07 s, rerun 1.88 s.
   `gold.tile_layer_vintage`: METRO 2023 (925, default), PLACE 2025 (32,629,
   default).
4. Martin restarted healthy; `/tiles/places` and `/tiles/metros` return
   TileJSON with `vector_layers`; `/api/v1/catalog/tile-layers` returns 200.
5. `measure_tile_layers.py` exited 0 (summary below; the New York places tile
   6/18/24 is 514,278 B of the 524,288 budget). METRO coverage reports
   `published: 0` although ACS, LAUS and BEA publish metros: a defect in the
   coverage query, followed up by the A-6 thread.
6. Not passable as written: the warehouse holds place geometries for the 2025
   vintage only, so `10/233/422?vintage=2010` and `?vintage=2024` are empty
   (204) like `?vintage=1999`; the default and `?vintage=2025` return 200
   (6,224 B). Historical place boundaries (TIGER place shapefiles per vintage,
   2010-2024) are a recorded Wave W gap, after W-8.

```json
{
  "budget_bytes": 524288,
  "tiles": [
    {
      "layer": "metros",
      "area": "world",
      "tile": "0/0/0",
      "status": 200,
      "bytes": 100171,
      "features": 925
    },
    {
      "layer": "places",
      "area": "new-york",
      "tile": "6/18/24",
      "status": 200,
      "bytes": 514278,
      "features": 2925
    },
    {
      "layer": "places",
      "area": "new-york",
      "tile": "8/75/96",
      "status": 200,
      "bytes": 96189,
      "features": 508
    },
    {
      "layer": "places",
      "area": "new-york",
      "tile": "10/301/385",
      "status": 200,
      "bytes": 2102,
      "features": 7
    },
    {
      "layer": "places",
      "area": "los-angeles",
      "tile": "6/10/25",
      "status": 200,
      "bytes": 74250,
      "features": 360
    },
    {
      "layer": "places",
      "area": "los-angeles",
      "tile": "8/43/102",
      "status": 200,
      "bytes": 23890,
      "features": 88
    },
    {
      "layer": "places",
      "area": "los-angeles",
      "tile": "10/175/408",
      "status": 200,
      "bytes": 5968,
      "features": 19
    },
    {
      "layer": "places",
      "area": "austin",
      "tile": "6/14/26",
      "status": 200,
      "bytes": 157527,
      "features": 638
    },
    {
      "layer": "places",
      "area": "austin",
      "tile": "8/58/105",
      "status": 200,
      "bytes": 46394,
      "features": 114
    },
    {
      "layer": "places",
      "area": "austin",
      "tile": "10/233/421",
      "status": 200,
      "bytes": 9066,
      "features": 24
    },
    {
      "layer": "metros",
      "area": "new-york",
      "tile": "4/4/6",
      "status": 200,
      "bytes": 169693,
      "features": 358
    },
    {
      "layer": "metros",
      "area": "new-york",
      "tile": "8/75/96",
      "status": 200,
      "bytes": 3281,
      "features": 3
    },
    {
      "layer": "metros",
      "area": "los-angeles",
      "tile": "4/2/6",
      "status": 200,
      "bytes": 34146,
      "features": 51
    },
    {
      "layer": "metros",
      "area": "los-angeles",
      "tile": "8/43/102",
      "status": 200,
      "bytes": 1773,
      "features": 3
    },
    {
      "layer": "metros",
      "area": "austin",
      "tile": "4/3/6",
      "status": 200,
      "bytes": 88062,
      "features": 243
    },
    {
      "layer": "metros",
      "area": "austin",
      "tile": "8/58/105",
      "status": 200,
      "bytes": 1916,
      "features": 2
    }
  ],
  "over_budget": [],
  "coverage": [
    {
      "grain": "PLACE",
      "default_vintage": 2025,
      "polygons": 32629,
      "published": 32631,
      "published_without_polygon": 319,
      "polygons_left_uncoloured": 317
    },
    {
      "grain": "METRO",
      "default_vintage": 2023,
      "polygons": 925,
      "published": 0,
      "published_without_polygon": 0,
      "polygons_left_uncoloured": 925
    }
  ]
}
```

## What this plan deliberately does not do

- It does not draw places or metros on any web page (D-8).
- It does not add a ZCTA layer (W-8 has not landed).
- It does not derive a metro polygon from its counties.
