---
id: tiles-places-and-metros
depends_on:
  - acs-decade-geography-and-serving
  - metro-figures
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/martin tests/unit/api tests/unit/shared -q
  - python -m pytest tests/dags -m dag -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_tile_boundary_versions.py tests/integration/database/test_schema_snapshot.py -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest tests/integration/api/test_tile_layer_catalog_contract.py -q
  - RUN_MARTIN_TESTS=1 python -m pytest -m martin tests/integration/martin -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - ruff format --check . ; ruff check .
---

# Place and metro map tiles, versioned by vintage (v1.0 build-out item A-6)

## Status

Not started. Written first, in the branch that implements it, as the
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

- `acs-decade-geography-and-serving` (W-2): place identities for every
  vintage. Merged into `dev`.
- `metro-figures` (W-7): metro grain, CBSA identities, delineation vintage and
  county membership. Merged into `dev`.
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

- [ ] Metro polygons load from the published CBSA boundary file with the
      delineation vintage; unlisted CBSAs are recorded, not dropped.
- [ ] `places` and `metros` are served by Martin, select a vintage by
      `?vintage=`, default to the newest, and never substitute a vintage.
- [ ] `/api/v1/catalog/tile-layers` lists every layer and its vintages.
- [ ] Features carry no values; the rule is stated in the API and helpers.
- [ ] Unit, DAG, database, API and Martin tiers pass; web unit, typecheck,
      build and bundle budgets pass.
- [ ] On OFFICE_LAB: metro boundaries loaded, refresh run, real tiles
      measured against the budget, coverage recorded (cloud sessions cannot
      reach the warehouse or the running Martin; see Hand-off).

## What this plan deliberately does not do

- It does not draw places or metros on any web page (D-8).
- It does not add a ZCTA layer (W-8 has not landed).
- It does not derive a metro polygon from its counties.
