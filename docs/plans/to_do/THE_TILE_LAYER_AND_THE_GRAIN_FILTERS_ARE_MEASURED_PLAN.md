---
id: tile-layer-and-grain-filter-reads
branch: claude/tile-layer-and-grain-filter-reads
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/martin tests/unit/api/test_serving_registry.py -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cdc or nass or fbi" -q
  - ruff format --check . ; ruff check .
---

# The tile layer and the grain filters are measured before they are indexed

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit. This is
  a measure-first plan: both deliverables begin with a measurement and end
  with a change only if the measurement says so.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

Two serving reads have grown past what they were built for, and neither has
been measured since.

**The `counties` tile layer serves every level.** `infra/martin/martin.yml`
publishes `gold.dim_geo_latest` unfiltered as the layer named `counties`.
Geometry is now loaded for state, county and place
(`src/data_ingestion_toolbox/silver_ref/geography_pipeline.py`), the
projection inserts every active `silver_ref.dim_geo` row and keeps retired
rows with a `geography_state` (`sql/gold_contract/002_gold_glossary_schema.sql`,
DB-038), and `apps/web/components/ChoroplethMap.tsx` applies no `geo_level`
filter. `tests/unit/martin/test_martin_config.py` pins the property list
without any place or state field, so the test has not noticed that the layer
carries them.

**Grain filters on the view-served sources have no supporting index.** The
API filters `geo_level` through expressions: `UPPER(subject_type)` for FBI
and `UPPER(agg_level_desc)` for NASS (`apps/api/registry.py:694,819`), and a
grain-of-type mapping for CDC and PEP, all over views on silver facts. The
indexes those views can use lead with other columns
(`sql/migrations/010_cdc_pipeline.sql:187`,
`sql/migrations/012_usda_nass_crop_pipeline.sql:297`); CDC's metric identity
includes `value_type_id`, which is in no index. The materialised sources are
indexed on `(metric_code, geo_id, observation_date)` and match the registry's
order expressions, so this concerns only the three view-served sources and
PEP.

Neither is a proven defect. Both are the kind of read that is fine at fixture
scale and not at 3,144 counties times every period, and neither has a number.

## Deliverables

### 1. Measure the tile

Record the byte size and feature count of `/tiles/counties/{z}/{x}/{y}` at
zoom 0, 4 and 8 on the smoke stack, with a breakdown by `geo_level` and
`geography_state`. If place or retired features are present, add a dedicated
tile relation (for example `gold.tile_boundary` filtered to
`geography_state = 'current'` with `geo_level` exposed, or one Martin source
per level), point the web at the level it draws, update the pinned property
list in `test_martin_config.py` and the join-key contract tests, and record
the before/after sizes here.

### 2. Measure the grain filters

Run `EXPLAIN (ANALYZE, BUFFERS)` for the registry's `/observations` query
with `geo_level=COUNTY` against CDC, FBI, NASS and PEP at fixture scale and at
a synthetic 3,144-county scale (the performance tier's bulk fixture is the
right generator). Add expression or composite indexes (rerun-safe, in the
source's DDL or a migration) only where the plan changes from a sequential
scan, and record the plans before and after in this plan.

## Acceptance criteria

- [ ] Both measurements are recorded here with the commands used.
- [ ] If the tile relation changes: the web draws the same counties,
      `tests/unit/martin` and `tests/integration/martin` pass with the new
      property list, and the smoke tier is green.
- [ ] If indexes are added: each is proven to change the plan, is rerun-safe,
      and is covered by a `DB-` or `PERF-` catalog row.
- [ ] If a measurement shows no problem, the plan records that and changes
      nothing for that deliverable.

## Definition of done

Both reads have a number beside them, and any index or relation added is
justified by the number.

## What this plan deliberately does not do

- It does not change the grain vocabulary or how the registry derives a
  grain; only what the database does with the filter.
- It does not add indexes speculatively.
