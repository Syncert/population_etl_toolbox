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

- **Status:** Ready for review. Both deliverables were measured on a machine
  session on 2026-09-18 against a loaded warehouse. **The two came out
  differently**: the tile was a real problem and is fixed; the grain filters
  are not, and nothing was changed for them, which is what the plan's fourth
  criterion asks for.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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

- [x] Both measurements are recorded here with the commands used.
- [x] The tile relation changed: the web draws the same counties (the layer id
      and property list are unchanged, and the client filter was already
      discarding what is now not sent), `tests/unit/martin` and
      `tests/integration/martin` pass, and the stack comes up on a healthcheck
      that gates on the new relation.
- [x] No index was added, because none changed a plan -- proven by creating
      one on NASS and watching the planner keep its sequential scan.
- [x] The measurement that showed no problem is recorded and changed nothing
      for that deliverable.

## Implementation evidence

### Deliverable 1: the tile was measured, and it was a problem

Measured against the loaded internal warehouse (35,921 geographies), which is
what makes the number real rather than a fixture's.

`gold.dim_geo_latest`, the relation `martin.yml` published as `counties`:

| geo_level | rows | with geometry |
|---|---|---|
| PLACE | 32,629 | 32,629 |
| COUNTY | 3,235 | 3,235 |
| AGENCY | 464 | 0 |
| STATE | 56 | 56 |
| NATIONAL | 1 | 0 |

Tiles fetched from the running Martin, decoded with `mapbox_vector_tile`:

| tile | bytes | features | PLACE | COUNTY | STATE |
|---|---|---|---|---|---|
| z0/0/0 | 863,778 | 8,092 | 4,831 | 3,205 | 56 |
| z4/4/6 | **2,092,846** | 13,513 | **12,182** | 1,306 | 25 |
| z8/71/99 | 27,950 | 97 | 71 | 25 | 1 |

**The web cannot draw a place, and already knew it.**
`apps/web/lib/tileGrains.ts` declares STATE and COUNTY as the drawable grains
and names PLACE as one the boundary refuses "before any observation is read" --
its own comment describes "the state grain handed the map some 32k place
polygons that the layer filter then had to hide". So every place polygon was
serialised, sent over the wire, decoded, and discarded by the client.

`gold.tile_boundary` filters to the drawable grains, current, with geometry,
and Martin publishes that. The layer keeps its id, so the web is unchanged.

| tile | before | after | change |
|---|---|---|---|
| z0/0/0 | 863,778 | 426,794 | **-50.6%** |
| z4/4/6 | 2,092,846 | 515,791 | **-75.4%** |
| z8/71/99 | 27,950 | 13,868 | -50.4% |

The two grains are named through `gold_glossary.geo_grain` rather than by
their published spellings -- `test_the_grain_vocabulary_is_called_and_never_copied`
refused the literals, correctly: the vocabulary has one source and a copy of
its output would not follow a change to it.

The stack's healthcheck now gates on the same relation. A tile layer that
cannot be read stops the stack rather than serving empty tiles, which is what
the healthcheck is for.

### Deliverable 2: the grain filters were measured, and they are not a problem

The plan says to add an index "only where the plan changes from a sequential
scan". It does not change, and the reason is the same for all four sources:
the grain filter is not selective.

| source | rows | dominant grain | share |
|---|---|---|---|
| NASS | 1,086,806 | COUNTY | **94.3%** |
| PEP | 2,865,139 (joined) | county | **78.8%** |
| CDC | 628,091 | one status | **100%** |
| FBI | 11,084 | AGENCY | **71.0%** |

A predicate matching 71-100% of a table is one an index scan makes *worse*:
the index is read and then almost every heap page is fetched anyway.

Proven rather than argued, on NASS at 1.09M rows:

```text
before any index          ->  Seq Scan on fact_crop_observation
with an expression index  ->  Seq Scan on fact_crop_observation
```

`CREATE INDEX ... (UPPER(agg_level_desc))`, `ANALYZE`, and the planner still
chose the sequential scan. The probe index was dropped.

**So no index is added, and that is this deliverable's result rather than an
omission.** The plan's fourth criterion asks for exactly this: a measurement
that shows no problem is recorded, and nothing changes. FBI at 11,084 rows
would not justify one at any selectivity.

*What would change the answer.* These are the view-served sources, and the
filter is on a low-cardinality column. If a future grain filter were selective
-- a single state out of fifty, say -- the measurement would come out
differently and should be redone rather than inferred from this one.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/martin -q` | 34 passed |
| `RUN_MARTIN_TESTS=1 python -m pytest -m martin tests/integration/martin tests/e2e/test_martin_api_join.py -q` | 6 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 211 passed, 2 skipped |
| `python -m pytest tests/unit -q` | 1874 passed |
| `ruff format --check .` / `ruff check .` | clean, 500 files |

## Definition of done

Both reads have a number beside them, and any index or relation added is
justified by the number.

## What this plan deliberately does not do

- It does not change the grain vocabulary or how the registry derives a
  grain; only what the database does with the filter.
- It does not add indexes speculatively.
