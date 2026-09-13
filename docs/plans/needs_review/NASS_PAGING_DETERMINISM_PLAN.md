---
id: nass-paging-determinism
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# The USDA NASS explorer pages a total order

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the hand-written source-explorer routers.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/usda_nass_service.py`
- **Depends on:** nothing. API-074 declared paging orders for the four
  registry-generated source routers; these two routes are hand-written and
  were outside it.
- **Next pickup:** none.

## Context

The repository states this rule in its own code. `cdc_queries.py`:

```python
# Deterministic paging order; observation_sk breaks any remaining tie.
_ORDER_BY = """
    ORDER BY asset_id, measure_id, value_type_id, geo_id,
             period_start, period_end, stratum_id, observation_sk
"""
```

The two USDA NASS list routes do not follow it.

| Route | Orders by | Rows it cannot separate |
| --- | --- | --- |
| `/api/v1/usda-nass/observations` | `product_id, release_watermark, short_desc, geo_id, year` | two rows differing only in `domaincat_desc` or `freq_desc` |
| `/api/v1/usda-nass/series` | `product_id, short_desc, geo_id` | the same, plus `class_desc` and the practice descriptors |

Those ties are not an edge case; they are what the resource is for. The
router's own docstring says "the Quick Stats grain is multidimensional, so
these endpoints filter on the provider's own classification rather than on a
single opaque metric code" — and `domaincat_desc` is one of the dimensions
that classification carries. A commodity published across several domain
categories produces several rows with one `short_desc`, and PostgreSQL
promises nothing about which of them a page boundary keeps.

Both relations already carry a unique column:

- `gold_nass.crop_observation` (and `latest_release_observation`, which is
  `SELECT observation.*` over it) carries `observation_sk`, the `BIGSERIAL`
  primary key of `silver_nass.fact_crop_observation`.
- `gold_nass.crop_series` computes `series_id` as an MD5 over the exact tuple
  it groups by, so it is unique per row by construction.

So the fix is the one CDC already made: name the identity that breaks the
tie.

## Objective

Both NASS list routes page an order no two rows of one response can tie on.

## Acceptance criteria

1. `/usda-nass/observations` orders by its current list plus `observation_sk`.
2. `/usda-nass/series` orders by its current list plus `series_id`.
3. The leading order is unchanged, so the shape of a page a caller already
   sees is the same — only ties resolve deterministically.
4. A guard proves both statements end in a column that is unique in the
   relation they read, rather than leaving it to a reviewer to notice.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the filters, the projection, or the `latest` release selection.
- Revisiting the CDC order, which already ends in `observation_sk`.
- Re-ordering the measures route, which is `ORDER BY source_dataset,
  display_name` over an export with one row per measure and is not paged.

## Evidence

### The gap, established first

`test_nass_list_routes_page_a_total_order` is parameterised over both routes
and failed 2/2: it renders each route's list statement, takes the text
between `ORDER BY` and `LIMIT`, and asserts it ends in the column that is
unique in the relation that statement reads. Neither did.

The uniqueness claim is not asserted by the test — it is read from
`sql/migrations/012_usda_nass_crop_pipeline.sql`, where
`silver_nass.fact_crop_observation` declares `observation_sk BIGSERIAL
PRIMARY KEY` and `gold_nass.crop_series` computes `series_id` as an MD5 over
the same tuple its `GROUP BY` names. The test records both, with the reason,
in `NASS_TIE_BREAKERS` beside the assertion.

### What changed

Two ORDER BY clauses gained a final column, with the comment CDC's queries
already carry. Nothing else: the leading order, the filters, the projection,
and the `latest` release selection are untouched, so a page a caller already
sees keeps its shape and only ties resolve.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_usda_nass_api.py -q` | 16 passed |
| `pytest tests/unit -q` | 1382 passed |
| `python -m tests.support.catalog_evidence` | 306-row register renders; API-080 is `FULL` |
| `ruff check .` / `ruff format --check .` | clean |

The reviewed OpenAPI snapshot is unchanged, as it should be: an ORDER BY is
not part of the declared operation shape, though the ordering it produces is
part of the contract the consumer guide states.

### Not run

`make test-integration` needs PostgreSQL, and this environment has no Docker
daemon. What it would add is that the two named columns exist on the served
relations; that was verified instead against the checked-in migration that
creates them, which is where the test's recorded reasons come from.

## Remaining work

None.
