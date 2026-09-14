---
id: the-geography-projection-is-its-own-refresh
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest tests/integration/api/test_catalog_serving_agreement.py -m "integration and database"
  - pytest tests/unit/api/test_consumer_guide.py
---

# The geography catalog is its own refresh, and says so

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/reference/API_CONSUMER_GUIDE.md`

## Context

The guide is careful about one independently-refreshed surface:

> Where `/observations/latest` answers from the durable history — the latest
> view refreshes independently, so an empty page there means "not refreshed
> yet", not "no such data" — …

`/catalog/geographies` is the same kind of surface and the guide says nothing.
Its whole entry is one table cell: "Geography identities and attribution".

It reads `gold_glossary.dim_geo_latest`, which is filled by
`gold_glossary.refresh_dim_geo_latest()`, and that procedure is called from
one Airflow task whose own docstring names the property:

```python
    @task
    def refresh_shared_geography() -> None:
        """Refresh the glossary-owned geography projection independently."""
        hook.run("CALL gold_glossary.refresh_dim_geo_latest()")
```

It runs in the glossary *reconciliation* DAG, after the publisher harvest and
on that DAG's own schedule — not with the source publishers that make
observations available.

Read off the live API against a real warehouse, one geography, two answers:

```
GET /observations?metric_code=CENSUS_ACS:acs5:B99997_…
  items[0] = { "geo_id": "state:95", "geo_level": "STATE", "value": "1234" }
GET /comparison?…&geo_level=STATE
  items[0] = { "geo_id": "state:95", "state_name": "Catalog agreement state" }
GET /catalog/geographies?q=95
  { "total": 0, "items": [] }
```

Both answers are correct. The observation surface attributes the geography
from the relation it serves; the projection has not been refreshed. What is
missing is that a consumer is told — and the same consumer is told, for the
sibling resource, in the same document.

This is not hypothetical for a reader: `apps/web` builds its state and county
pickers from `/catalog/geographies`. A geography with published observations
and no projection row yet is a place a person searches for, does not find,
and is given no reason about.

## Acceptance criteria

1. The guide says `/catalog/geographies` answers a projection refreshed on its
   own schedule, in the same terms it already uses for `/observations/latest`:
   an absent geography there means "not projected yet", not "no such
   geography".
2. The behaviour is pinned behaviourally, against a real warehouse: one
   geography that `/observations` serves and attributes, and that
   `/catalog/geographies` does not list. The node exists so that a future
   change making the catalog read the durable relation *fails* — at which
   point the caveat should go too, deliberately, rather than being left
   behind as a stale warning.
3. The map's silence on such a geography is named where the guide already
   discusses the tile boundary, if it discusses it; otherwise not invented
   here.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-107).

## Non-goals

- Serving a `refreshed_at`. The projection's `ON CONFLICT … DO UPDATE …
  WHERE (…) IS DISTINCT FROM (…)` guard leaves an unchanged row's timestamp
  alone, so `MAX(refreshed_at)` is when the projection last *changed*, not
  when it was last refreshed. Publishing that under a name like `refreshed_at`
  would be a new wrong answer, not a fix.
- Making the catalog read the durable relation. That is a design change with
  its own consequences (the projection is what carries geometry for the tile
  layer), not a documentation defect.

## Validation

**Found by reading answers off a live API**, not by reading code: an ad-hoc
probe over the real warehouse with the ACS publisher fixture, printing nine
routes' answers side by side. `/observations` and `/comparison` both answered
for `state:95` — the second with its state name — and
`/catalog/geographies?q=95` answered `total: 0`. Checking why led to
`refresh_dim_geo_latest()` and the one Airflow task that calls it, whose
docstring already says what the guide does not.

**Two answers, both correct.** The node asserts all three facts together, so
it reads as the design rather than as a bug: the observation surface serves
the geography *and* attributes it, and the projection does not list it. Its
failure message says what to do if it ever fails — remove the guide's caveat
deliberately, rather than leave a stale warning behind — because a change
making this resource read the durable relation would be legitimate.

**What is deliberately not delivered.** Serving a `refreshed_at` was
considered and declined: the procedure's `ON CONFLICT … DO UPDATE … WHERE (…)
IS DISTINCT FROM (…)` guard leaves an unchanged row's timestamp alone, so
`MAX(refreshed_at)` is when the projection last *changed*, not when it was
last refreshed. Publishing that under that name would be a new wrong answer.
Recorded in the plan's non-goals so the next reader does not have to
re-derive it.

**The consequence the guide now names.** The projection is also what carries
the geometry the tile layer publishes, so a geography it does not hold yet
has values and no shape — which is why "not listed" has to read as a
statement about the projection rather than about the warehouse.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1488 passed |
| Unit, the guide | `pytest tests/unit/api/test_consumer_guide.py` | 7 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | **142 passed**, 2 skipped (was 141) |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

**Register.** 363 rows.

## Remaining work

- None.
