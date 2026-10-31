---
id: api-cross-level-compare
branch: feat/api-cross-level-compare
depends_on:
  - api-per-capita-and-shares
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api tests/unit/semantics tests/unit/shared -q
  - ruff format --check . ; ruff check .
---

# Compare one measure across two places at any level

## Status

To do (v1.0 build-out, item A-4), branch `feat/api-cross-level-compare` from
`dev` after A-3 (#175). Governed by
[ADR-0010](../../decisions/0010-cross-level-comparison.md), accepted by Nick
on 2026-10-26, and [ADR-0009](../../decisions/0009-per-capita-and-share-views.md)
for the per-capita steer, so no new ADR is needed. Unblocks D-7 (the web's
cross-level compare page).

## Why

The comparison API compares two measures across many geographies of one
grain. Readers ask the other question: one measure, two places, possibly at
different levels -- Austin against Travis County, Dane County against
Wisconsin, the Austin metro against the nation. The web answers that today
by fetching rows client-side, refuses county against state, and shows city
against county counts side by side as if they were comparable.

## Deliverables

1. `kind` on every entry of `docs/semantics/metric_definitions.json`
   (`count`, `median`, `mean`, `rate`, `share`, `index`), in the schema and
   the loader. Entries stay `draft` until Nick reviews them (ADR-0010 rule 2).
2. `GET /api/v1/comparison/places?metric_code=&geo_id_a=&geo_id_b=` (or
   `ratio_id=` instead of `metric_code` for an ADR-0009 derived value, and an
   optional `metric_code_b` for a different code on side b), answering:
   - both sides' values with their period, release, margin of error and 90
     percent interval, or "not published", never zero;
   - `comparable` and, when refused, a `refusal` with its reason and what
     would answer the question: `counts_differ_in_size` (with the served
     ADR-0009 pairings for that count), `programs_differ`,
     `universes_differ`, `units_differ`, `different_measures`;
   - the period basis: the newest period both sides publish, else each side's
     newest named with `periods_differ` (no difference computed across
     periods);
   - containment from the reference's relationships (`contains`, `part_of`,
     `intersects` with the share of the smaller place inside the larger),
     with no difference computed between a place and its container;
   - whether the two 90 percent intervals overlap; no verdict words.
3. Unit tests, behavioural catalog rows, consumer guide section, OpenAPI
   snapshot.

## Checkpoint

- [ ] kind in registry, schema and loader
- [ ] service and route
- [ ] tests, catalog rows, guide, snapshot
- [ ] warehouse check relayed to OFFICE_LAB (unverified until then)
