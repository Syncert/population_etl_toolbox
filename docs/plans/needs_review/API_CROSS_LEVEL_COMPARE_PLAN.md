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

Needs review (v1.0 build-out, item A-4), built 2026-10-31 on branch `feat/api-cross-level-compare` from
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

## Decisions taken here (within ADR-0010)

- **Draft kinds are used, marked.** Every kind is `draft` until Nick reviews
  it, like the plain labels (#170). The answer names the kind and its status
  (`measure_kind_status`) and adds a caveat; `METRIC_LABELS_REVIEWED_ONLY=1`
  makes an unreviewed kind fall back to `count`, so the strict reading of the
  ADR is one switch away. Kinds were assigned by a rule over each entry's
  name and unit (median by name; index by unit; percent and per-N units are
  rates; per-capita and average figures are means; the rest are counts): 87
  counts, 10 rates, 5 medians, 5 means, 3 indexes.
- **The newest common period is the default.** Picking the newest period both
  places publish is not silent alignment: the answer says `period_basis:
  newest_common`. With no common period each side shows its own newest with
  `periods_differ` and no difference.
- **Same-level counts are served.** ADR-0010 refuses counts across levels;
  two counties' counts are what the existing compare page shows, so they are
  served with a `counts_differ_in_size` note pointing at the per-capita view.
- **No difference for overlapping places.** A city and a county or metro it
  `intersects` partly describe the same people, so like nesting they are shown
  side by side without a difference.
- **Interval overlap assumes independent samples.** For a place inside its
  container the two estimates share respondents; the overlap is still
  reported as the ADR asks, with the containment note beside it.
- **The nation contains every place.** The reference records edges up to the
  grandparent only, so a tract or city beside the nation is answered by level.

## Checkpoint

- [x] kind in registry, schema and loader
- [x] service and route (`GET /api/v1/comparison/places`), merged in #180
- [x] tests (API-175, API-176), consumer guide, OpenAPI snapshot
- [x] warehouse checks on OFFICE_LAB (API rebuilt from `dev` at 59e6a25)
- [x] a side with no value refuses the comparison (follow-up PR)

Ready for Nick's review: the draft `kind` of each definition.

## Evidence

- 2026-10-31, cloud session: `python -m pytest tests/unit/api tests/unit/semantics
  tests/unit/shared -q` -> 1342 passed at #180, 1380 after merging `dev`;
  `ruff format --check .` and `ruff check .` (ruff 0.15.16) clean. #180 merged
  with all 30 CI checks green.
- 2026-10-31, OFFICE_LAB, API image rebuilt from `dev` at 59e6a25 (relayed by
  the coordinator):
  1. Austin city (`state:48|place:05000`) vs Travis County
     (`state:48|county:453`), `CENSUS_ACS:acs5:B19013_001`: **pass**. 200,
     `comparable: true`, `period_basis: newest_common`, 93,658 beside 99,611,
     both 2020-2024; `containment: intersects`, `smaller_side: a`,
     `share_of_smaller_inside_larger: 0.9187`, vintage 2025, evidence
     `census_boundary_intersection`; `difference: null`.
  2. Dane County (`state:55|county:025`) vs Wisconsin (`state:55`),
     `CENSUS_ACS:acs5:B17001_002`: **pass**. 200, `comparable: false`,
     `counts_differ_in_size`, `alternatives` lists `poverty_share:acs5` (drafts
     served on the local stack); `containment: part_of`, share 1.0, evidence
     `exact_census_code_hierarchy`.
  3. Austin metro (`cbsa:12420`) vs the nation (`us:1`), `BLS:LAU:UNEMP_RATE`:
     **partial**. 200, `comparable: true` with side a 3.7 (2026-09) and side b
     empty, named `us:1`, `period_basis: each_newest`. BLS LAUS publishes no
     national series (the national rate is CPS `LNS14000000`). Fixed in the
     follow-up: a side with no value refuses the comparison with
     `not_published` ("there is no published value for United States in this
     series"), and the nation is named "United States" where the catalog's
     name is its code. Mapping `us:1` to the CPS series is not done: it is a
     different survey, so it would be a different measure.
- A-3 follow-up #181 on OFFICE_LAB: SAIPE and `birth_rate` answered for 254 of
  254 Texas counties with the state filter. The uncached `EXPLAIN` for #181 is
  still outstanding there.
