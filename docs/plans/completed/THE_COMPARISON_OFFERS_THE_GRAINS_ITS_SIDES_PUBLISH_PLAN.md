---
id: the-comparison-offers-the-grains-its-sides-publish
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-comparison-csv-of-a-prefix-says-so]
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The comparison offers the grains its two sides publish, not a client-authored three

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row WEB-074.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/components/ComparisonWorkspace.tsx`,
  `apps/web/lib/comparison.ts`

## Context

`ComparisonWorkspace.tsx:686-688` hard-codes `NATIONAL`, `STATE`, `COUNTY`
as the grain options and ignores each side's published
`valid_geo_grains`. `parseComparisonState` accepts all five words
(`urlState.ts:228-231`) and the workspace assigns the parsed value into the
selection (`:155`), so a `?geo_level=PLACE` link puts a value in the select
that no option carries: the control shows one grain while
`comparisonRequestParams` sends another. The analysis routes serve Census
PEP, which publishes `PLACE`; this is reachable data.

## Findings

- `comparison.spec.js:425-436` round-trips only `STATE`.

## Acceptance criteria

1. The grain options are the intersection of the two sides' published
   grains, read from the capability map, and say why a grain is absent (the
   explorer's WEB-038 wording).
2. A parsed grain neither side publishes is reported, not silently held.
3. Failing-first browser coverage round-trips `PLACE` and `AGENCY`.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-068 at authoring time).

## Non-goals

- Comparing across grains.

## Validation

- `comparisonGrainOffer` in `apps/web/lib/comparison.ts` is the whole rule:
  the offered levels are the **intersection** of both sides' published
  `valid_geo_grains`, in the published vocabulary's order, because a
  comparison is answered at one grain — a level only one side publishes is
  one the pair cannot be read at. It reuses the explorer's own reading of a
  declaration (`metricSupportedGeoLevels`, `normalizeGeoLevel`) and the
  vocabulary and labels WEB-064 declared (`GEO_GRAIN_ORDER`,
  `GEO_GRAIN_LABELS`), so there is no second list of grain words.
- A measure that declares no grains keeps the whole vocabulary: silence is
  unknown, not none — the explorer's WEB-038 rule, stated in the helper and
  asserted.
- The narrowing note uses the explorer's wording ("this is the publishers'
  declaration, not a limit of this screen"), and a pair with no grain in
  common says exactly that rather than showing an empty select.
- Criterion 2: a parsed grain neither side publishes is **reported and
  replaced**, not held. `grainNotice` survives the replacement — the
  selection must not build a request for a grain the pair cannot be read at,
  and the reader still has to be told what the link asked for. Choosing a
  level clears the notice.
- The offer is computed from the selected metric rows, independently of the
  compatibility verdict, so an agency-grain pair the analysis routes decline
  is still offered its own grain. Both facts survive together.
- Fixtures (WEB-043): the comparison spec's metric rows carried **no**
  `valid_geo_grains` at all, so the screen could not have been tested for the
  narrowing. They now carry what each source publishes — ACS national/state/
  county, PEP those plus places, CDC counties, and a new FBI UCR source and
  measure at the agency grain, declined by the analysis routes like CDC.
- New tests:
  - six `tests/frontend/unit/comparison.test.js` nodes over the helper: the
    intersection and its note, a PLACE round trip, a reported unavailable
    grain, an agency-only pair, a pair with nothing in common, and the
    unknown-declaration case.
  - three `comparison.spec.js` nodes: `/compare?geo_level=PLACE` on an
    ACS/PEP pair offers `National, State, County`, reports Place, shows
    County and never requests PLACE; a PEP/PEP link at PLACE offers all four,
    keeps `geo_level=PLACE` in the address bar and in the issued request; an
    FBI/FBI link at AGENCY offers `Agency` alone and still reports
    `data-comparable="false"`.
- One existing node needed the fixtures to be right: it selects NATIONAL to
  prove a national comparison has no geometry, which only works because both
  sides publish national totals — they now say so.
- Break-test: restoring the three hard-coded options and dropping the
  replacement leaves `3 failed, 12 passed` in `comparison.spec.js` — exactly
  the three new nodes.
- Tiers: frontend units 372 passed; browser tier 93 passed in 1.0m against a
  fresh production build; `pytest tests/unit` 1560 passed; `npm run lint` and
  `tsc --noEmit` clean.

## Remaining work

- None.
