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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect; WEB-038's rule unapplied on the comparison screen.**
- **Last updated:** 2026-09-13
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
