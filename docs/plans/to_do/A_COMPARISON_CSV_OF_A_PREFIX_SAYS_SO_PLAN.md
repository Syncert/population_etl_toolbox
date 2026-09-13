---
id: a-comparison-csv-of-a-prefix-says-so
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A comparison CSV of a page-bounded read says it is a prefix

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect; WEB-059 for the other export.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/ComparisonWorkspace.tsx`,
  `apps/web/lib/comparison.ts`

## Context

WEB-059 made the explorer's file of a bounded read name itself
`…-partial-40000-of-51234.csv` and say so inside. The comparison workspace
holds `pages.complete` and `pages.total`, states the shortfall in its pill
(`ComparisonWorkspace.tsx:353-367`), then drops both: `comparisonExport`
(`comparison.ts:340-401`) takes no completeness, names the file
`comparison-A-vs-B.csv`, and its `caveats` column carries only the API's
caveats. `viewModes.export.supported` is `rowCount > 0`, so the button is
enabled on a truncated answer.

## Findings

- A grain aligning more than `1000 × COMPARISON_PAGE_LIMIT` geographies:
  the pill reads "loaded 8,000 of 12,400 ... this comparison is
  incomplete"; the file says nothing, and the file outlives the pill.
- `comparison.test.js:323` pins the unconditional filename;
  `comparison.spec.js:571` asserts only that the pill is not green.

## Acceptance criteria

1. The export receives the completeness the pill reads, names a partial
   file as WEB-059 does, and carries the shortfall in its caveats.
2. Failing-first unit and browser coverage; the pinned filename assertion
   is corrected.
3. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-066 at authoring time).

## Non-goals

- Fetching more pages. The bound is the API's.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
