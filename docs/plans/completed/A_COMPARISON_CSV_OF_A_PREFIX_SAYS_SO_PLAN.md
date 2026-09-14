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

- **Status:** Accepted 2026-09-14 (Implemented; awaiting review. Authored 2026-09-13 by the assessment agent; claimed and completed 2026-09-13. It was a present defect; WEB-059 is the same fix for the other export. Register row **WEB-067** (WEB-066, suggested at authoring time, had been taken).)
- **Last updated:** 2026-09-14
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

## What changed

- `ComparisonLoad` (`{loaded, total, complete}`) is the shape the workspace
  already computes, and `comparisonExport` takes it as a third argument
  defaulting to `null` — so an export handed no load is read as complete,
  which is what every caller before this did.
- The file is named the way WEB-059 names the explorer's, and the shortfall
  *leads* the caveats column: a bounded read is the first thing a reader of
  the file needs, ahead of what the API published about the pair.
- `ComparisonWorkspace` keeps the load in state beside the payload rather
  than only rendering it, and resets it on a failed load.

## Validation

- `npm --prefix apps/web run test:unit` — **351 passed** (348 before: +3).
  The pinned filename assertion is kept and annotated as the complete-read
  name.
- `npm --prefix apps/web run test:browser` — the new node passes and
  **fails without the wiring**: dropping `comparisonLoad` from the
  `comparisonExport` call leaves it failing on the downloaded file's own
  name. `ComparisonWorkspace.tsx` was restored byte-for-byte afterwards.
  It is the first browser node in this repository to read a download's
  suggested filename, which is the only place the file's name is observable.
- `npx tsc --noEmit` (apps/web) and `npm --prefix apps/web run lint` — clean.
- `python -m tests.support.catalog_evidence` renders WEB-067 `FULL`.

## Remaining work

- None. Review is the remaining step.
