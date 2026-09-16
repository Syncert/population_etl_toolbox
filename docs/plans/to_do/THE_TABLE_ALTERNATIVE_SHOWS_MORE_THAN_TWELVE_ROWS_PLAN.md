---
id: bounded-render-honesty
branch: claude/bounded-render-honesty
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
---

# The table alternative shows more than twelve rows, and says how many

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

The explorer's table is the map's accessible alternative. `README.md`
("Supported browsers") says every value the map would show "remains
available in the observation table and the CSV export", and
`tests/frontend/browser/accessibility-operations.spec.js` treats the table
as that alternative. The table shows twelve rows:
`apps/web/components/SourceExplorerPage.tsx:2589` renders
`observations.slice(0, 12)` under the heading "Observation Sample" with no
caption, count or paging; `grep "12 of\|rows shown"` finds nothing. The
comparison table does the same at `components/ComparisonWorkspace.tsx:991`
with `rows.slice(0, 25)`. A national county map has 3,144 values; a
keyboard or screen-reader user reaches twelve of them without downloading a
CSV.

WEB-036, WEB-056 and WEB-059 made bounded *reads* honest (a partial read is
named, never presented as the whole). The bounded *render* is not, and it
contradicts the handoff's rule that "a presentation is offered only where it
can answer".

## Deliverables

### 1. A caption and a page control on the explorer table

The table states "Showing 1–50 of 3,144 rows" (the count from the same
paging model that already knows it) and pages with the same
Previous/Next control the catalog uses (`apps/web/app/catalog/page.js`),
keyboard-reachable, with the page in the URL state so a shared link opens
the same page. Keep the CSV export as the whole-set path and say so beside
the caption.

### 2. The same on the comparison table

The 25-row cap becomes a paged table with a caption.

### 3. The sort is declared

The table's order is the API's declared order for the resource (the
guide's paging tables), stated in the caption, so a reader knows which
twelve (or fifty) they are looking at.

## Acceptance criteria

- [ ] A unit test on the caption model covers first page, last page, a
      single page and an empty result.
- [ ] A browser test reaches a row beyond the twelfth by keyboard on the
      explorer and beyond the twenty-fifth on the comparison, and reads the
      caption's count.
- [ ] The page survives a reload from the URL.
- [ ] The accessibility spec's table-alternative assertion is extended to
      require the caption.
- [ ] `TESTING_CONTRACT.md` gains a `WEB-` row.

## Definition of done

A reader who cannot use the map can reach every row the map colours, and is
told how many there are and which ones they are seeing.

## What this plan deliberately does not do

- It does not virtualise the table; a paged table is honest and keyboard
  predictable, and the read is already bounded upstream.
- It does not change the CSV export.
