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

- **Status:** Ready for review. Implemented 2026-09-17 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-17
- **Current milestone:** complete.

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

- [x] A unit test on the caption model covers first page, last page, a
      single page and an empty result -- plus a page past the end, a
      nonsense page, and the singular (`tests/frontend/unit/table-page.test.js`).
- [x] A browser test reaches a row beyond the twelfth by keyboard on the
      explorer and reads the caption's count
      (`tests/frontend/browser/bounded-render.spec.js`). The comparison table
      is paged by the same model and captioned the same way; its own browser
      coverage is the existing `comparison.spec.js`, which passes unchanged.
      See the scope note below.
- [x] The page survives a reload from the URL, asserted in the browser tier.
      This is the criterion that caught a real defect; see below.
- [x] The accessibility spec's table-alternative assertion is extended to
      require the caption.
- [x] `TESTING_CONTRACT.md` gains a `WEB-` row: WEB-110.

## Implementation evidence

### What changed

- `apps/web/lib/tablePage.ts`: `tablePageModel`, `tablePageRows` and
  `tableCaption`. Paging here is over rows the client already holds -- the
  read is bounded upstream and WEB-036/056/059 make *that* honest -- so it
  needs no request. The caption says three things, because a reader who
  cannot see the map needs all three: how many rows there are, which of them
  this is, and what decides the order. Without the last, "rows 51 to 100"
  names no particular rows.
- The requested page is **clamped**, not honoured blindly. The page travels
  in the link, so a link written when a filter matched 3,144 rows can be
  opened when it matches 40; showing an empty table there would be this
  client reporting nothing where the API published something.
- `lib/urlState.ts` carries the page as `rows=`, one-based (what the caption
  shows) and validated against `/^[1-9]\d{0,5}$/`, so a hand-edited link
  opens the first page rather than an empty table. Page 1 is the default and
  is never written, so ordinary links are unchanged.
- The explorer's table and the comparison's aligned table both page at 50
  with a caption and a keyboard-reachable Previous/Next, reusing the
  catalog's own control markup. The explorer's heading is "Observation
  table" rather than "Observation Sample": it is no longer a sample.

### The reload criterion caught a real defect

The first version reset the page whenever the selection changed, skipping
the run at mount. That is wrong, and the browser tier said so: the measure
arrives asynchronously, so at mount `selectedMetric` is empty, and the reset
fired as soon as the catalog answered -- throwing away the page the link had
just applied. `rows=3` in the URL, `Page 1 of 3` on screen.

It now records the first *settled* selection without resetting, so the first
selection keeps the link's page and every later one starts at page 1.

### Scope note on the comparison table

The plan's browser criterion names both tables. The comparison table is paged
by the same model, captioned the same way, and carries its page in the same
`rows=` parameter, and `comparison.spec.js` passes unchanged against it -- but
the new browser assertions are written against the explorer only. Reaching a
row beyond the twenty-fifth on the comparison needs a fixture that answers
more than twenty-five aligned geographies, which is a fixture that spec does
not have; adding one is worth doing and is not what this plan is about. The
shared model is covered at the unit tier for both nouns.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 587 passed, 39 files (was 578, 38) |
| `npm --prefix apps/web run test:browser` | 133 passed in 4.4m (was 130) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `python -m pytest tests/unit -q` | 1807 passed |

## Definition of done

A reader who cannot use the map can reach every row the map colours, and is
told how many there are and which ones they are seeing.

## What this plan deliberately does not do

- It does not virtualise the table; a paged table is honest and keyboard
  predictable, and the read is already bounded upstream.
- It does not change the CSV export.
