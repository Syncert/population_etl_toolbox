---
id: route-boundaries-and-guarded-storage
branch: claude/route-boundaries-and-guarded-storage
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
---

# A refused local save is reported, and a route can fail without losing the page

## Plan status

- **Status:** Ready for review. Implemented 2026-09-16 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.

## Why

The handoff's discipline for browser storage is stated and mostly followed:
`apps/web/lib/apiToken.ts` and `components/ComposedArticle.tsx` guard every
access "because storage throws outright in a private window rather than
returning null", and every reader of the saved-chart and builder-draft stores
is guarded. Two writers are not:

- `apps/web/lib/savedCharts.js:19`
  `window.localStorage.setItem(SAVED_CHARTS_KEY, JSON.stringify(next));`,
  called unwrapped from `SourceExplorerPage.tsx`, `WorkbenchPage.tsx`,
  `ProfileProduct.tsx` and `ComparisonWorkspace.tsx`. Line 18 also
  `.slice(0, 50)`s the list, silently evicting the oldest saved view.
- `apps/web/components/EvidencePacketBuilder.tsx:232`
  `window.localStorage.setItem(BUILDER_DRAFT_KEY, JSON.stringify(packet));`.

A `QuotaExceededError` or a browser with storage blocked turns "Save view"
into an unhandled exception inside a click handler. And there is nowhere for
it to land: `apps/web/app` contains `layout.js`, `page.js`, styles and one
`page.js` per route, with no `error.js`, `loading.js`, `not-found.js` or
`global-error.js` anywhere (`scripts/bundle-budgets.json` carries Next's
default `/_not-found`). The whole segment falls to Next's generic error
page and the analysis on screen is gone. The handoff's rule that a refused
save is reported, never hidden, is broken for the local destination.

## Deliverables

### 1. Guarded writers with an outcome

Both writers wrap the write, return a result (`saved`, `refused` with the
reason, `evicted` with the count), and the four callers render it in the
existing `.status-row` the way an account save's outcome is rendered
(`describeSaveSuccess` and its refusal counterpart). Eviction at fifty is
stated on the control, not silent.

### 2. Route boundaries

`app/error.js` (a client boundary with a named, recoverable state and a
"back to the selection" action that preserves the URL state), `app/not-found.js`
in the site's own shell, and `app/global-error.js` for a failure in the
root layout. Each keeps `main` and an `h1` so the accessibility audit's
landmark checks hold on the failure page.

### 3. Loading states where a route is server-rendered

If `per-route-metadata` lands and the wrapper pages become server
components, add `loading.js` where a route awaits data; otherwise record
that the pages are client-rendered and need none.

## Acceptance criteria

- [x] A unit test with `setItem` throwing `QuotaExceededError` shows the
      refusal in the status row and leaves the on-screen analysis intact;
      the same for a `localStorage` accessor that throws. Both cases assert
      the `SaveOutcome` the status row renders — `state: "bad"`, the cause in
      words, and "The analysis on screen is unchanged" — and that
      `saveChart` reports the store as it actually is rather than as the
      write intended.
- [x] A unit test saving a fifty-first view reports the eviction
      ("the fifty-first view is saved, and the eviction is reported").
- [x] A browser test that forces a render error on one route shows the
      boundary with `main` and `h1` present and the recovery action working;
      `/no-such-route` renders the site's own not-found page
      (`tests/frontend/browser/route-boundaries.spec.js`, two tests).
- [x] `bundle-budgets.json` is updated deliberately for the new segments —
      and for `/workbench/page`, which this branch pushed over its ceiling.
      See "The bundle budget" below; the numbers are stated there because the
      file cannot carry a comment.
- [x] `TESTING_CONTRACT.md` gains `WEB-` rows for both behaviours: WEB-106
      for the guarded writers, WEB-107 for the route boundaries.

## Implementation evidence

### What changed

- `apps/web/lib/savedCharts.js`: one guarded `write` that both writers go
  through. It returns `{outcome, evicted, reason}` and never throws.
  `storageRefusalReason` names the two cases a browser actually produces —
  `QuotaExceededError` and a `SecurityError`, including one raised by the
  `localStorage` accessor itself before any method is called, which is the
  ordinary state of a private window in several browsers. `saveBuilderDraft`
  joins `saveChart` there, so the packet draft is not a second unguarded
  path. `SAVED_CHART_LIMIT` names the cap that was a bare `50`.
- `apps/web/lib/savedAnalysis.ts`: `describeLocalSave` turns that result into
  the same `SaveOutcome` vocabulary an account save already used, so a reader
  is told which store answered and what it said whichever one they are on. A
  refusal says what is still true — the analysis on screen is unchanged — and
  an eviction is reported as a save that cost the oldest view.
- All five callers report it: `SourceExplorerPage`, `WorkbenchPage`,
  `ComparisonWorkspace`, `ProfileProduct` (whose plainer toast carries the
  same message string) and `EvidencePacketBuilder`.
- `app/error.js`, `app/not-found.js` and `app/global-error.js`. The first two
  render in the site's shell and keep one `main` and one `h1`, because the
  accessibility audit asserts both on every route a reader can be on.
  `global-error.js` carries its own `html` and `body` and no site classes: at
  that point the layout that would have styled it is the thing that failed.

### Deliverable 3: no route needs a loading state

`loading.js` shows while a *server* component awaits. Every route under
`app/` is either `"use client"` and fetches after mount — where the page's own
status surface reports the wait — or one of the three retired routes that
`redirect()` and render nothing. Neither can show a loading state, so none is
added.

`tests/frontend/unit/route-shell.test.js` asserts that rather than leaving it
as a note, and it asserts the right rule: **nothing awaits on the server**,
not "nothing is server rendered". `per-route-metadata` is queued and will turn
these wrappers into server components so they can export `metadata` — a
wrapper that only renders a client component still awaits nothing and still
needs no loading state, so a test that banned server rendering would have
failed that plan for the wrong reason. What needs a loading state is a page
that awaits, and that is what this fails on.

### The bundle budget

Three new segments had no declared budget, which the checker fails on by
design. They were given the file's own formula — 15% above what shipped,
rounded to a kilobyte, exactly what `check-bundle-budget.mjs --update`
writes: `/error` 405 kB, `/not-found` 404 kB, `/global-error` 395 kB.

`/workbench/page` also went over, and this branch is why. Measured on
`origin/main` (built in a worktree for this comparison) it is 472.3 kB
against a 474 kB budget — 1.7 kB of headroom. On this branch it is 474.2 kB:
this plan's `describeLocalSave` and `SAVED_CHART_LIMIT`, and the preceding
formatter plan's `lib/format` and `pairedGeographies`, together added about
1.9 kB and crossed it. The budget did its job. It is rebaselined by the same
formula to 546 kB rather than to a hand-picked number a few bytes above the
measurement, so this route's headroom matches every other route's; a
reviewer who would rather have the tighter number should say so, because the
file cannot record the choice itself.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 568 passed, 36 files (was 562, 35) |
| `npm --prefix apps/web run test:browser` | 117 passed in 3.2m (was 115); see the note below |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run build` | succeeded |
| `npm --prefix apps/web run check:bundle` | every route within its declared budget |
| `npm --prefix apps/web run check:csp` | passed, 0 prerendered routes |
| `python -m pytest tests/unit -q` | 1802 passed |

The browser tier needs `PLAYWRIGHT_CHROMIUM_EXECUTABLE` pointed at the
container's pre-installed Chromium; see the note in the formatter plan.

The first full run of this tier spanned an edit: four import statements were
reordered into alphabetical order while it was in flight, and the tier runs
against `next dev`, which recompiles on change. It reported 117 passed, and
the edit cannot change behaviour -- but a result taken across a recompile is
not a result. The 117 recorded above is from a second run on a settled tree.

## Definition of done

No write to browser storage can throw into a handler, every refusal is
stated on the control that caused it, and a route that fails shows the
site's own page with a way back.

## What this plan deliberately does not do

- It does not change the account save path or its destination rule.
- It does not add retry or automatic fallback to the browser store; the
  handoff forbids rewriting a refused save to another destination.
