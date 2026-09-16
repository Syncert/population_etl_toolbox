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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] A unit test with `setItem` throwing `QuotaExceededError` shows the
      refusal in the status row and leaves the on-screen analysis intact;
      the same for a `localStorage` accessor that throws.
- [ ] A unit test saving a fifty-first view reports the eviction.
- [ ] A browser test that forces a render error on one route shows the
      boundary with `main` and `h1` present and the recovery action working;
      `/no-such-route` renders the site's own not-found page.
- [ ] `bundle-budgets.json` is updated deliberately for the new segments.
- [ ] `TESTING_CONTRACT.md` gains `WEB-` rows for both behaviours.

## Definition of done

No write to browser storage can throw into a handler, every refusal is
stated on the control that caused it, and a route that fails shows the
site's own page with a way back.

## What this plan deliberately does not do

- It does not change the account save path or its destination rule.
- It does not add retry or automatic fallback to the browser store; the
  handoff forbids rewriting a refused save to another destination.
