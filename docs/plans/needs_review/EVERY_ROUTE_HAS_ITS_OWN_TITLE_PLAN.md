---
id: per-route-metadata
branch: claude/per-route-metadata
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every route has its own title

## Plan status

- **Status:** Ready for review. Implemented 2026-09-16 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.

## Why

The product's core value is a shareable, reproducible link: the handoff says
"public state travels in URLs by design", and `apps/web/lib/urlState.ts`
serialises the explorer, comparison, catalog, profile and workbench state.
Every one of those links lands in a tab, a bookmark, a history entry or a
social preview titled "Economic Data Studio".

`apps/web/app/layout.js:13` holds the only `metadata` export, with a
`%s | ...` template nothing uses. No route exports `metadata` or
`generateMetadata`, and there is no `robots` or `sitemap`. The reason is
mechanical: every `page.js` under `apps/web/app` is `"use client"` (ten
files), and a client component cannot export metadata. Eight of them are
seven-line wrappers around one component and would become server wrappers
with no other change.

## Deliverables

### 1. Server wrappers with metadata

Remove `"use client"` from the wrapper pages (`explore`, `compare`,
`profiles`, `quality`, `saved`, `builder`, `articles`, `workbench`) and
export a `metadata` per route with a title that names the screen; the
component they render keeps `"use client"`.

### 2. Titles that name the analysis

`generateMetadata` on `/explore`, `/compare`, `/workbench` and `/profiles`
reads `searchParams` through the same parsers `urlState.ts` uses and titles
the page with source and measure (and geography where the URL carries one).
The title never contains a value, a saved-analysis name or id, or anything
else the handoff's privacy boundary keeps out of the address bar; `/saved`
and `/builder` get fixed titles.

### 3. `robots` and `sitemap`

`app/robots.js` allowing the public routes and disallowing `/saved` and
`/builder`; `app/sitemap.js` listing the public routes only.

## Acceptance criteria

- [x] A browser test asserts `document.title` per route, and that an explorer
      link with a source and measure titles the tab with them
      (`tests/frontend/browser/route-titles.spec.js`, one test per route).
- [x] A unit test asserts the title builder rejects or omits any key outside
      the public URL state vocabulary and never renders a value
      (`tests/frontend/unit/route-titles.test.js`), and the browser tier
      asserts the same where it would actually leak -- the rendered document.
- [x] `/robots.txt` and `/sitemap.xml` are served and exclude `/saved` and
      `/builder`, from one shared constant so they cannot contradict.
- [x] `check:csp` and the CSP browser spec still pass. The check needed one
      scoped change; see below.
- [x] `TESTING_CONTRACT.md` gains a `WEB-` row: WEB-108.

## Implementation evidence

### What changed

- `apps/web/lib/routeTitles.ts`: `STATIC_ROUTE_TITLES` for the routes whose
  address carries no state, and `explorerTitle`, `comparisonTitle`,
  `profileTitle` and `workbenchTitle` for the four that do. Each takes the
  **parsed** state from `urlState.ts`, never the raw query, which is what
  makes the privacy rule structural rather than a filter someone has to keep
  updated: a key the vocabulary does not define has nowhere to arrive from.
  A workbench composition stops naming series after the second and counts the
  rest, because eight metric codes is a title every surface truncates in an
  arbitrary place.
- The eight wrapper pages lost `"use client"` and became server components.
  The four that read state export `generateMetadata`; the four that do not
  export a static `metadata`. Every component they render is unchanged and
  still a client component.
- `apps/web/app/catalog/layout.js`: `/catalog` is not a thin wrapper -- it is
  the screen itself -- and a client component cannot export metadata. A
  segment layout can, and one that renders only its children changes nothing
  about how the page renders. The alternative was splitting a working client
  page in two to give it a title.
- `apps/web/lib/siteMap.ts`, `app/robots.js`, `app/sitemap.js`: the published
  and private route lists stated once, so a sitemap cannot name a route
  `robots.txt` disallows.

### The CSP check needed one scoped change

`robots.txt` and `sitemap.xml` are prerendered, and `check:csp` failed: its
first rule was "nothing is prerendered". That rule exists because a
prerendered *page* is HTML written at build time, so its script tags carry no
per-request nonce. A `text/plain` and an `application/xml` route have no
script tag to stamp, and forcing them to render per request to satisfy a
check about script nonces answers the letter of the rule against its reason.

The rule now reads the route's own recorded content type and applies to HTML
documents. A route recording no content type is treated as HTML, so the
exemption cannot widen by omission, and the check's independent
static-HTML-on-disk assertion is untouched.

**Proved rather than assumed.** Commenting out `export const dynamic =
"force-dynamic"` in `app/layout.js` -- the regression the check names in its
own failure message -- still fails it, on both assertions, naming all ten
prerendered documents:

```
CSP nonce check failed: 10 route(s) are prerendered and would ship without a
nonce: /bls, /census, /fred, /, /builder, /articles, /saved, /_not-found,
/quality, /catalog.
CSP nonce check failed: static HTML was emitted for app routes: ...
```

### One test from a sibling plan needed the same scoping

`tests/frontend/unit/route-shell.test.js` (from `route-boundaries-and-guarded-storage`,
in `needs_review/`) asserts that no route awaits on the server, so none needs
a `loading.js`. That plan's deliverable 3 named this plan as the thing that
would change the answer, and it did: in Next 15 `generateMetadata` reads
`await searchParams`.

`searchParams` is not data. Next already holds it, it resolves at once, it
suspends nothing, and no loading state can result. The rule now ignores
`await searchParams` and `await params` and flags every other await --
verified by making one page `export default async` with a real `fetch`, which
fails it naming that route. So the answer to deliverable 3 is unchanged: no
route needs a loading state, and the reason is now stated precisely.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 576 passed, 37 files (was 568, 36) |
| `npm --prefix apps/web run test:browser` | 130 passed in 3.2m (was 117) |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run build` | succeeded; `/robots.txt` and `/sitemap.xml` served |
| `npm --prefix apps/web run check:csp` | passed, 0 prerendered documents |
| `npm --prefix apps/web run check:bundle` | every route within its declared budget |
| `python -m pytest tests/unit -q` | 1807 passed |

`bundle-budgets.json` declares the three segments the build gained
(`/catalog/layout`, `/robots.txt/route`, `/sitemap.xml/route`) at the file's
own formula, 15% above what shipped.

## Definition of done

A shared link carries a title that says what it shows, and nothing private
ever reaches one.

## What this plan deliberately does not do

- It does not add OpenGraph images or server-side rendering of user content,
  which the handoff lists as a non-goal.
