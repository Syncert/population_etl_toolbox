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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] A browser test asserts `document.title` per route, and that an explorer
      link with a source and measure titles the tab with them.
- [ ] A unit test asserts the title builder rejects or omits any key outside
      the public URL state vocabulary and never renders a value.
- [ ] `/robots.txt` and `/sitemap.xml` are served and exclude `/saved` and
      `/builder`.
- [ ] `check:csp` and the CSP browser spec still pass (the wrappers are
      server components now; the nonce path is unchanged).
- [ ] `TESTING_CONTRACT.md` gains a `WEB-` row.

## Definition of done

A shared link carries a title that says what it shows, and nothing private
ever reaches one.

## What this plan deliberately does not do

- It does not add OpenGraph images or server-side rendering of user content,
  which the handoff lists as a non-goal.
