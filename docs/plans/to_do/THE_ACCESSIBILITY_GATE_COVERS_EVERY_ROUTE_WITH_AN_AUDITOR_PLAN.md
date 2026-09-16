---
id: accessibility-axe-gate
branch: claude/accessibility-axe-gate
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:browser
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:unit
---

# The accessibility gate covers every route with an auditor

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

WEB-025 promises that "every core workflow" is audited for landmarks,
headings, form-control names and live regions, and
`tests/frontend/browser/accessibility-operations.spec.js` does that by hand
for the routes in its `CORE_ROUTES` list. The list has nine routes and does
not include `/workbench`, which `analytics-workbench` added later;
`workbench.spec.js` checks none of the landmark, heading or status-row
rules. The form-control naming check runs on four routes and inspects
`select`, `input` and `textarea`, never a button or a link.

No automated auditor runs at all: `axe` appears under `tests/frontend` only
as chart axes, and `@axe-core/playwright` is not in `apps/web/package.json`.
Hand-written assertions cannot catch a contrast failure, a duplicate id, a
button with no accessible name, or a table without headers, all of which an
auditor reports for free.

Two small gaps sit beside that: `apps/web/components/SiteHeader.js` marks
the active navigation link with a class only and no `aria-current`, and
`apps/web/app/layout.js` has no skip link although `.sr-only` exists in the
stylesheet.

## Deliverables

### 1. An auditor on every route

Add `@axe-core/playwright` as a development dependency and one spec that
runs it on every route in the app, including `/workbench`, after the
route's data has loaded and its status row has settled, with WCAG 2.1 AA
tags. Known findings that cannot be fixed (MapLibre's canvas, the
attribution control) are an explicit allowlist with a reason per entry,
never a disabled rule.

### 2. `/workbench` joins the hand-written audit

Add it to `CORE_ROUTES`, and extend the form-control check to buttons and
links across every route.

### 3. The two fixes

`aria-current="page"` on the active header link; a skip link as the first
focusable element in the layout, targeting `main`, visible on focus.

## Acceptance criteria

- [ ] The auditor spec runs on every route under `apps/web/app` (derived from
      the directory listing, so a new route cannot be missed) and passes with
      zero violations outside the allowlist.
- [ ] Removing the `aria-label` from one button makes the auditor fail
      naming the route (proven failing-first).
- [ ] `/workbench` passes the landmark, heading and status-row checks.
- [ ] The active navigation link carries `aria-current="page"` and the skip
      link reaches `main` by keyboard, both asserted in the browser tier.
- [ ] `TESTING_CONTRACT.md` WEB-025 is extended or a `WEB-` row added;
      `CI_EVIDENCE_MAP.md` names the spec under `frontend`.

## Definition of done

Every route is audited by an auditor and by the repository's own rules, and
a new route is audited without anyone adding it to a list.

## What this plan deliberately does not do

- It does not add `prefers-reduced-motion` handling; the app has one
  transition and it is not motion in the WCAG sense. Note it if a later
  animation is added.
- It does not change the map's keyboard model, which is already tested.
