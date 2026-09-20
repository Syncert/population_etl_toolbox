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

- **Status:** Ready for review. Implemented 2026-09-17 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-17
- **Current milestone:** complete.

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

- [x] The auditor spec runs on every route under `apps/web/app` (derived from
      the directory listing, so a new route cannot be missed) and passes with
      zero violations outside the allowlist. Eleven routes; the three retired
      `redirect()` routes are excluded, with the reason stated in the spec.
- [x] Removing the `aria-label` from one button makes the auditor fail
      naming the route (proven failing-first). Proved with a bare
      `<button type="button" />` on `/quality`: one route failed, ten passed,
      and the failure named `/quality`.
- [x] `/workbench` passes the landmark, heading and status-row checks -- it is
      in `CORE_ROUTES` now, and passed on the first run.
- [x] The active navigation link carries `aria-current="page"` and the skip
      link reaches `main` by keyboard, both asserted in the browser tier
      ("the active navigation link says so, and the skip link reaches main").
- [x] `TESTING_CONTRACT.md` gains WEB-111; `CI_EVIDENCE_MAP.md` names the
      spec under `frontend`.

## Implementation evidence

### The auditor found a real defect on its first run

`/quality`'s "Where the evidence lives" table is wider than its column, so
its `.table-wrap` scrolls -- and had no keyboard access to scroll it
(`scrollable-region-focusable`). That is exactly the class of finding the
hand-written assertions cannot see, and it was there before this plan. Fixed
with `tabIndex={0}` and a named `role="region"`, so a reader who lands on it
is told what they have landed on.

The other `.table-wrap` elements did not fire the rule, because axe reports
it only where the element actually scrolls. They are not pre-emptively given
tab stops: a tab stop on something that does not scroll is a control that
does nothing. The auditor now runs on every route, so the next one that
starts scrolling is caught rather than reasoned about.

### What the allowlist is, and is not

An allowlist of **findings** -- `(rule, selector, reason)` -- never a
disabled rule. Disabling `region` would hide every future instance of it,
including the ones that are this application's fault; allowing it on
`.maplibregl-control-container` hides one, and says why. One entry today.

### The skip link's target

The link points at a `#main-content` wrapper in `app/layout.js`, not at each
route's own `<main>`. There are fifteen `<main>` elements across thirteen
files, and "someone adds a route and forgets the id" is the failure this
whole gate exists to prevent -- the nine-route list that never gained
`/workbench` is the same mistake. One target in the layout cannot be missed.
`tabIndex={-1}` makes it focusable by the link without adding a tab stop.

### Two lists, deliberately

The auditor's route list is **derived** from the app directory, because WCAG
applies to every route whether or not anyone thought about it. The
hand-written `CORE_ROUTES` list stays hand-written, because those assertions
are this repository's own rules rather than WCAG's and each is worth stating
deliberately -- but it gained `/workbench`, and the control-name check now
covers every route in it and reaches buttons and links rather than only
`select`, `input` and `textarea`.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:browser -- accessibility-audit` | 11 passed |
| `npm --prefix apps/web run test:browser -- accessibility-operations` | 11 passed |
| `npm --prefix apps/web run test:browser` | 145 passed in 4.9m (was 133) |
| `npm --prefix apps/web run test:unit` | 587 passed |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web audit --omit=dev --audit-level=high` | 0 vulnerabilities |
| `python -m pytest tests/unit -q` | 1807 passed |

`@axe-core/playwright` is a development dependency, so it is absent from the
production audit above and from the shipped bundle.

## Definition of done

Every route is audited by an auditor and by the repository's own rules, and
a new route is audited without anyone adding it to a list.

## What this plan deliberately does not do

- It does not add `prefers-reduced-motion` handling; the app has one
  transition and it is not motion in the WCAG sense. Note it if a later
  animation is added.
- It does not change the map's keyboard model, which is already tested.
