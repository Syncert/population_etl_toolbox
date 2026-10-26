---
id: web-plain-labels-everywhere
depends_on:
  - api-plain-labels
parallel_safe: false
complexity: small
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every screen names a metric with the API's plain label

## Status

Ready for review (v1.0 build-out, item D-2), 2026-10-26, branch
`feat/web-plain-labels-everywhere`. Merge after `feat/api-plain-labels`.

## What changed

- `displayMetricName` (`apps/web/lib/format.js`) returns the API's
  `metric_label` when it is served, and only otherwise strips the provider
  label's `Estimate!!` path as before. The place cards, standouts, workbench,
  comparison picker and explorer picker all name a metric through it, so no
  screen reformats a provider label its own way.
- A place card no longer prints its metric code beside the name; the code
  is kept in the card's `data-metric-code` and `title` attributes.
- The place-page fixtures serve `metric_label` from the same registry the
  API reads (`docs/semantics/metric_definitions.json`).

## Acceptance criteria

- The nation, a state, a county and a city page render no `Estimate!!` and
  no ACS variable code in their main text (WEB-144, browser).
- `displayMetricName` prefers the served label (WEB-144, unit).

## Evidence (OFFICE_LAB, Windows, 2026-10-26)

- `npx playwright test`: 252 passed.
- `npm run test:unit`: 864 passed, 1 failed -- `explainers.test.js` "refuses
  HTML", which fails identically on untouched `main` on Windows and passes in
  CI.
- `npm run lint`, `npm run typecheck`, `next build`: pass.
