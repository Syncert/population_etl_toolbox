---
id: web-plain-labels-everywhere
depends_on:
  - api-plain-labels
parallel_safe: false
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every screen names a metric with the API's plain label

## Status

Ready for review (v1.0 build-out, item D-2), 2026-10-31, branch
`claude/project-thread-xc0hjm` (continues `feat/web-plain-labels-everywhere`,
with `dev` merged in after A-2 landed as #170).

## What changed

- `displayMetricName` (`apps/web/lib/format.js`) returns the API's
  `metric_label` when it is served, and only otherwise strips the provider
  label's `Estimate!!` path. `metricNameNote` gives the hover text: "Plain
  label awaiting review" for a `draft` label, "The publisher's own label"
  for `provider`, then the code. `components/MetricName.tsx` renders a name
  with the code in `data-metric-code`/`title` and the status in
  `data-label-status`.
- Reconciled with A-2's final shape: `MetricLabelStatus` (approved, draft,
  needs_review, deprecated, provider) and `metric_label_a`/`_b` on
  `/comparison`, its preflight and correlation, and `metric_label` on matrix
  measures. `pairLabel` in `lib/comparison.ts` names each side from them, so
  comparison column headers, the scatter axes and the coverage note show
  labels. `METRIC_LABELS_REVIEWED_ONLY` needs nothing from the client: the
  API serves the provider label instead, with status `provider`.
- Codes removed from the visible text of: place cards (kept in attributes),
  standouts, profile and use-case identity lines, the profile "looked for"
  reason (now `lookedFor`, in `data-looked-for`), a place card with no
  measure, the compare-places rows, the workbench picker, grain note and
  series errors, the explorer table's metric column and its source note, the
  catalog list (now shows the served definition), the data-quality table, the
  studio frame footer, the evidence-packet saved-view list, and the explainer
  footer ("Applies to N published measures").
- Left visible on purpose, marked `data-provenance`: an evidence envelope's
  measures (a reproduction record), a saved analysis's stored request, the
  API's stale-block verdict quoted as given, and a link's own unknown code
  ("This link asked for ..."), which has no label because no catalog row
  names it.

## Acceptance criteria

- Every browser test ends by reading its page's rendered text (less
  `data-provenance` records) and fails on `Estimate!!` or an ACS variable
  code (`tests/frontend/support/servedRequests.js`, WEB-144). On its first
  run it found 38 screens that still printed codes; all are fixed.
- `displayMetricName` prefers the served label; comparison headers use
  `metric_label_a`/`_b`; the hover text marks drafts (WEB-144, unit).

## Evidence (cloud session, Linux, 2026-10-31)

- `npm run test:unit`: 878 passed.
- `npm run lint`, `npm run typecheck`, `next build`: pass.
- `npx playwright test`: 252 passed.
- `pytest tests/unit/shared/test_catalog_evidence.py`: 2 passed.
- Merged into `dev` as #177 (8357a3d).

## Evidence (OFFICE_LAB, live stack, 2026-10-31)

Web and API rebuilt from `dev` 8357a3d; headless Chromium against
`localhost:3001`, each page's `innerText` scanned with `[data-provenance]`
elements removed, for `Estimate!!` and ACS codes (`[BCS]#####_###`):

| Page | Status | Text | `Estimate!!` / codes |
|---|---|---|---|
| `/us/texas/travis-county` | 200 | 90k chars | 0 / 0 |
| `/compare` | 200 | 45k chars | 0 / 0 |
| `/workbench` | 200 | 17k chars | 0 / 0 |
| `/catalog` | 200 | 33k chars | 0 / 0 |

## Earlier evidence (OFFICE_LAB, Windows, 2026-10-26, before the dev merge)

- `npx playwright test`: 252 passed. `npm run test:unit`: 864 passed, 1
  failed (`explainers.test.js` "refuses HTML", Windows-only, passes in CI).
