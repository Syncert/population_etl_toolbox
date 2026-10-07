---
id: workbench-time-views
depends_on:
  - time-windows-and-rollups
parallel_safe: false
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
  - python -m pytest tests/unit/api -q
---

# Workbench time views

## Status

Ready for review (2026-10-07). Split out of
[`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`](TIME_WINDOWS_AND_ROLLUPS_PLAN.md) RU-7.
WT-1..WT-3 are implemented and tested (WEB-141). WT-4 is an owner decision
and is raised at review; until it is made, compatibility keeps refusing a
monthly and an annual measure as before, so nothing here changes it.

## Why

A reader building a longitudinal chart wants "CPI by calendar year" or "FBI
counts by quarter" beside a monthly series. The API already answers both
(`time_grain`, `window`, ADR-0007) and labels every derived row; only the
workbench cannot ask.

## Work items

- [x] **WT-1: series shape.** Add an optional time view to a workbench
  series (`lib/workbench.ts`): part of the series key, the saved document,
  and the shared link. Older saved documents without it read as native.
  Revise WEB-095's pass metric for the new field.
- [x] **WT-2: control.** Offer only the views `/catalog/metrics/{code}`
  publishes (`time_grains`, `time_windows`), reusing `lib/timeViews.ts`;
  build requests through `buildHistoryObservationRequest` with `timeView`.
- [x] **WT-3: chart honesty.** Derived points are marked as derived in the
  legend and tooltip with the method; an incomplete window is a gap labelled
  with its reason, never a zero or an interpolated point; provider annual
  averages are labelled as the provider's.
- [ ] **WT-4: alignment.** Decide with the owner whether compatibility
  (`apps/api/services/compatibility.py`) may align a monthly and an annual
  measure through a declared rollup (the time-windows plan's optional RU-7
  clause). Geographic roll-up stays refused. **Open: raised with the owner
  at review.** Default if declined or unanswered: alignment stays refused
  (current behavior); a yes becomes its own plan.

## Decisions

- The stored document writes `time_grain` and `window` only for a time view,
  so a native series' document (and WEB-095's exact-shape expectation) is
  unchanged and an older document reads as native through the API defaults.
- The series key appends the view only when it is not native, so every key
  minted before this plan is unchanged.
- A time view is a latest read: the document forces `scope=latest` with no
  release or reduction, and the saved-analysis service refuses any stored
  combination the route refuses, plus a view on a measure with no calendar
  relation or no approved method.
- An evidence block refuses a time view: its envelope has no field yet for a
  derived window.

## Acceptance criteria

- A workbench series reads its chosen view and survives save, reload and a
  shared link; an older saved document still opens as native.
- No derived point is drawn without its label, and no gap is drawn as a value.
- Unit, browser and build gates pass.

## Evidence

- API: `saved_analysis.py` schema fields, `_require_consistent_observation_read`
  refusals, evidence-packet contradiction, `CONFIGURATION_DOCUMENT_FIELDS`;
  OpenAPI contract regenerated. `tests/unit/api/test_saved_analysis.py`,
  `test_evidence_packets.py`.
- Web: `lib/timeViews.ts` (`parseTimeView`, `timeViewFromDocument`,
  `timeViewDocumentFields`, `derivationLabel`), `lib/workbench.ts`,
  `lib/savedAnalysis.ts`, `lib/urlState.ts` (`tv:` key),
  `components/WorkbenchPage.tsx` (Time control, legend and hover name the
  view and method, derivation caption; an incomplete window is dropped and
  counted, never drawn).
- Validation (2026-10-07): `npm run test:unit` 731 passed; `tsc` and
  `npm run lint` clean; `npm run build` ok; `npx playwright test` 203 passed;
  `ruff check .` clean; `pytest tests/unit` passed.
