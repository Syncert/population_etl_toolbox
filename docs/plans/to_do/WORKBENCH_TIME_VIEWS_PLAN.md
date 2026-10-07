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

To do. Split out of
[`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`](../needs_review/TIME_WINDOWS_AND_ROLLUPS_PLAN.md)
RU-7 on 2026-10-07. The explorer's Time control shipped there (WEB-140); the
workbench was left on native periods because giving a workbench series a
time view changes its saved-document shape, which WEB-095 grades and the
time-windows plan said to revise only deliberately.

## Why

A reader building a longitudinal chart wants "CPI by calendar year" or "FBI
counts by quarter" beside a monthly series. The API already answers both
(`time_grain`, `window`, ADR-0007) and labels every derived row; only the
workbench cannot ask.

## Work items

- [ ] **WT-1: series shape.** Add an optional time view to a workbench
  series (`lib/workbench.ts`): part of the series key, the saved document,
  and the shared link. Older saved documents without it read as native.
  Revise WEB-095's pass metric for the new field.
- [ ] **WT-2: control.** Offer only the views `/catalog/metrics/{code}`
  publishes (`time_grains`, `time_windows`), reusing `lib/timeViews.ts`;
  build requests through `buildHistoryObservationRequest` with `timeView`.
- [ ] **WT-3: chart honesty.** Derived points are marked as derived in the
  legend and tooltip with the method; an incomplete window is a gap labelled
  with its reason, never a zero or an interpolated point; provider annual
  averages are labelled as the provider's.
- [ ] **WT-4: alignment.** Decide with the owner whether compatibility
  (`apps/api/services/compatibility.py`) may align a monthly and an annual
  measure through a declared rollup (the time-windows plan's optional RU-7
  clause). Geographic roll-up stays refused.

## Acceptance criteria

- A workbench series reads its chosen view and survives save, reload and a
  shared link; an older saved document still opens as native.
- No derived point is drawn without its label, and no gap is drawn as a value.
- Unit, browser and build gates pass.
