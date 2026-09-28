---
id: selected-geography-shows-painted-period
branch: codex/analytics-backlog-2026-09-28
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The selected geography shows the period the map paints

## Plan status

- **Status:** Ready for human review on `codex/analytics-backlog-2026-09-28`.
- **Last updated:** 2026-09-28
- **Dependencies:** none.
- **Next pickup:** None.

## Why

On `/explore?source=FBI_UCR&metric=FBI_UCR:summarized_homicide:HOM:clearance:absolute_total&geo_level=STATE`,
the map colours and tooltips every state by its newest period (Alaska:
`2023-06-01 – 2023-06-30`), but selecting Wisconsin (`state:55`) shows
**Latest value 11 count, Period 1990-01-01 – 1990-01-31** — its *oldest* row.
Observed on the development stack on 2026-09-25. Wisconsin has all 402 months
(20,904 rows / 52 geographies = 402 = Jan 1990 … Jun 2023); this is a selection
defect, not missing data.

Cause (`apps/web/components/SourceExplorerPage.tsx:575-578`):

```ts
const selectedObservation = useMemo(
  () => observations.find((item) => item.geo_id === selectedGeoId) || null,
  [observations, selectedGeoId],
);
```

`observations` is the unreduced page set. FBI UCR, CDC and USDA NASS do not
publish the aligned reduction (`publishes_aligned_reduction` is false, so the
explorer cannot send `newest_per_geography`), and history is served
oldest-first (`apps/api/registry.py` FBI `latest_order`; API consumer guide,
ordering), so `.find` returns the first — oldest — row. The map, tooltip,
click handler and selection outline all read `mappableObservations` /
`observationIndex`, which `newestPerGeography()` reduced
(`apps/web/lib/observationAccess.ts`). The panel is the one reader that does
not. Sources the server reduces return one row per geography, which is why
this was never seen on BLS/ACS/FRED/PEP.

The "Latest value" label therefore states something false about a provider
fact on every non-reducing source.

## Objective

The Selected geography panel's value, period, MOE and dataset are the same row
the map painted for that geography — for every source, reducing or not.

## Work items

- [x] **SG-1: failing-first unit test.** Extract the selection into a pure
  helper (or test through the existing `observationAccess` seam) and prove
  that, for a multi-period unreduced row set delivered oldest-first, the
  selected row is the newest period for that geography and equals what
  `newestPerGeography` yields. Include a geography whose newest period is
  withheld (`not_reported`): the panel shows the withheld newest row, never an
  older reported value, matching what the map paints.
- [x] **SG-2: fix.** Read the selection from `observationIndex` /
  `mappableObservations` (the same source as `SourceExplorerPage.tsx` lines
  1562 and 1753), not from raw `observations`. Keep the observation table on
  raw rows — it lists everything as published by design.
- [x] **SG-3: browser evidence.** Extend `tests/frontend/browser/explorer.spec.js`
  selection flow (currently asserts only heading and history count) to assert
  that the panel's Period equals the painted/tooltip period for a
  non-reducing source fixture with more than one period.
- [x] **SG-4: catalog.** Add the behaviour to `docs/reference/TESTING_CONTRACT.md`
  web catalog and map it in `docs/reference/CI_EVIDENCE_MAP.md`.

## Acceptance criteria

- For FBI UCR / CDC / NASS fixtures with multiple periods, selecting a
  geography shows the newest period's row, identical to the tooltip.
- A withheld newest value is shown as withheld, not replaced by an older one.
- Reducing sources (BLS, ACS, FRED, PEP) are unchanged.
- Web unit and browser tiers pass; the new tests fail on the current code.

## Out of scope

Choosing a period other than the newest, and any time rollup — see
`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`.

## Implementation and validation, 2026-09-28

- `selectedMappedObservation` now selects from `mapRows(...).mappable`, and
  the panel uses it. The raw observation table still receives every row.
- The focused unit test failed first because the selection helper was absent.
  The browser test failed first with `32.4 percent` and the 2021–2022 period
  while the map coloured the 2023–2024 row. Both pass after the fix.
- Unit fixtures cover oldest-first FBI UCR, CDC and USDA NASS publications,
  a newest `not_reported` row, and an already reduced Census ACS row. Browser
  fixtures cover both a numeric and a withheld newest CDC row.
- `npm --prefix apps/web run test:unit`: 697 passed.
- `npm --prefix apps/web run lint` and `run typecheck`: passed.
- `npm --prefix apps/web run build`: passed.
- `CI=1 npm --prefix apps/web run test:browser`: 167 passed on the production
  build. After adding the withheld case, a focused browser run passed 2/2.
- `python -m pytest tests/unit/shared/test_repository_hygiene.py
  tests/unit/tooling/test_plan_environments.py --basetemp=.tmp/pytest-sg -q`:
  21 passed. No warehouse or external service check is required by this web
  selection contract.
