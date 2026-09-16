---
id: one-formatter-and-absent-count
branch: claude/one-formatter-and-absent-count
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
---

# One formatter, and an absent count is not zero

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

**An unpublished count renders as zero.** `apps/web/lib/api/types.ts:486`
declares the correlation statistic's `n?: number` as optional, and the
client fills it in: `lib/workbench.ts:1255` `Number(statistic.n ?? 0)`,
`:1448` `n: Number(statistic?.n ?? 0)`, and
`components/WorkbenchPage.tsx:2096` renders
`(entry.statistic?.n ?? 0).toLocaleString()} paired geographies`. The
handoff's rule is "a value the source did not publish is never a zero", and
every other `?? 0` under `apps/web` is a counter or an index; this one is a
published statistic shown as "0 paired geographies" when it was not
published.

**Numbers and dates are formatted twenty ways.** `lib/explorerViewModel.ts`
uses `Intl.NumberFormat("en-US")`; more than twenty call sites use a bare
`toLocaleString()` (`ScatterChart.tsx`, `LineChart.tsx`, `BarChart.tsx`,
`HeatmapChart.tsx`, `CorrelationMatrixChart.tsx`, `ComparisonWorkspace.tsx`,
`SourceExplorerPage.tsx`, `app/catalog/page.js`, `lib/comparison.ts`,
`lib/workbench.ts`); `SourceNote.js` uses `toLocaleDateString()` and
`EvidencePacketBuilder.tsx` `toLocaleTimeString([])`. In a browser whose
locale is not `en-US`, the legend groups digits one way and the chart beside
it another.

## Deliverables

### 1. `n` absent is stated

`workbench.ts` keeps `n` as `number | undefined`; the eligibility rule that
needs it treats absence as "not published" with the same refusal shape the
module already uses for a missing reading; the panel renders "paired
geographies not published" instead of a zero.

### 2. One formatter module

`lib/format` exports `formatNumber`, `formatDate` and `formatTime` with
the locale chosen once (`en-US`, the current de-facto choice, as a named
constant), and every site above uses them. Add a lint rule or a unit test
that greps `apps/web/{app,components,lib}` for bare `toLocaleString(`,
`toLocaleDateString(` and `toLocaleTimeString(` and fails on any hit outside
`lib/format`.

## Acceptance criteria

- [ ] `tests/frontend/unit/workbench-correlation.test.js` has a case with
      `n` absent that asserts the "not published" reading and no zero.
- [ ] The grep test passes and fails when one bare call is reintroduced.
- [ ] Rendered numbers in the legend, the charts and the tables agree on
      grouping in the browser tier (one assertion on a value above 1,000).
- [ ] `TESTING_CONTRACT.md` gains a `WEB-` row for the absent count and
      extends the formatting row if one exists.

## Definition of done

Every number and date the app shows goes through one formatter, and a
statistic the API did not publish is shown as not published.

## What this plan deliberately does not do

- It does not localise the app; the locale is one constant, chosen once.
