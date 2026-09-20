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

- **Status:** Ready for review. Implemented 2026-09-16 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.

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

- [x] `tests/frontend/unit/workbench-correlation.test.js` has a case with
      `n` absent that asserts the "not published" reading and no zero
      ("an unpublished pair count reads as not published, never as zero"),
      plus a matrix case asserting `cell.n` is `null` rather than `0`.
- [x] The grep test passes and fails when one bare call is reintroduced —
      proved by putting `Number(value).toLocaleString(undefined, …)` back into
      `BarChart.tsx` and watching it report
      `components/BarChart.tsx:49` by file and line.
- [x] Rendered numbers in the legend, the charts and the tables agree on
      grouping in the browser tier (one assertion on a value above 1,000):
      "one number is grouped the same way wherever it is shown", in a
      `de-DE` browser context, on 38,900,000.
- [x] `TESTING_CONTRACT.md` gains a `WEB-` row for the absent count and
      extends the formatting row if one exists — WEB-105 covers both halves;
      WEB-001 keeps its own scope and is unchanged.

## Implementation evidence

### What changed

- `apps/web/lib/format.js` gains `DISPLAY_LOCALE` (`en-US`, named once),
  `formatNumber`, `formatDate` and `formatTime`. `formatNumber` guards
  `null`, `undefined` and `""` explicitly before coercing, because
  `Number(null)` is `0` and turning an absence into a zero is the other
  defect this plan closes.
- Every bare `toLocaleString(`, `toLocaleDateString(` and `toLocaleTimeString(`
  under `apps/web/{app,components,lib}` now goes through that module — the
  four chart components, `SourceExplorerPage`, `WorkbenchPage`,
  `ComparisonWorkspace`, `EvidencePacketBuilder`, `SourceNote`,
  `app/page.js`, `app/catalog/page.js`, `lib/comparison.ts` and
  `lib/workbench.ts`.
- `lib/explorerViewModel.ts` asked `Intl` for `"en-US"` by name in two
  places. That is the same defect spelled the other way round — a second
  place the locale is decided — so it now reads `DISPLAY_LOCALE`, and the
  swept assertion covers a locale literal handed to `Intl` as well as a
  bare `toLocale*` call.
- `lib/workbench.ts`: `pairedGeographies` returns `number | null`,
  `PAIRED_GEOGRAPHIES_NOT_PUBLISHED` states the wording once, and
  `pairedGeographiesText` is shared by the pair list and the matrix tooltip
  so one absent count cannot read two ways on one screen. The pair count,
  the coverage line and the contemporaneous line refuse together, and
  `CorrelationMatrixCell.n` is `number | null` — the diagonal, a declined
  pair and a pair with no answer carried `n: 0`, which said the API measured
  zero pairs.
- `WorkbenchPage`'s "Every plotted value" table rendered `point.value` raw,
  so it was the one table on the page that did not group at all while the
  ranking table beside it did. It formats now, which is what makes the
  browser assertion about agreement possible.

### The gap, before the fix

The browser assertion was run against the pre-fix rendering by putting
`{point.value}` and the bare `toLocaleString` back: in a `de-DE` context the
table read `38900000` and the chart's own labels grouped with `.`, so the
assertion failed on the exact text a de-DE reader would have seen.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 562 passed, 35 files (was 557) |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run test:browser` | 115 passed in 3.4m (was 114; see the note below) |
| `python -m pytest tests/unit -q` | 1802 passed |

The browser tier needs `PLAYWRIGHT_CHROMIUM_EXECUTABLE` set to the
container's pre-installed Chromium
(`/opt/pw-browsers/chromium-1194/chrome-linux/chrome`); the
`@playwright/test` version pinned here looks for a revision the image does
not carry, and `playwright.config.mjs` documents that variable as the escape
hatch for exactly this case. CI installs its own browsers and needs nothing.

## Definition of done

Every number and date the app shows goes through one formatter, and a
statistic the API did not publish is shown as not published.

## What this plan deliberately does not do

- It does not localise the app; the locale is one constant, chosen once.
