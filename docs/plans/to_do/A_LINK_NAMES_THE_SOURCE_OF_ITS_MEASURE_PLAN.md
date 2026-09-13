---
id: a-link-names-the-source-of-its-measure
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A link into an analysis names the source that publishes its measure

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect on four screens.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/app/catalog/page.js`,
  `apps/web/components/DataQualityExplorer.tsx`,
  `apps/web/components/ProfileProduct.tsx`, `apps/web/app/page.js`,
  `apps/web/lib/savedAnalysis.ts`

## Context

`explorerHref` accepts `source` (`urlState.ts:29, 138-144`) and
`ComparisonWorkspace.tsx:779-783` passes it. The catalog page
(`catalog/page.js:224`), the quality screen (`DataQualityExplorer.tsx:288`),
the profile (`ProfileProduct.tsx:562`) and the home page (`page.js:83`)
build `explorerHref({ metric })` with no source. `/explore` mounts
`sourceKey="census"`, so the explorer fetches the Census ACS catalog, the
requested metric fails `items.some(item => item.metric_code === requested.metric)`
(`SourceExplorerPage.tsx:686-697`), and the page silently selects
`pickPreferredMetric` instead.

The same shape in the saved library: `reopenHref` for a comparison
(`savedAnalysis.ts:162-169`) omits `source_a`/`source_b`, which
`parseComparisonState` supports and the workspace applies. A saved BLS
versus FRED comparison reopens on the first two discovered sources, each
side falling back to `items[0]`, and then runs a real preflight and
comparison on a pair the reader never saved.

## Findings

- `/quality` → BLS → Explore on `BLS:LAU:UNEMP_RATE` opens the explorer on
  Census ACS showing `CENSUS_ACS:acs5:B01003_001`, with no statement that
  the requested measure was dropped.
- `catalog.spec.js:170-171` asserts the source-less href exactly, so it
  pins the defect. `data-quality.spec.js:152-155` and
  `profiles.spec.js:267-270` match `/metric=…/` and never follow the link.
  `saved-analysis.test.js:149-169` asserts only `toContain("/compare?")`.

## Acceptance criteria

1. Every `explorerHref` call carries the metric's source, derived from the
   metric row (`source_code`) rather than a client-side prefix rule, and
   `reopenHref` for a comparison carries both sources.
2. The explorer, given a metric it cannot find in the selected source's
   catalog, says so on the page rather than substituting one -- the
   WEB-036 rule for a link.
3. Browser specs follow one link from each screen and assert the explorer
   opened on the named measure; the pinned catalog assertion is corrected.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-063 at authoring time).

## Non-goals

- Resolving a source from a metric code's prefix in the client. The API
  publishes `source_code`; read it.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
