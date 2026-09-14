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

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row WEB-072.)
- **Last updated:** 2026-09-14
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

- All four `explorerHref` call sites carry the measure's own source, read
  from the catalog row the screen already holds: `catalog/page.js`
  (`metric.source_code`), `DataQualityExplorer.tsx` (a new `sourceCode` on
  `MetricQualityRow`, from the metric row rather than the source the screen
  has selected), `ProfileProduct.tsx` (`metric?.source_code` of the resolved
  metric) and `app/page.js` (the featured metric's, falling back with the
  metric it falls back to). No prefix rule anywhere.
- `findExplorerSource` resolves either published source identity — the route
  segment the explorer's tabs use, or the glossary `source_code` a metric row
  publishes. One resolver, because both spellings come from the API and a
  link built from a metric row can only carry the second.
- `apps/web/lib/requestedMetric.ts` states the rule once for both screens:
  a requested measure the loaded catalog lists is selected; one it does not
  list is *named on the page* and nothing is selected;
  no requested measure at all leaves the screen to its own preferred choice.
  `SourceExplorerPage` renders it as `requested-metric-note`, and the
  dataset-facet effect no longer fills the empty selection while that notice
  stands — filling it there is the same substitution one effect later. The
  reader's own choice of measure or dataset clears the notice.
- The comparison workspace applies the same rule per side
  (`requested-metric-note-a`/`-b`) instead of falling back to `items[0]`, so
  reopening a saved pair can no longer run a real preflight and comparison on
  a pair nobody saved.
- **Deviation from criterion 1, recorded rather than worked around:**
  `reopenHref` cannot carry a source for a comparison. `AnalysisDocument` —
  the API's stored contract — records metric codes and no source codes, and
  the saved-analyses screen reads only its own configurations, so there is
  nothing published there to read a source from. Inferring one from the
  metric code's prefix is this plan's own non-goal. The rule is therefore
  enforced where the data is: the workspace refuses to substitute for a
  measure the chosen source does not publish, which is the harm the finding
  names.
- New and corrected tests:
  - `tests/frontend/unit/requested-metric.test.js` — the rule's three cases,
    both source spellings through `findExplorerSource`, and the quality row's
    `sourceCode`.
  - `explorer.spec.js` — "a link naming a measure and its source opens on
    that measure" (`source=BLS`, the glossary code, resolving to the `bls`
    tab) and "a link naming a measure this source does not publish says so".
  - `catalog.spec.js` asserted the source-less href exactly; corrected to the
    href the catalog now builds.
  - `data-quality.spec.js` now asserts `source=BLS` beside the metric.
  - `explorer.spec.js`'s WEB-047 node navigated to a Census PEP metric with
    no source, so it graded the saved document of a *substituted* ACS view.
    It now names the source and asserts `data-selected-metric` first, so it
    cannot pass on a substitute again.
- Break-test: restoring the single-identity resolver, the silent
  substitution, and the source-less catalog href leaves `3 failed, 31 passed`
  across `explorer.spec.js` and `catalog.spec.js` (the catalog href, the
  notice, and the WEB-047 node) and `1 failed | 364 passed` in the frontend
  units.
- Tiers: frontend units 365 passed; browser tier 88 passed in 54.8s against a
  fresh production build; `npm run lint` and `tsc --noEmit` clean;
  `pytest tests/unit/shared` 205 passed (the register guards).

## Criterion 3, as delivered (recorded 2026-09-14 at review)

Criterion 3 says "browser specs follow one link from each screen". What the
suite actually asserts is the href each screen builds, plus two explorer
nodes proving such a URL opens on the named measure rather than a
substitute. Reviewing this plan found two screens short of even that, and
both were closed rather than re-scoped away:

- `tests/frontend/browser/profiles.spec.js` matched only `metric=` on the
  measure card's link. It now also asserts `source=CENSUS_ACS`.
- The landing panel's link was asserted nowhere, and no browser node visits
  `/`. `tests/frontend/unit/catalog-view-model.test.js` now reads
  `apps/web/app/page.js` — the same way the hard-coded-name guard beside it
  does — and requires the panel's `explorerHref` call to carry a `source`
  taken from `source_code`. Removing that argument fails the node.

So every one of the four call sites in criterion 1 now has an assertion that
its link names a source: catalog (`catalog.spec.js`), quality
(`data-quality.spec.js`), profile (`profiles.spec.js`) and landing
(`catalog-view-model.test.js`), with `explorer.spec.js:1408` and `:1424`
proving the URL's two outcomes — it opens on the measure, or it says the
measure was not found and substitutes nothing. No node clicks a link and
follows it across screens; the plan's wording is stronger than the evidence,
and this section is the correction.

## A second recorded deviation under criterion 1

The plan records that `reopenHref`'s comparison branch
(`apps/web/lib/savedAnalysis.ts:256-262`) omits the sources because an
`AnalysisDocument` carries none. The explorer branch of the same function
(`savedAnalysis.ts:311-318`) calls `explorerHref` without a source for
exactly the same reason, and is covered by the same deviation. A stored
document that recorded its sources would close both at once; that is a
saved-analysis contract change, not this plan's.

## Remaining work

- None. `a-shared-explorer-link-reopens-its-view` is unblocked.
