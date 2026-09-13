---
id: a-shared-explorer-link-reopens-its-view
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-link-names-the-source-of-its-measure]
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A shared explorer link reopens the view it was copied from

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect in two fields.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/SourceExplorerPage.tsx`,
  `apps/web/lib/urlState.ts`

## Context

Two fields a link carries are lost on the way back in.

**Grain.** WEB-038 widened the vocabulary and the control to five words and
the serializer writes all five, but the apply step was not widened:

```js
if (requested?.geoLevel === "STATE" || requested?.geoLevel === "COUNTY") {
  setSelectedGeoLevel(requested.geoLevel);                 // SourceExplorerPage.tsx:698-700
}
```

`NATIONAL`, `PLACE` and `AGENCY` are parsed, validated, and discarded; the
selection falls to `COUNTY`, and a measure publishing both keeps the wrong
grain.

**Dimension filters.** `ExplorerState` has no dimension field
(`urlState.ts:28-46`); the serializer omits `dimensionSelections`
(`SourceExplorerPage.tsx:1570-1581`). A CDC view narrowed to one
`stratum_id` -- the narrowing the screen itself demands before it will
chart a series -- reopens stratified with a blank map. The saved-view path
(`savedAnalysis.ts:56-60`) carries the filters; the URL path does not, so
the two records of one view disagree.

## Findings

- `url-state.test.js:38-43` asserts all five grains round-trip through the
  parser and nothing asserts the page applies them; no browser spec
  navigates with `geo_level=` in the URL.
- Nothing asserts what the address bar holds after a dimension selection.

## Acceptance criteria

1. The apply step accepts every grain the vocabulary names; a link with
   `geo_level=PLACE` on a measure that publishes it opens at PLACE.
2. Dimension selections round-trip through the URL, with the same names the
   saved document uses.
3. Failing-first browser coverage for both, opening a copied URL and
   asserting the request the explorer issues.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-067 at authoring time).

## Non-goals

- Changing the saved-view document.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
