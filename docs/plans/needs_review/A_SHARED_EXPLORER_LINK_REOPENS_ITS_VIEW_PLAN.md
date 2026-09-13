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

- **Status:** Needs review. Implemented 2026-09-13 as catalog row WEB-073.
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

- **Grain.** The apply step is now `if (requested?.geoLevel)`. The parser has
  already refused anything outside `GEO_LEVELS`, and whether the *measure*
  publishes the grain is `offeredGeoLevels`' rule (WEB-038), which is where it
  belongs; the apply step's job is to stop discarding three of five published
  words.
- **Dimensions.** `ExplorerState.dimensions` carries the narrowing under the
  source's own declared filter names — the same keys the saved document's
  `filters` uses, so the two records of one view now agree.
  `serializeExplorerState` writes them sorted (two equivalent selections
  produce one link, which is this serializer's existing rule) and
  `parseExplorerState` reads any remaining filter-shaped key. The parser
  deliberately does not know which names a source declares: the capability
  entry does, and `SourceExplorerPage` applies only
  `dimensionFilters`/`neutralDimensionFilters` names, so the declaration check
  stays in the one place that already makes it. A key the explorer's own
  controls own (`source`, `metric`, `geo_level`, `map_mode`, `value_scale`,
  `state`, `geo`, `scope`, `release`) is never carried as a dimension — no
  source declares one today, and the guard means adding one cannot silently
  break a link. Names are bounded to the API's own filter shape and values to
  200 characters.
- The URL-sync effect gained `dimensionKey`/`dimensionSelections`, so the
  address bar changes when the narrowing does.
- New tests:
  - `tests/frontend/unit/url-state.test.js` — the dimension round trip, the
    sorted serialization, an empty selection carrying nothing, and a
    reserved key refusing to be overwritten.
  - `explorer.spec.js` > "a copied link reopens at the grain it names" — opens
    `?source=CENSUS_PEP&…&geo_level=PLACE` and asserts both the control and
    **every request issued for that measure** carries `geo_level=PLACE`.
  - `explorer.spec.js` > "a copied link reopens the dimension narrowing it
    names" — opens a CDC link carrying `stratum_id=overall` and asserts the
    read is one row (not stratified), the control shows it, every CDC request
    carries it, and the address bar still holds it, so copying again
    reproduces the view.
- Break-tests, both recorded:
  - restoring the two-grain apply branch and dropping `dimensions` from the
    serializer: `2 failed` — both new nodes.
  - removing only the dimension apply block: the CDC node fails with
    `Expected: "1" Received: "2"`, the stratified read the link was supposed
    to narrow.
- Tiers: frontend units 366 passed; `pytest tests/unit` 1552 passed;
  `npm run lint` and `tsc --noEmit` clean; browser tier 90 passed in 56.2s
  against a fresh production build.

## Remaining work

- None.
