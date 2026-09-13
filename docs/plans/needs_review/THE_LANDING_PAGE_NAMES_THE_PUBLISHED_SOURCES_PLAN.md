---
id: the-landing-page-names-the-published-sources
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The landing page names the sources the API published, and no others

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/app/page.js`, `apps/web/app/layout.js`,
  `apps/web/lib/catalog.ts`

## Context

Found by asking which pages the browser tier never asserts anything about.
The landing page is visited by the accessibility and CSP specs and checked by
neither, so I read it.

```js
const sourceNames = {
  CENSUS_ACS: "Census ACS",
  BLS: "Bureau of Labor Statistics",
  FRED: "Federal Reserve Economic Data",
};
…
{(sources.length ? sources : Object.keys(sourceNames).map((source_code) => ({ source_code }))).map((source) => (
  <span key={source.source_code}>{source.source_name || sourceNames[source.source_code] || source.source_code}</span>
))}
```

Under the heading **"Connected sources — Public data with its identity
intact"**, the page rendered that three-entry map whenever `sources` was
empty. `sources` is empty on every first paint, before discovery answers, and
again whenever discovery fails. So the first thing a visitor read there was a
list from the page rather than from the warehouse, unlabelled and
indistinguishable from the published answer — and right only for as long as
those three happened to be what the glossary had harvested.

`lib/catalog.ts`'s own header states the rule this broke:

> Filtering a page of results client-side would report a total the API never
> published … the catalog never carries a closed client-side source
> enumeration.

Two more enumerations of the same three sat beside it: the intro sentence
("Explore trusted Census, BLS, and FRED data") and the document description,
both written when three sources were all there were, and the API now serves
seven.

## What was changed

- `connectedSourcesBand(status, sources)` in `lib/catalog.ts` decides what the
  band may say: nothing while the list is being read, nothing on a failure
  beyond saying the list is unavailable, the published names in each source's
  own words with its code as the fallback — never a label from the page — and
  a statement rather than an invention when the published list is empty.
- The map is gone, and the page renders the band's decision.
- The intro sentence and the document description describe the whole
  ("published federal statistics"; "economic, health, agricultural and
  population analytics") rather than enumerating a subset. A document
  description cannot be derived from a request, so it has to be written in
  terms that stay true as sources are added.

## Validation

`tests/frontend/unit/catalog-view-model.test.js`:

- nothing is named while the list is being read
- a discovery failure names no source and says the list is unavailable
- a published source is named in the words it published; one that published
  no name falls back to its code
- an empty published list says so rather than inventing one
- the page's own source carries no hard-coded display name and does call
  `connectedSourcesBand`, read by walking up to the repository root the way
  the browser-tier guard does

## Deliberately not done

- **The band is not given a loading skeleton.** It says what it is doing in
  words, which is what the rest of this application does with a status line
  rather than with a shimmer.
- **`/catalog/capabilities` is not read instead.** "Connected" is the right
  reading of `/catalog/sources`: a source the registry declares and the
  glossary has never harvested is not connected, and the capability resource
  would list it. The nine-source difference between the two resources is a
  fact worth showing somewhere, and the catalog page is where it belongs.
