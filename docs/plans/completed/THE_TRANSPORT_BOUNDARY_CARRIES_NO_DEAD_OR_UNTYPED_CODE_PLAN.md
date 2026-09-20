---
id: transport-boundary-hygiene
branch: claude/transport-boundary-hygiene
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
---

# The transport boundary carries no dead or untyped code, and its literals are graded

## Plan status

- **Status:** Ready for review. Implemented 2026-09-17 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-17
- **Current milestone:** complete.

## Why

The handoff makes `apps/web/lib/api/client.ts` "the single transport
boundary" and says "do not add a second fetch path". The boundary has
loosened in four ways:

- `client.ts:393` exports `getFreshness(): Promise<unknown>` while the
  snapshot declares `SourceFreshness` and `SourceContent`, and
  `components/DataQualityExplorer.tsx` bypasses it with a literal
  `"/catalog/freshness"`.
- `getLatestObservations`, `getTimeseries`, `getSourceLatestObservations`
  and `getSourceTimeseries` in `client.ts` have zero consumers outside the
  file; `lib/observationAccess.ts` builds its own resource strings.
- `tests/frontend/support/servedContract.js` and
  `tests/frontend/unit/served-contract-fixtures.test.js` grade the test
  fixtures against `tests/fixtures/api/openapi_contract.json`. Nothing
  grades the roughly thirty route literals under `lib/` and `components/`,
  or the property names in `lib/api/types.ts`, against that snapshot. The
  client is hand-written, and a renamed field on the API is found by a
  reader, not a test.
- `lib/tiles.js` (the Martin tile boundary, ~400 lines) and
  `lib/savedCharts.js` (the persistence boundary) are untyped `.js` under
  `checkJs: false` (`apps/web/tsconfig.json:17`), while every other
  contract-boundary module is `strict` TypeScript.

Two tracked scratch files sit beside the app:
`apps/web/.verify-devreset-pid.txt` and `apps/web/.verify-pre-next-state.txt`
are in git and not ignored. `components/ComparisonWorkspace.tsx` carries the
app's only three `eslint-disable-next-line react-hooks/exhaustive-deps`.

## Deliverables

### 1. Dead and untyped exports

Delete the four unused helpers or route `observationAccess.ts` through them
(one or the other; not both paths). Type `getFreshness` against the
snapshot's schema and make `DataQualityExplorer` use it.

### 2. Route literals and field names are graded

Extend the served-contract test with a second walker over
`apps/web/{lib,components}` that asserts every `/api/v1`-relative literal
(including the templated source-scoped ones) is a declared operation in the
snapshot, and that every property of the named interfaces in `types.ts`
exists in the snapshot's `schemaFields` for that schema.

### 3. The two boundaries become TypeScript

Convert `lib/tiles.js` and `lib/savedCharts.js` to `.ts` under the existing
`strict` settings, with the tile URL and property contracts typed against
`tests/fixtures/martin` where a fixture exists.

### 4. Hygiene

`git rm` the two scratch files and add `apps/web/.verify-*` to `.gitignore`;
resolve the three hook suppressions (the effects read values already in
their dependency lists, so a `useCallback` extraction is the likely shape),
and record why if one must stay.

## Acceptance criteria

- [x] The walker test fails on a literal for an undeclared path and on a
      `types.ts` property absent from the snapshot, both proven failing-first.
      The first attempt at the literal half proved nothing; see below.
- [x] `grep -rn "Promise<unknown>" apps/web/lib/api` returns nothing.
- [x] No `.js` file remains under `apps/web/lib` that builds a request to
      the API or Martin, or reads or writes browser storage. `lib/format.js`
      remains and does neither -- it renders numbers and dates.
- [x] The scratch files are gone from `git ls-files` and ignored, with
      `apps/web/.verify-*` proven to match by creating one and checking
      `git status`.
- [x] `TESTING_CONTRACT.md` gains a `WEB-` row for the walker: WEB-109.

## Implementation evidence

### 1. Dead and untyped exports

The four MVP-shaped observation wrappers -- `getLatestObservations`,
`getTimeseries`, `getSourceLatestObservations` and `getSourceTimeseries` --
are deleted rather than routed to. `observationAccess.ts` is the established
way those routes are addressed: it picks the access shape a source's
capability entry declares and returns the resource and params, and the smoke
tier drives it. Keeping both is the thing the handoff's "single transport
boundary" forbids, and the unused one is the one that drifts. The api-client
test that exercised one of them now sends the same request through `apiFetch`,
which is what `observationAccess` produces.

`getFreshness` is typed `CollectionResponse<SourceFreshness>`, the schema the
snapshot declares for the route, and `DataQualityExplorer` calls it instead of
sending its own literal. `SourceFreshness` moved from `lib/dataQuality.ts` to
`lib/api/types.ts` -- it is what a route answers, and the walker grades
`types.ts` against the snapshot -- and `dataQuality.ts` re-exports it, so its
readers are unchanged.

### 2. The walker

`tests/frontend/unit/transport-boundary.test.js` sweeps
`apps/web/{lib,components,app}` for resources addressed two ways: an argument
to a transport function, and a `resource`/`PATH` an access-shape module hands
back. It does **not** sweep every string starting with a slash: `/explore` is
a route in this application, not a resource on the API, and a sweep that
cannot tell them apart needs a list of exceptions that is itself untested.

**The first version proved nothing, twice.** Its generic matcher was
`(?:<[^>(]*>)?`, which stops at the inner `>` of
`apiFetch<CollectionResponse<SourceSummary>>(...)` -- so it matched none of
the call sites written that way, which is most of them. It still found enough
literals elsewhere to clear a floor of eight and pass. Worse, the mutation I
used to check it (`apiFetch<CollectionResponse<SourceSummary>>` → a
misspelling) named a line that does not exist in this file, so the edit was a
no-op and the green run meant nothing. Both are fixed: the generic is matched
with `[^(]*`, the floor is 20, and the mutation is now confirmed present in
the file before the test runs. With `/catalog/sources` misspelled it fails
naming the file, the line and the literal; with a `metrics_retired` added to
`SourceFreshness` it fails naming `SourceFreshness.metrics_retired`.

### 3. The two boundaries are TypeScript

`lib/savedCharts.js` and `lib/tiles.js` are now `.ts` under the existing
`strict` settings. Notable:

- Reading the saved-chart store returns `SavedChart[]`, and an entry without
  an id is dropped: the store holds whatever an earlier version of this
  application wrote, so it is read as data rather than trusted as a shape.
- `tiles.ts` uses GeoJSON's own `Feature`/`FeatureCollection` types rather
  than an approximation, because the collection goes straight to MapLibre's
  `geojson` source -- a near-miss shape would only move the cast.
- `@mapbox/vector-tile` and `pbf` ship no types and have no `@types` package
  here, so `apps/web/types/vector-tile.d.ts` declares the surface this
  application calls and nothing wider. The alternative was `any` at the one
  place a malformed tile would first be noticed.
- `noUncheckedIndexedAccess` caught a real edge in `loadPreviewTileFeatures`:
  a tile carrying no layers at all indexed `Object.keys(...)[0]` as
  `undefined` and then read a property off it.

### 4. Hygiene

`apps/web/.verify-devreset-pid.txt` and `apps/web/.verify-pre-next-state.txt`
are untracked and deleted, and `apps/web/.verify-*` is ignored -- a
developer's process id and a snapshot of their pre-run state were travelling
in the repository.

All three `react-hooks/exhaustive-deps` suppressions are gone, and none was
replaced by a wider dependency list that re-runs a request nothing asked for:

- `metricsTrackers` was an object literal rebuilt every render, so an effect
  depending on it re-ran every render. Memoised, it is a real dependency.
- `preflightRequestParams` and `comparisonRequestParams` declared they needed
  a whole `ComparisonSelection` while reading two and four of its fields. That
  over-declaration is what forced the suppressions: the effects had to spread
  the whole object, which made `selection` a dependency of effects that must
  not re-run when an unrelated part of it changes. They now take a
  `ComparisonPair` and the two scope fields; a `ComparisonSelection` still
  satisfies both structurally, so every other caller and test is unchanged.
**Narrowing those dependency lists caught a defect, and introduced one.**
Adding `metricCodeA` and `metricCodeB` to the comparison effect made it run on
the intermediate state where one side has been cleared while the previous
pair's `comparable` verdict is still held -- and it sent a comparison request
naming one measure. `comparison.spec.js` caught it: two requests where the
test expects one, the second missing `metric_code_a`. The effect now refuses
an incomplete pair before anything else, which the old list hid rather than
prevented.

- The catalog effect read `sources` only to name a source in a notice.
  Re-fetching both catalogs because a source title arrived is a request
  nothing asked for, so it reads a ref -- the same pattern the module already
  uses for the requested link state -- which says that out loud where the
  suppression said nothing.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 578 passed, 38 files (was 576, 37) |
| `npm --prefix apps/web run test:browser` | 130 passed in 4.0m; see the note below |
| `npm --prefix apps/web run lint` | clean, and no rule suppressed |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run build` | succeeded |
| `npm --prefix apps/web run check:bundle` / `check:csp` | both pass |
| `python -m pytest tests/unit -q` | 1807 passed |

### One note on running the browser tier

Two full runs of this tier overlapped on one `next dev` server on port 3100,
and neither result could be trusted; both were killed. The 130 recorded above
is from a single run with nothing else in flight. Run exactly one at a time:
the tier reuses an existing server rather than starting its own, so a second
run shares the first one's.

## Definition of done

The client's contract with the API and with Martin is one typed module per
boundary, every literal in it is proven against the reviewed snapshot, and
nothing in it is unused.

## What this plan deliberately does not do

- It does not generate the client from OpenAPI; a graded hand-written client
  keeps the classified `ApiError` and the bounded paging the handoff built.
- It does not split `SourceExplorerPage.tsx` or `WorkbenchPage.tsx`; that is
  a refactor without a contract behind it.
