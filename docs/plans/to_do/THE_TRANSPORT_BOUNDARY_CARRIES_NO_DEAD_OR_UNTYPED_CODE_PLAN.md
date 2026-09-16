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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] The walker test fails on a literal for an undeclared path and on a
      `types.ts` property absent from the snapshot (proven failing-first
      with a throwaway edit).
- [ ] `grep -rn "Promise<unknown>" apps/web/lib/api` returns nothing.
- [ ] No `.js` file remains under `apps/web/lib` that builds a request to
      the API or Martin, or reads or writes browser storage.
- [ ] The scratch files are gone from `git ls-files` and ignored.
- [ ] `TESTING_CONTRACT.md` gains a `WEB-` row for the walker.

## Definition of done

The client's contract with the API and with Martin is one typed module per
boundary, every literal in it is proven against the reviewed snapshot, and
nothing in it is unused.

## What this plan deliberately does not do

- It does not generate the client from OpenAPI; a graded hand-written client
  keeps the classified `ApiError` and the bounded paging the handoff built.
- It does not split `SourceExplorerPage.tsx` or `WorkbenchPage.tsx`; that is
  a refactor without a contract behind it.
