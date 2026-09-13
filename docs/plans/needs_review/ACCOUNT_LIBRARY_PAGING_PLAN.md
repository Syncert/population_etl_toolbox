---
id: account-library-paging
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - bounded-history-honesty
  - comparison-paging
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# An account's library is not capped at its first page

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13,
  closing the bounded-read class the two plans above opened.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/api/client.ts`,
  `apps/web/components/SavedAnalyses.tsx`,
  `apps/web/components/EvidencePacketBuilder.tsx`,
  `apps/web/components/ComposedArticle.tsx`
- **Depends on:** WEB-036 and WEB-039, whose wording and status treatment this
  follows. Both satisfied on this branch.
- **Next pickup:** none.

## Context

Three screens read an account's own library with a single request at
`limit: "200"` — the maximum `/analysis-configurations` and
`/evidence-packets` accept — and report, in green:

```ts
message: `${rows.length} of ${payload.total ?? rows.length} saved analyses`,
```

An account past two hundred saved analyses or packets therefore sees two
hundred of them, is told so, and has no way to reach the rest: these lists
are pickers, so the missing entries cannot be opened, edited, or added to a
packet. The status stays `ok`, which by the standard WEB-036 and WEB-039
established is the part that is wrong — a partial answer is never green here,
and both of those reads now page instead.

Both routes declare `offset` (`ge=0, le=100000`), so the entries are
reachable. `fetchCollectionPages` already does exactly this paging, and the
only reason these three could not use it is that it never forwarded the
bearer token.

## Objective

The three account library reads page, and say so when a bound stops them.

## Acceptance criteria

1. `fetchCollectionPages` forwards a bearer token, so an authenticated
   collection can be paged by the same helper every public one uses.
2. The token travels as it always has — an `Authorization` header, never a
   query parameter — and a paged request carries it on every page.
3. The three reads page to the reported total or to a declared page bound.
4. A complete load reports `ok`; a bound-limited one is failure-shaped and
   names the shortfall, in the wording WEB-036 established.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the routes' `limit` bound, their ordering, or their auth.
- Adding search or filtering to the library pickers.
- Paging the packet *content* reads, which fetch one document by id.

## Evidence

### The gap, established first

`the bearer token travels on every page, never in the query` failed first:
`fetchCollectionPages` destructured no `token`, so a paged authenticated read
answered `401`. Its companion — that an unauthenticated collection still
sends no `Authorization` header — passed before and after, and is kept
because widening a shared helper toward auth is exactly where a token starts
travelling somewhere it should not.

### What changed

- `fetchCollectionPages` forwards `token` to every page. It reaches
  `apiFetch`, which has always put it in an `Authorization` header and never
  in a URL; the test asserts the token appears in no request path.
- The three library reads page with a 200-row page and a ten-page bound, and
  report through one shared `describeLibraryLoad`.
- That helper lives in `lib/savedAnalysis.ts`, which already owns how account
  storage outcomes read (`describeSaveSuccess`, `describeSaveFailure`). It
  was briefly copied into all three components; three copies of one sentence
  is three places for one of them to drift, which is the defect WEB-043 was
  written about two commits earlier.
- It takes a singular and a plural noun. The first draft read "1 packets in
  your account", which the browser suite caught.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 250 passed |
| `npm --prefix apps/web run test:browser` | 61 passed (Chromium) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `pytest tests/unit -q` | 1382 passed |
| `python -m tests.support.catalog_evidence` | 310-row register renders; WEB-044 is `FULL` |

Two browser assertions moved with the wording: the composer and the reader
asserted "1 of 1 packets", which a complete load no longer says.

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for, and the account routes need provisioned tokens it does not
carry. The paging contract is the API's own `limit`/`offset` on both routes,
which the reviewed OpenAPI snapshot declares and API-071 holds server-side.

## Remaining work

None.
