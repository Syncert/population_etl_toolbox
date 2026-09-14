---
id: release-picker-paging
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - account-library-paging
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# Every published release a metric has is one the picker can pin

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13, the last instance of the bounded-read class WEB-036 opened.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/components/SourceExplorerPage.tsx`
- **Depends on:** WEB-044, whose wording and status treatment this follows.
  Satisfied on this branch.
- **Next pickup:** none.

## Context

The explorer's release control asks `/observations/releases` once, for two
hundred, and says:

```ts
message: items.length < total
  ? `${items.length} of ${total} published releases listed`
  : `${items.length} published release…`,
state: "ok",
```

It states the shortfall, which is why this is the last of five rather than
the worst. What it does not do is let anyone reach the releases it did not
list. This control is a picker: selecting a release is what sends
`scope=as_released&release=…`, and it is the only way the screen builds that
request or the link that reproduces it. A release past the two hundredth is
therefore unreachable and unshareable, and the screen reports that in green.

`/observations/releases` declares `limit` up to 1000 and `offset`, so the
entries are there to be read.

## Objective

The release control lists every release the metric published, and when a
bound stops it, says so as a failure rather than as health.

## Acceptance criteria

1. The release listing pages with `limit`/`offset` to the reported total or
   to a declared page bound.
2. A complete listing reports `ok` with its count; a bound-limited one is
   failure-shaped and names the shortfall, in the WEB-036 wording.
3. A resource that published no total is not reported as short.
4. The request is still built by `buildReleaseListRequest`, so a source that
   does not declare the route is still not asked.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the release control itself, the as-released scope, or what pinning
  a release sends.
- Raising the route's `limit` bound.

## Evidence

### The gap, and the guard that caught the missing test

This one stated its own shortfall — "200 of 240 published releases listed" —
which is why it is the last of five rather than the worst, and why there was
no failing assertion to write first: the defect was that it said so in green
and left the other forty unreachable.

Registering the catalog row without one was caught by the repository's own
guard — `test_every_catalog_id_has_an_implementation_reference` failed with
`{'WEB-045'}`, because a row no test names is a row with no evidence. The
browser test below was written in answer to it: the mock serves one release
per page against a total no number of pages will meet, and the test asserts
the status names the shortfall, carries the failure class, and that the
request offsets are `0, 1, …` rather than a single `offset=0`.

### What changed

The listing goes through `fetchCollectionPages` with a two-hundred-row page
and a ten-page bound, and reports through the same `describeLibraryLoad`
WEB-044 put in `lib/savedAnalysis.ts`, so a bound-limited listing is
failure-shaped and worded exactly as the map, the trend, the comparison, and
the account libraries word theirs. `buildReleaseListRequest` still builds the
request, so a source that declares no release route is still not asked.

`MetricReleaseListResponse` became unused and was removed; neither `tsc` nor
eslint flagged it, so it was found by reading the diff.

**Correction, 2026-09-14 at review.** That removal was described but not
made: `MetricReleaseListResponse` was still declared in
`apps/web/lib/api/types.ts`, and the single-shot `getObservationReleases`
wrapper in `apps/web/lib/api/client.ts` still used it, with no consumer
anywhere in `apps/web` or `tests` — the paging read calls
`fetchCollectionPages<MetricRelease>` instead. Both are deleted now, with
the import that carried the type. `MetricRelease`, the item shape, stays:
the paged read is typed on it.

### The class, now closed

Five reads presented a bounded page as a whole answer. All five now page and
report the bound the same way:

| Read | Catalog row |
| --- | --- |
| Profile measure card, explorer trend | WEB-036 |
| Comparison workspace | WEB-039 |
| Saved analyses, evidence packets | WEB-044 |
| Release picker | WEB-045 |

The explorer's map panel, which already did this, is what the other four were
written to match.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 250 passed |
| `npm --prefix apps/web run test:browser` | 62 passed (Chromium), up from 61 |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `pytest tests/unit -q` | 1382 passed |
| `python -m tests.support.catalog_evidence` | 311-row register renders; WEB-045 is `FULL` |

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for. The paging contract is `/observations/releases`' own
`limit`/`offset`, which the reviewed OpenAPI snapshot declares.

## Remaining work

None.
