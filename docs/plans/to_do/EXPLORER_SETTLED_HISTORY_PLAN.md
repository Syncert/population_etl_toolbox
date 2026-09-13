---
id: explorer-settled-history
branch: feat/web-settled-history
depends_on:
  - newest-release-per-period
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The explorer asks for a settled history instead of computing one

## Plan status

- **Status:** Ready to claim. Authored 2026-09-13 as the consumer half of
  API-081, which is in `needs_review/`.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/observationAccess.ts`,
  `apps/web/components/SourceExplorerPage.tsx`
- **Depends on:** `newest-release-per-period` (API-081). Its parameter must
  be served and declared on `/catalog/capabilities` before this can read it.
- **Next pickup:** claim it, then start at "Acceptance criteria".

## Context

`AGENTS.md`: "Do not duplicate warehouse or API rules in client code."

A source whose latest relation keeps one row per geography — Census ACS
holds only the newest vintage — has a geography's history only across its
releases. The explorer handles that by re-reading the metric under
`scope=as_released` and reducing it in the browser:

```ts
export function collapseToNewestRelease(rows: ObservationRow[]): ObservationRow[]
```

Grouping by period is fine. Deciding which release is newer is not:

```ts
// Release identities compare as numbers where both sides are numeric
// (a vintage year, a watermark) and as text otherwise (an as-of date).
```

That is a client-authored rule standing in for one the warehouse publishes.
Every dispatch entry declares `release_order_expression`, and the two can
disagree: `2023.10` and `2023.9` order one way as numbers and the other as
text.

API-081 closed the gap upstream. `/observations` now serves
`scope=as_released&newest_release_per_period=true`, which returns exactly
this reduction, ranked by the source's own declared order, counted and paged
as the reduced set.

## Acceptance criteria

1. `buildHistoryObservationRequest`, or a sibling beside it, asks for
   `newest_release_per_period=true` where the capability entry declares the
   parameter — and never where it does not, which is the rule every other
   parameter in that module already follows.
2. The explorer's history fallback uses it instead of reading every release
   and reducing them: one request, already reduced, already ordered.
3. `collapseToNewestRelease` and `compareReleases` are removed once nothing
   calls them. A client-side release comparison left in place is the rule
   this plan exists to delete.
4. Where the capability does not declare the parameter, the current
   behaviour stands and is still tested — a deployment on an older API must
   not lose its trend.
5. The panel still reports when a page bound cut the answer short (WEB-036).
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing what the trend chart draws, or the as-released controls.
- Removing `scope=as_released` reads elsewhere; pinning a release is a
  different question and keeps its own path.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything. This plan is unclaimed.
