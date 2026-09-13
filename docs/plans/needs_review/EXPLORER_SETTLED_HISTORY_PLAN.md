---
id: explorer-settled-history
branch: claude/iterate-plans-improvements-ir885c
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

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13,
  as the consumer half of API-081, which is in `needs_review/`.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/observationAccess.ts`,
  `apps/web/components/SourceExplorerPage.tsx`
- **Depends on:** `newest-release-per-period` (API-081). Its parameter must
  be served and declared on `/catalog/capabilities` before this can read it.
- **Next pickup:** none.

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

## Evidence

### The gap, established first

Three unit tests failed before `buildSettledHistoryRequest` existed: the
request it builds, that it answers `null` against an API that does not
declare the parameter, and that a source with no as-released surface is not
asked at all. A browser test then drove the explorer against a capability
that declares it and asserted the request carries
`scope=as_released&newest_release_per_period=true` with the geography and no
pinned release.

Writing the first of those surfaced a guard that was too strong: the builder
initially required `servesAsReleased`, which also demands the
`/observations/releases` route. That route is how a client discovers an
identity to **pin**, and a settled history pins nothing — the condition is
the neutral route's own declarations, `scope` and `newest_release_per_period`.

### Criterion 3 is not met, deliberately

The plan asked for `collapseToNewestRelease` and `compareReleases` to be
removed "once nothing calls them", and for the older-API fallback to keep
working (criterion 4). Those cannot both hold: the fallback is what calls
them.

The fallback was kept, so they stay. This is the discipline the rest of
`observationAccess.ts` already follows — every parameter is conditional on
the capability entry declaring it, and the client degrades honestly rather
than assuming — and the alternative is a deployment whose API predates
API-081 silently losing a geography's trend. The client-side comparison is no
longer how the answer is reached where the API can give it; it is the
declared fallback for where it cannot, and the unit tests that pin its
behaviour remain.

Deleting it becomes correct once no supported deployment can serve an API
without the parameter. That is a decision about deployment support, not about
this screen, and it is not one this plan should make silently.

### What changed

- `ExplorerSource` gained `supportsSettledHistory`, read from the neutral
  route's declared parameters exactly as `supportsNewestPerGeography` is.
- `buildSettledHistoryRequest` builds the one request, carrying only declared
  filters and never a pinned release.
- The explorer's cross-release branch uses it where it is offered and uses
  the previous read-and-reduce where it is not.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 253 passed |
| `npm --prefix apps/web run test:browser` | 63 passed (Chromium), up from 62 |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` / `check:csp` | succeeded; every route within budget; CSP passed |
| `pytest tests/unit -q` | 1388 passed |
| `python -m tests.support.catalog_evidence` | 313-row register renders; WEB-046 is `FULL` |

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for. It is the tier where the reduction would be read from a
real warehouse rather than a fixture; API-081 owns that the SQL is right, and
this plan owns only which request the client sends.

## Remaining work

None.
