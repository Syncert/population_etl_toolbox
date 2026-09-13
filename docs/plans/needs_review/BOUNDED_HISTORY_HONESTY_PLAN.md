---
id: bounded-history-honesty
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
---

# A bounded read is never presented as a whole answer

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the web app's observation reads.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/observationAccess.ts`,
  `apps/web/components/ProfileProduct.tsx`,
  `apps/web/components/SourceExplorerPage.tsx`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

The explorer's map panel is careful about page bounds. `describeObservationLoad`
says "loaded N of M … records; the page bound cut the answer short, so the map
is incomplete", and the status pill turns red. Two other reads in the same
application take a bounded page and present it as the whole answer.

### A profile shows the oldest row it was sent

`ProfileProduct` asks `buildHistoryObservationRequest` for a geography's
observations with `limit: "50"` and then displays
`rows[rows.length - 1]` as the place's figure. Every observation order in
this API is ascending, so the last row of a bounded page is the newest row
*of that page*, not of the publication.

Under `scope=latest` most sources answer one row per geography — Census ACS
serves only the newest vintage, and the BLS and FRED latest relations hold
one row per series — so the last row is the newest one and the card is
right. Census PEP is the exception the API documents at length: its latest
publication is **every estimated year of the current vintage**, roughly 54
rows per county for `POPESTIMATE`. The first 50 of those end around 2020, so
the card shows a four-year-old estimate as the place's population.

`CENSUS_PEP:pep_cty_alldata:POPESTIMATE` fills the headline population slot
in two of the three shipped templates, so this is the number a reader sees
first.

The API already answers this question directly. `newest_per_geography=true`
exists for exactly this (API-066), ranks inside the source's own relation,
and the explorer's map already uses it. The profile asks for a page of the
publication and reduces it in the client instead — and reduces it wrongly.

### A history chart stops without saying so

`SourceExplorerPage`'s history panel issues one `apiFetch` with `limit:
"1000"`, ignores `payload.total`, and labels the result
`${items.length} historical observations`. A geography whose publication
exceeds the bound charts a prefix and reports it as the history. The
as-released fallback read in the same effect has the same shape.

## Objective

Every observation read in the web app either asks the resource for the
answer it wants, or reports that what it got was bounded.

## Acceptance criteria

1. A new `buildNewestValueRequest` asks the neutral resource for one row —
   the geography's newest published period — using `newest_per_geography`
   where the source declares it, and says in its result whether the resource
   did the reducing.
2. `ProfileProduct` uses it. Where the resource reduced, the card shows that
   row. Where it did not, the card still shows the newest row the client can
   identify and the measure's status reports the bound rather than implying
   completeness.
3. The explorer's history panel pages with `limit`/`offset` and reports
   "loaded N of M" with a warning state when the bound cut the series short,
   in the same words the map panel already uses.
4. The as-released fallback read in that effect is bounded the same way.
5. No request is sent that a source's capability entry does not declare.
6. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing what the profile templates measure, or their slot order.
- Rendering more than one series in the history chart.
- Raising the page bounds themselves; the point is to be honest about them,
  and to stop asking for a page when the resource can answer directly.
- Changing the map panel, which is the model being followed here.

## Evidence

### The gap, established first

Three unit tests for `buildNewestValueRequest` failed on import before the
function existed. The browser assertion in `profiles.spec.js` then failed for
a second, separate reason worth recording: the spec's capability fixture
declared `/api/v1/observations` without `newest_per_geography`, so the client
correctly refused to send a parameter the capability did not publish. The
fixture was stale against the served contract
(`tests/fixtures/api/openapi_contract.json` declares the parameter), and it
was corrected rather than worked around — a client that sends an undeclared
parameter is the defect this repository's capability discipline exists to
prevent, and the test passing for that reason would have been worthless.

### What changed

**`observationAccess.ts`**

- `buildNewestValueRequest(source, {metricCode, geoId})` asks `/observations`
  with `scope=latest`, the declared `geo_id` filter, `newest_per_geography=true`
  and `limit=1` where the capability declares the parameter. It returns
  `reducedByResource` so the caller knows whether the single row it holds is
  the publication's newest or the end of a bounded page.
- Where a source does not declare the parameter the read stays a bounded page
  (`NEWEST_VALUE_FALLBACK_LIMIT`), because inventing the reduction in the
  client would assert an order the API did not publish.
- `describeHistoryLoad` lives here rather than inside the 2,300-line explorer
  component, so the sentence a reader is shown is directly testable. A
  resource that published no `total` is never reported as short: there is no
  shortfall to state, and inventing one would be this client asserting a
  count the API did not publish.

**`ProfileProduct.tsx`** uses the new request, takes the row, and marks the
measure `warn` with `read N of M published rows; the page bound cut the
answer short, so this may not be the newest` when it held a bounded page.

**`SourceExplorerPage.tsx`** pages the history with `fetchCollectionPages`
(`pageSize` 1000, `maxPages` 5) for both the latest read and the as-released
fallback, and reports completeness through `describeHistoryLoad`. The
as-released path deliberately drops the total: released rows collapse to
fewer periods, so carrying their count forward would read as a shortfall
that is not one. The status span gained a `data-testid` so it is assertable.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 20 files, 227 tests passed |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run test:browser` | 55 passed (Chromium) |
| `pytest tests/unit -q` | 1380 passed |
| `python -m tests.support.catalog_evidence` | 301-row register renders; WEB-036 is `FULL` |
| `ruff check .` / `ruff format --check .` | clean |

### Not run

`make test-web-smoke` needs a live API, Martin, and proxy under Docker, and
this environment has no Docker daemon. The conclusion it would carry is that
the requests this change builds are accepted by a deployed API; that rests on
`newest_per_geography` and `offset` being declared on `/observations` in the
reviewed OpenAPI snapshot, which the profile fixture is now aligned to, and
on the client sending a parameter only where the live capability response
declares it — which is the rule the third unit test pins.

## Remaining work

None.
