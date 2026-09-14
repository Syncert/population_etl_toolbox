---
id: comparison-paging
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - bounded-history-honesty
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A national comparison is the nation, not its first thousand counties

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13 from an investigation of the comparison workspace's reads.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/api/client.ts`,
  `apps/web/components/ComparisonWorkspace.tsx`
- **Depends on:** `bounded-history-honesty` (WEB-036) — the same defect class,
  and this follows its wording and its status treatment. Satisfied; merged on
  this branch.
- **Next pickup:** none.

## Context

`ComparisonWorkspace` asks `/comparison` once, with `limit: 1000`:

```ts
const payload = await getComparison(comparisonRequestParams(selection, COMPARISON_PAGE_SIZE));
…
message: `${items.length} of ${payload.total ?? items.length} aligned geographies`,
state: "ok",
```

`limit` is capped at 1000 by the route, and a national county comparison
aligns 3,144 geographies. So the workspace holds the **first 1,000 rows
ordered by `geo_id`** — Alabama through part of Illinois — and draws its
scatter plot, its choropleth, and its export from them.

The count is stated, which is better than silence, and it is where this stops
being enough:

- The state is `ok`. A green pill over a third of the country reads as a
  healthy answer, while the explorer's map panel — the model WEB-036
  followed — turns red and says the page bound cut the answer short.
- A scatter plot of alphabetically-first counties is not a sample of the
  United States. It is a systematically biased subset presented as the
  comparison, and no reader can tell from the chart.
- `/comparison` declares `offset` (`ge=0, le=100000`). The rows are reachable;
  the client simply never asks for them.

This is the third instance of the defect WEB-036 names: a bounded read
presented as a whole answer. The other two are fixed.

## Objective

The workspace loads every aligned geography the API will serve for the
selection, and when a bound stops it, says so in the same words and the same
failure-shaped state the explorer already uses.

## Acceptance criteria

1. `/comparison` is paged with `limit`/`offset` until the reported total is
   reached or a declared page bound stops it.
2. The response envelope — units, derivations, caveats, the metric and source
   identities — comes from the first page and is preserved; only `items`
   accumulate.
3. A complete load reports `ok`; a bound-limited load reports a
   failure-shaped state and names the shortfall, in the wording WEB-036
   established.
4. The page bound covers a national county comparison (3,144 geographies)
   with room to spare, so the ordinary case is complete.
5. Paging stops on an empty page, and a resource that published no total is
   never reported as short.
6. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the comparison's compatibility gate, its derivations, or what the
  API computes. The API owns the verdict; this is about how much of its
  answer the client reads.
- Raising the route's `limit` bound.
- Paging the preflight, which is a single verdict rather than a collection.

## Evidence

### The gap, established first

Four unit tests for `fetchComparisonPages` failed on import before the
function existed. A browser test then drove the workspace against a mock that
serves one aligned row per page against a total no number of pages will meet,
and asserted three things the single-request client could not do: the status
names the shortfall, the pill carries the failure class rather than `ok`, and
the request offsets are `0, 1, …` rather than a single `offset=0`.

### What changed

- `fetchComparisonPages` in the API client pages `/comparison` with
  `limit`/`offset`. The envelope describes the pair rather than the page, so
  it is taken from the first response and kept, with every page's rows merged
  into its `items`; only rows accumulate.
- It stops on an empty page, at the page bound, or when the reported total is
  reached, and reports `complete`. A response that published no total is
  never reported as short — the same rule `describeHistoryLoad` follows.
- `ComparisonWorkspace` uses it with an 8-page bound: 8,000 aligned
  geographies against a national county comparison's 3,144, so the ordinary
  case is complete and the bound exists for a grain that grows.
- A complete load now reads `N aligned geographies` rather than
  `N of N`; an incomplete one is failure-shaped and uses the WEB-036 wording.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 234 passed |
| `npm --prefix apps/web run test:browser` | 60 passed (Chromium), up from 59 |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `pytest tests/unit -q` | 1380 passed |
| `python -m tests.support.catalog_evidence` | 304-row register renders; WEB-039 is `FULL` |

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for. The paging contract it would exercise is the API's own
`limit`/`offset` on `/comparison`, which the reviewed OpenAPI snapshot
declares (`offset`, `ge=0, le=100000`) and which API-060 already holds on the
server side.

## Remaining work

None.
