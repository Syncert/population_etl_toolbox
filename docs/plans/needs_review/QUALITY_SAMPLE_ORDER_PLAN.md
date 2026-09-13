---
id: quality-sample-order
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The quality screen's sample can contain the problem it reports

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the data-quality screen.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/dataQuality.ts`,
  `apps/web/components/DataQualityExplorer.tsx`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`/quality` does two things in sequence. Its rollup reports, from
`/catalog/freshness`, that a source has — say — "12 stale of 2,487 published
metrics". Its per-measure table then loads every metric for that source and
shows `metricRows.slice(0, 40)`.

`/catalog/metrics` orders by `metric_code`, so those 40 are the
alphabetically first: for Census ACS, forty codes beginning `B01001…`. The
twelve stale measures the screen just reported are in there only by
coincidence, and with 2,487 metrics the coincidence is a one-in-sixty-two
chance per measure.

So the screen states a problem and then shows a sample that almost certainly
cannot contain it. A reader who came to find out *which* measures went stale
has no way to reach them: there is no filter, no sort, and the table is
capped at forty.

The fix is not a score or a ranking. `freshness_state` is the warehouse's own
published vocabulary — `current`, `stale`, `retired`, and absent — and this
module's own doc says those states stay distinct. Ordering the sample by that
published field puts the measures a reader is looking for where a reader can
see them, without computing anything.

## Objective

The per-measure sample is ordered by the published state that brought the
reader to the screen, and says so.

## Acceptance criteria

1. `metricQualityRows` orders by published `freshness_state` in the order a
   reader needs it: `stale`, then a state the publisher did not publish, then
   `retired`, then `current`, then anything else the vocabulary later adds.
2. Within a state, rows keep a deterministic order by metric code, so the
   sample is reproducible and two loads agree.
3. No row is dropped, no state is merged into another, and no field is
   computed: the ordering is the only change to what the function returns.
4. The screen states the order beside the "showing N of M" count, so the
   sample is not mistaken for the catalog's own order.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- A quality score, index, grade, or percentage. The module forbids them and
  this plan does not introduce one by the back door.
- Filtering or sorting controls on the table; the fix is to make the default
  sample answer the screen's own question.
- Raising the forty-row sample, or paging the table.
- Changing the rollup, the coverage segments, or the evidence-location
  guidance.

## Evidence

### The gap, established first

Three unit tests failed first: `metricQualityRows` returned its input order,
so a `current` measure that sorted early stayed ahead of a `stale` one. The
browser test then reproduced the screen's own version of the problem — the
BLS fixture gained a stale measure whose code sorts last
(`BLS:LAU:ZZ_STALE_MEASURE`), which is exactly the row an alphabetical sample
cannot reach — and asserted it now leads the table.

### What changed

- `metricQualityRows` sorts by `attentionRank(freshness)` and then by metric
  code. The rank is an index into one declared list; a word the vocabulary
  adds later falls off the end of it and sorts last, keeping its published
  value rather than being folded into a state this client recognises. A
  third unit test pins that.
- `QUALITY_SAMPLE_ORDER` is one sentence the screen renders beside its
  "showing N of M" count, so the sample is not mistaken for the catalog's
  own order.
- Nothing is dropped, merged, or computed. The module's standing rule — no
  score, index, grade, or percentage — is untouched, and the ordering is over
  a field the warehouse published rather than anything derived from it.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 239 passed |
| `npm --prefix apps/web run test:browser` | 61 passed (Chromium), up from 60 |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `pytest tests/unit -q` | 1382 passed |
| `python -m tests.support.catalog_evidence` | 307-row register renders; WEB-041 is `FULL` |

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for. It would add nothing: the ordering is a pure function over
catalog rows, and the browser tier drives it through the real screen.

## Remaining work

None.
