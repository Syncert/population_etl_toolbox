---
id: distribution-empty-bins
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# A distribution reports every bin it was asked for

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the analysis response shapes.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/distribution_service.py`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`GET /api/v1/distribution/bins?bin_count=N` builds its `items` from the rows
a `GROUP BY bin_index` returns, so a bin no geography falls into produces no
row and appears nowhere in the response. A caller asking for 7 bins over a
long-tailed measure — county population, say — can receive 2.

The response still declares `bin_count: 7`, `min_value`, and `max_value`, so
the missing bins carry no information a caller could not recompute; the
omission only forces every consumer to recompute them. `apps/web` does
exactly that: `explorerViewModel.ts::distributionBins` builds a
`Map(bin_index -> count)` and then walks `1..bin_count` filling the gaps.
Every other consumer has to write that loop or draw a histogram whose bars
are adjacent when they should be separated by empty ranges.

It is also the repository's own failure mode, in its own words: an absent
bin and a bin containing zero geographies are different statements, and
returning the first where the second is true asks the caller to infer a fact
the API measured. `AGENTS.md` states the rule for values — never silently
convert missing to zero — and the converse holds here: never present a
measured zero as absent.

## Objective

`items` has exactly `bin_count` entries covering the full range, each with
its exact count.

## Acceptance criteria

1. A response with `total > 0` and `min_value != max_value` carries exactly
   `bin_count` items, `bin_index` 1..`bin_count` in order, with contiguous
   bounds from `min_value` to `max_value`.
2. A bin no geography falls into carries `count: 0`, not absence.
3. Counts still sum to `total`, and every non-empty bin's count is unchanged.
4. The degenerate cases are unchanged: no numeric values answers `total: 0`
   with no items and null bounds; a single distinct value answers one bin
   whose bounds are that value.
5. The consumer guide states the shape, and the behavior is a
   `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the binning method (equal width over the observed range) or the
  `bin_count` bound.
- Changing what is counted: null, suppressed, and missing values stay
  excluded rather than binned.
- Quantile, logarithmic, or Jenks bins. `apps/web` offers a logarithmic
  legend computed locally and says so; an API-derived alternative is a
  separate contract, not a silent change to this one.

## Evidence

### The gap, established first

`test_every_bin_asked_for_is_reported` asks for 5 bins over a 0..100 range
whose grouped rows land only in bins 1 and 5, and asserts
`bin_index == [1,2,3,4,5]` with `count == [2,0,0,0,1]`. It failed first: the
response carried two items, `bin_index` 1 and 5, with a 20..80 gap the
caller had to infer.

### What changed

- The service reads the grouped rows into a `bin_index -> count` map and then
  emits `1..bin_count`, filling absent bins with `0`.
- The last bin still closes on the observed maximum rather than on
  `min + n*width`, unchanged — floating-point width must never leave the
  largest value outside the range it was binned into.
- Degenerate answers are untouched, and a second test pins both: no numeric
  values still answers `total: 0` with null bounds and no items, and a single
  distinct value still answers one bin whose bounds are that value. Neither
  is a `bin_count`-length list, because neither has a range to divide.

### The one consumer, checked

`apps/web/lib/explorerViewModel.ts::distributionBins` already builds a
`Map(bin_index -> count)` and walks `1..bin_count`, so it was doing this
reconstruction itself and reads the fuller response identically. Its 220
unit tests pass unchanged, which is the evidence that this widening is not a
break for the repository's own client.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_distribution.py -q` | 13 passed |
| `pytest tests/unit -q` | 1380 passed |
| `npm --prefix apps/web run test:unit` | 20 files, 220 tests passed |
| `python -m tests.support.catalog_evidence` | 300-row register renders; API-079 is `FULL` |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 440 files already formatted |

### Not run

No integration or e2e tier is implicated: the binning arithmetic happens in
the service over counts the database returns, and the unit tier drives it
through the served HTTP surface. The reviewed OpenAPI snapshot is unchanged —
`DistributionBin` already declared every field; only how many of them are
returned changed.

## Remaining work

None.
