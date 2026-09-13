---
id: source-route-code-sweep
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - source-route-grain-sweep
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit/shared -q
---

# A catalog code answers on every route that accepts one

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row DB-031.
- **Last updated:** 2026-09-13
- **Owner surface:** `tests/integration/api/test_catalog_serving_agreement.py`

## Context

DB-025 sweeps every registered source's current catalog codes and requires
each to answer — on `/api/v1/observations`. DB-030 has just extended the
*grain* sweep to each source's own `/{segment}/observations/latest`, and in
doing so found that no Census PEP code answered there at all, because the
relation composes an identity the catalog does not publish.

The fix applies to both halves of that route family: `metric_match_condition`
is read by the latest read and the timeseries read alike. Only the latest half
is guarded. `/{segment}/observations/timeseries` accepts no `geo_level`, so
the grain sweep does not reach it — but it accepts the same `metric_code`,
and it was equally unanswerable for every PEP metric.

That is the half of DB-025's question nothing asks: a code the catalog
publishes must answer on every route that takes one, not only on the neutral
resource and not only on the route the grain sweep happens to visit.

## Acceptance criteria

1. Every serving contract's `/{segment}/observations/timeseries` is asked for
   each current catalog code of its source, and answers.
2. The geography it asks for is one the same source actually published, read
   from the latest route's own answer rather than assumed, so the sweep
   cannot pass by asking for a geography nothing covers.
3. A source with no catalog content contributes nothing, and the guard says
   so rather than passing vacuously — the same shape DB-025 uses.
4. Reverting the metric-identity declaration makes this fail, naming the
   route and the code.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Sweeping routes that take no `metric_code`.
- Seeding the remaining sources. Census ACS, FRED and Census PEP are
  published end to end by fixtures; the sweep exercises whatever the
  warehouse under test holds.

## What was built

`test_every_catalog_code_answers_on_every_route_that_accepts_one` sweeps each
serving contract's timeseries route for each current catalog code of its
source. The geography it asks for comes from that source's own latest answer,
so the sweep cannot pass by asking for a geography nothing covers — and if the
latest route itself answers nothing, that is reported as the unresolvable code
it is rather than skipped.

It borrows DB-025's shape for emptiness: a source with no catalog content
contributes nothing, and a final assertion stops the whole sweep from passing
vacuously on a warehouse with no content at all. It adds one of its own —
Census PEP must be among the sources exercised, because PEP is the source
whose relations compose an identity the catalog does not publish, and a sweep
that skipped it would prove nothing about the case it exists for.

No production code changed. DB-030 already taught the serving contracts how
their relations compose an identity, and that declaration is read by the
timeseries read as well as the latest one; this is the guard for the half
that was fixed but unwatched.

## Validation

Run 2026-09-13 on this branch, against a live PostgreSQL 16 with PostGIS.

| Tier | Command | Result |
|---|---|---|
| This file | `pytest tests/integration/api/test_catalog_serving_agreement.py -m "integration and database and not slow"` | 7 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1439 passed |
| Register | `python -m tests.support.catalog_evidence` | 333 rows; DB-031 is `FULL` |
| Format | `ruff format --check .` | 440 files already formatted |

Failing-first was established the same way DB-030's was: removing Census PEP's
`metric_match_condition` makes this report `CENSUS_PEP publishes current
catalog code 'CENSUS_PEP:AGREEMENT_…', which /api/v1/pep/observations/latest
answers with no rows`.

## A formatting miss this change also carries

The DB-030 commit failed CI's `lint` job on
`ruff format --check .`: the integration test file was never formatted,
because the format step that session was run against the files changed by
hand rather than against the repository. `ruff format .` is what the workflow
checks, and it is what this change ran. One file was reformatted; nothing
else in 440 moved.

## Acceptance criteria, as delivered

1. **Met.** Every serving contract's timeseries route, per current code.
2. **Met.** The geography comes from the latest route's own answer.
3. **Met.** DB-025's emptiness shape, plus the PEP assertion.
4. **Met.** Reverting the declaration reproduces the failure with the route
   and the code named.
5. **Met.** `DB-031` in `docs/reference/TESTING_CONTRACT.md`, owned by
   `postgres-integration`, with `AUDITED_COUNTS["DB"]` raised to 31.

## Remaining work

- None.
