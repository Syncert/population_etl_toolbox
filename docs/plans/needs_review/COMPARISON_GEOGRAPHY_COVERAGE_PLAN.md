---
id: comparison-geography-coverage
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - distribution-one-snapshot
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# A comparison says how much of each side it could not pair

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row API-087.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/comparison_service.py`,
  `apps/api/schemas/analysis.py`
- **Depends on:** API-084, whose lesson this follows: counts that are read
  together are measured in one statement.

## Context

`/comparison` reduces each side to one newest value per geography and joins
them:

```sql
FROM side_a JOIN side_b USING (geo_id)
```

An inner join. A geography one side publishes and the other does not is not
in the answer at all — not as a row with a null value, not in `total`, not in
any field of the response. `total` counts the intersection, and nothing says
what it is an intersection *of*.

That is a silent narrowing of the question. A county-level comparison of a
measure covering all 3,143 counties against one covering 500 answers 500
rows and reports `total: 500`. Read as it is written, that says "500
counties" — and a reader has no way, from this response, to learn that
2,643 counties were dropped, or which side dropped them.

The route is careful about the adjacent cases. A geography whose value is
null on one side *is* in the answer, carrying its null, because the API
refuses to coerce a missing value to zero. Its `caveats` name every rule the
publication left unverifiable. Its `derivations` name every field the API
computed rather than a source publishing. The one thing it does not report
is the size of the set it silently intersected.

This is the same defect class as API-074 and API-084, one join over: a
response that counts what it answered without saying what it did not.

## Acceptance criteria

1. The response reports how many geographies each side published under the
   request's own filters, alongside the count it paired.
2. The three counts are measured in one statement over one evaluation of the
   two reductions, so they describe one reading of the warehouse and cannot
   disagree (API-084).
3. `total` keeps its meaning — the rows this request can page — so no
   existing consumer's paging changes.
4. The counts are exact counts of the reduced sides, not estimates, and a
   comparison that pairs everything reports all three equal.
5. The addition is additive under ADR-0002: it lands in v1, the reviewed
   OpenAPI snapshot is regenerated, and the consumer guide states what the
   fields mean.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Turning the join into an outer join. "Compare A and B" asks for the pairs;
  a row where one side publishes nothing for a geography would carry no
  derivation and is a different question.
- Reporting *which* geographies were dropped. That is a listing, and it is
  already reachable by asking each measure's own `/observations`.

## What was built

`ComparisonResponse` gained `geographies_a` and `geographies_b`, and the
count query became one statement answering three scalars:

```sql
SELECT
    (SELECT COUNT(*)::INT FROM joined)  AS total,
    (SELECT COUNT(*)::INT FROM side_a)  AS geographies_a,
    (SELECT COUNT(*)::INT FROM side_b)  AS geographies_b
```

One statement rather than three, for the reason API-084 established: these
counts are read *against* each other — `total` is only meaningful beside
what it is an intersection of — so measuring them separately would let a
refresh land between them and report a narrowing that never happened.

Both fields default to `0` in the schema, so the addition is additive: a
client that does not read them is unaffected, and the response shape gains
two integers under v1.

`total` is unchanged in meaning and in value. Nothing about paging moves.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Comparison unit | `python -m pytest tests/unit/api/test_comparison.py -q` | 12 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1412 passed |
| Contract snapshot | `python -m tests.support.regenerate_openapi_contract` | two integer properties added; nothing else moved |
| Register | `python -m tests.support.catalog_evidence` | 322 rows; API-087 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

All four tests were confirmed failing-first: the two coverage fields were
absent from the response and from the served schema, and the counting
assertion reported that `FROM side_b` did not appear in the single
`COUNT` statement.

### Against a real database

The counting statement was run on a live PostgreSQL 16 over two sides shaped
like the reductions: side A publishing `g1`, `g2`, `g3` and side B publishing
`g1`, `g4`. It answered `total 1, geographies_a 3, geographies_b 2` — the
answer carries one pair, and the response now says that three and two
geographies went into it.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide.

## A note on the test harness

`_ComparisonSession` matched the counting statement by its exact old text
(`"COUNT(*)::INT FROM joined"`) and answered a scalar. It now matches any
counting statement and answers a row, which is both what the service reads
and a fixture that will not silently stop matching the next time this SQL is
edited.

## Acceptance criteria, as delivered

1. **Met.** `geographies_a` / `geographies_b` beside `total`.
2. **Met.** One statement, asserted by
   `test_comparison_counts_are_measured_in_one_statement`.
3. **Met.** `total` is still `COUNT(*) FROM joined`; no existing assertion
   about it changed.
4. **Met.** Exact `COUNT(*)`s over the reduced sides;
   `test_a_comparison_that_pairs_everything_reports_equal_counts` covers the
   all-paired case.
5. **Met.** Snapshot regenerated (two additive properties), and the consumer
   guide now states both what the fields mean and that the two sides are not
   aligned to a shared period.
6. **Met.** `API-087` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 87.

## Remaining work

- None. A consumer-side row surfacing these counts in the comparison
  workspace would be a separate plan.

