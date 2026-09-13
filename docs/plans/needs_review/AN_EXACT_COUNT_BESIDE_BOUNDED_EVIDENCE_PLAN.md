---
id: an-exact-count-beside-bounded-evidence
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest tests/integration/database/test_source_quality_checks.py -m "integration and database"
  - pytest tests/unit
---

# A quality result carries an exact count beside its bounded evidence

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/quality/sources.py`

## Context

`DATA_QUALITY_OPERATIONS.md` says what the evidence relation holds, and the
two halves are deliberately distinct:

> | `control.data_quality_result` | rule × object × partition | **exact
> counts**, **bounded evidence ids**, warning review state |

and its own operator query selects `observed_count` as the thing that tells
somebody how bad a failure is:

```sql
SELECT rule_id, severity, object_name, source_code, partition_key,
       observed_count, expected_count, evidence, evaluated_at
FROM control.data_quality_latest_result
WHERE result = 'fail'
```

For fifteen rules in `quality/sources.py` the count is the evidence's length.
Each builds its offender list through `_ids`, whose query ends

```sql
         ORDER BY 1, 2
         LIMIT {EVIDENCE_LIMIT + 1}
```

and whose helper returns `rows[:EVIDENCE_LIMIT]`. The `+ 1` is the standard
trick for detecting "there are more", written deliberately and then sliced
away — and the outcome records `observed_count=len(offenders)`. So
`EVIDENCE_LIMIT` is 20, and a rule with 20 bad rows and a rule with 20,000
both persist `observed_count: 20`.

Nothing is mis-graded: `result` is `fail` either way. What is wrong is the
magnitude, always in the direction of understating it. An operator cannot
tell a handful of rows from a systemic break, and a trend across runs goes
flat the moment it saturates — on the one number the operations guide points
them at.

The module already knows the right shape. `reference_resolution_accounting`
holds its predicate in a `publishable` fragment and reuses it:

```python
uncovered = _ids(cursor, f"SELECT … {publishable} AND NOT EXISTS (…) …")
expected = _count(cursor, f"SELECT COUNT(*) {publishable}")
```

One declaration, two measurements. It does that for `expected_count` and not
for `observed_count`.

## Acceptance criteria

1. `observed_count` is the exact number of offenders for every rule in
   `sources.py`, and `evidence` stays bounded at `EVIDENCE_LIMIT`.
2. The predicate is declared once. A rule must not carry its `WHERE` twice,
   which is the duplication this repository refuses everywhere else.
3. One statement per rule, so the count and the sample describe one reading
   of the warehouse — the same reasoning as API-084 and API-100.
4. The evidence sample stays deterministic: each rule's own ordering decides
   which rows are kept, unchanged.
5. A rule with no offenders still reports `0` and passes; an empty relation
   still answers `not_applicable`.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (DQ-008).

## Non-goals

- Raising `EVIDENCE_LIMIT`. Bounded evidence is the point; the count is what
  was missing beside it.
- `RuleOutcome` gaining a truncation flag. An exact count makes truncation
  self-evident — `observed_count: 20000` beside twenty evidence ids — and a
  new column would have to be persisted and read everywhere.

## Validation

**Failing first**, against a real PostgreSQL with 43 offending ACS slices:

```
FAILED test_slice_ledger_defects_fail_with_bounded_evidence
E   AssertionError: the offender count saturated at the evidence cap: an
E   operator reading it cannot tell a handful of bad rows from a systemic
E   failure
E   assert 20 == 43
```

**Scope, and one more site than expected.** Fourteen rules in `sources.py`
went through the `_ids` helper. `reconciliation.py` had four more written
inline against the cursor, and those were *worse*: they never sliced the
list, so `observed_count` saturated at 21 — `EVIDENCE_LIMIT + 1`, a number
that is not even the cap. The helper moved into `reconciliation.py`, which
already owns `EVIDENCE_LIMIT`, and both modules use it.
`pep_sentinel_conformance` was a fifteenth site in `sources.py` that ran its
own cursor and was converted too.

`verify_capture_checksums` was examined and left alone: its bound is a
declared `capture_limit` scope rather than an evidence cap, and its
`observed_count`/`expected_count` are both exact for the window it says it
measures.

**Two mistakes the tests caught, both mine.**

The first: the mechanical conversion left a trailing positional `params`
tuple after the new `order_by=` keyword, which `ruff` refused to parse. Fixed
by converting those two sites to `params=`.

The second is the more interesting one. The helper guards its contract by
refusing a query that carries its own `ORDER BY` or `LIMIT`, and the first
version tested `"LIMIT" in sql.upper()` — which rejected the USDA NASS rule
for containing the *status literal* `'over_limit'`. The integration suite
failed immediately and named the rule. The guard now matches SQL words
(`\b(?:ORDER\s+BY|LIMIT)\b`), which cannot match inside `over_limit`
because `_` is a word character.

**Why the ordering moved out of the subquery.** `COUNT(*) OVER ()` is
evaluated over the whole offender set before `LIMIT`, so one statement gives
an exact count and a bounded sample. Leaving each rule's `ORDER BY` inside
the wrapped subquery would have made the *sample* depend on a planner
preserving a subquery's sort — true in PostgreSQL today, guaranteed by
nothing. The ordering is applied to the wrapping statement instead, and
`_shifted` moves positional references past the count column so fifteen
rules keep writing the positions of their own select lists. The new node
asserts the sample is sorted, so a lost ordering fails rather than
silently returning an arbitrary twenty.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1488 passed |
| Unit, quality | `pytest tests/unit/quality` | 38 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | **141 passed**, 2 skipped (was 140) |
| Integration, this file | `pytest tests/integration/database/test_source_quality_checks.py` | 7 passed (was 6) |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

One note for a future agent: running `tests/integration/database/` with an
explicit `-m "integration and database"` pulls in `legacy/test_bls_ingest.py`
and `legacy/test_bls_metadata.py`, which call the live BLS API and fail in a
sandbox without it. The documented tier command deselects them (14
deselected), and they touch no code this plan changed.

**Register.** 362 rows.

## Remaining work

- None.
