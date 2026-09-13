---
id: comparison-uncertainty-caveat
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - published-uncertainty
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A comparison says when it dropped a published uncertainty

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/compatibility.py`

## Context

`ComparisonRow` publishes fourteen fields and none of them is an uncertainty:
`value_a`, `value_b`, `difference`, `ratio`, and the geography and period
attribution. That is a reasonable shape for an aligned comparison — the two
sides' uncertainty vocabularies do not have to be the same, and a difference
of two intervals is a statistic neither source published.

What is not reasonable is saying nothing about it. One of the four
analysis-ready sources publishes an uncertainty:

| Source | Analysis-ready | Publishes |
|---|---|---|
| Census ACS | yes | `margin_of_error`, `margin_of_error_pct` |
| BLS | yes | — |
| FRED | yes | — |
| Census PEP | yes | — |

Census ACS is also the source most comparisons involve, and its county-level
margins are not small. So `/comparison` computes and serves a `difference`
and a `ratio` from two ACS estimates, each of which the same API publishes a
margin of error for on `/observations`, and the response says nothing — while
that response already carries a `caveats` array built for exactly this: the
module's own docstring says an unverified rule "is stated as a caveat instead
of being silently assumed to pass".

WEB-053 just made the explorer carry every uncertainty field the envelope
publishes. This is the same value one route over: the comparison cannot carry
the numbers, so it must carry the fact that they exist.

## Acceptance criteria

1. When either metric's source declares published uncertainty, the
   compatibility verdict carries a caveat naming the source, the fields it
   publishes, and where a caller can read them.
2. The caveat is derived from the reviewed dispatch registry's
   `uncertainty_expressions`, never from a list of sources written beside it,
   so a source that begins publishing one is named without an edit.
3. It is a caveat, not a rule finding: the metrics stay comparable, the
   derivations stay available, and `comparable` does not change.
4. Both `/comparison/preflight` and `/comparison` carry it, because they
   share one verdict.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-096).

## Non-goals

- Carrying the uncertainty values themselves into `ComparisonRow`. A
  difference of two margins is a statistic neither source published, and
  propagating them correctly is a methodological decision this API does not
  make.
- `/distribution/bins`. It has no `caveats` field, so saying the same thing
  there is an additive contract change with its own plan.
- Changing the comparability verdict. A published margin does not make two
  metrics incomparable; it makes the difference less precise than it looks.

## Validation

**Failing first.** `test_a_source_that_publishes_uncertainty_is_named_in_the_caveats`
found no caveat naming Census ACS on a comparable ACS/ACS pair. Its companion,
`test_a_pair_publishing_no_uncertainty_earns_no_such_caveat`, passed both
before and after — silence stays silence for FRED.

**What a caller now sees.** On a pair either side of which publishes one:

> metric_code_a is served by source 'CENSUS_ACS', which publishes
> margin_of_error, margin_of_error_pct; an aligned comparison carries
> neither, so read the published uncertainty on /observations before treating
> a difference or a ratio as exact

`comparable` is unchanged and the derivations stay available: a published
margin does not make two metrics incomparable, it makes the difference less
precise than the numbers look. Both `/comparison/preflight` and `/comparison`
carry it, because they share one verdict, and the web workspace already
renders `caveats` — the list it shows under `verdict-caveats` — so it arrives
in the product without a client change.

**Derived, not listed.** The field names come from the dispatch entry's own
`uncertainty_expressions`. Today that names Census ACS among the four
analysis-ready sources; a source that begins publishing one is named without
an edit, and the test reads the same registry rather than the string.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1454 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 133 passed, 2 skipped |
| Frontend units | `npm --prefix apps/web run test:unit` | 286 passed |
| Frontend browser | `npm --prefix apps/web run test:browser` | 73 passed |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 344 rows. The consumer guide's Analysis section now says what a
comparison cannot carry, beside what its caveats already said about what it
could not verify.

## Remaining work

- None. `/distribution/bins` has no `caveats` field and is named under
  Non-goals; saying the same thing there is an additive contract change with
  its own plan.
