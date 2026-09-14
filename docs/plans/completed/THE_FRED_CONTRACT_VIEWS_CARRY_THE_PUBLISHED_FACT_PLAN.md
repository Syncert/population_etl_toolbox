---
id: the-fred-contract-views-carry-the-published-fact
branch: claude/iterate-plans-improvements-ir885c
depends_on: [the-audit-of-what-every-note-claims]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
---

# The FRED contract views carry the published fact, unaltered

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13. **The first of the declared-but-unbuilt BLOCK rules to gain an executor: 24 remain.**)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/quality/sources.py`,
  `src/data_ingestion_toolbox/quality/inventory.py`,
  `tests/integration/database/test_fact_quality_injections.py`,
  `docs/reference/DATA_QUALITY_OPERATIONS.md`

## Context

The audit that ran through DQ-013 to DQ-016 established which unimplemented
rules were really unimplemented. This picks the most valuable of those and
builds it.

`DQ-FRED-007` — "FRED serving contract views preserve the published fact's
identity, values, and metric codes", BLOCK, conformance — was declared by
DQ-001 with the note "no executor confirms" it. Nothing anywhere confirmed
that what the API serves is what the warehouse published. A served row with
no backing fact, or a value the fact does not hold, is a number the API
presents and the warehouse does not have; that is the worst thing this system
can do, and it was unmeasured.

It is also the safest of the 25 to implement, which is why it went first: a
correct warehouse passes it by construction, so enabling it cannot block a
publication that should have proceeded.

## What the rule measures, and what it deliberately does not

**Measured — the conformance direction, which cannot lag.** Every row a
contract view serves must trace to a published fact with

- the metric code derived from that fact's own series (`'FRED:' ||
  series_id`, which is how the refresh builds it), so a code naming no
  series fails;
- the same value;

plus two identity claims: `v_metric_latest_by_geo` must be a reduction of
`fact_observation` rather than a second source, and every served metric code
must be one `metric_publisher` exports — a measure served and not exported is
one the catalog cannot describe.

**Not measured — completeness.** The serving layer is rebuilt a calendar year
at a time with a commit per chunk (DB-041), so a fact published after the last
refresh is legitimately absent from the views. DQ-FRED-002 measures that
ledger. Failing it here would make the rule fire on every warehouse between
an ingest and its next serve.

For the same reason the value comparison **exempts a fact revised after the
refresh watermark**: ETL-037 advances `ingested_at` only when a row's content
changed, so a revision inside that window is a served value the next refresh
will replace, not one the serving layer invented. `control.serving_refresh_state`
is where that watermark lives, and DB-039 made the two clocks one fact
(`as_of_date` and `updated_at` both come from `ingested_at`), which is what
makes the comparison like-for-like.

A source with **no** refresh-state row is read strictly rather than leniently.
The chunked driver seeds that row before it refreshes anything, so its absence
means no refresh has run and any served rows are the anomaly the rule is for.

## Validation

Six nodes in `tests/integration/database/test_fact_quality_injections.py`,
each against a warehouse the production path built — the gold element
refresh, the publisher harvest, then the serving refresh — because a
hand-built serving row would prove nothing about that path:

- the rule passes on what the pipeline published
- an invented served row fails, naming the metric code and date
- a served value the published fact does not hold fails
- **a fact revised after the refresh watermark passes** — the exemption
- a latest row the as-released view does not carry fails
- a served metric the publisher export omits fails, and the identity check
  catches it too

Writing those injections found this executor's own first defect. The
watermark exemption defaulted a missing state row to `-infinity`, which made
every fact "ingested after the last serve" and exempted all of them — so the
altered-value injection passed the rule. The test caught it immediately,
which is the whole argument for injecting the defect rather than asserting
the happy path: a rule that cannot fail is worse than no rule, because it
reports a pass.

## Deliberately not done

- **The three sibling rules stay unimplemented.** `DQ-ACS-007`,
  `DQ-BLS-007` and `DQ-PEP-007` claim the same thing of their own sources,
  and the shape of this executor generalises — but each source's refresh
  derives its metric code differently (ACS by dataset, vintage and variable;
  PEP across overlapping products) and a generic version would either
  re-derive four refresh procedures or compare nothing specific. One at a
  time, with its own injections, is how DQ-004 did the reconciliations.
- **The resolution rules (`DQ-ACS-004`, `DQ-BLS-004`, `DQ-CDC-005`,
  `DQ-NASS-004`) stay unimplemented.** They count what the serving refresh
  *dropped*, and a legitimately-unresolved population exists in at least one
  source — `gold_pep`'s own DDL comments record 1.6M rows whose geography
  never resolved. A BLOCK rule that fires on a known and accepted population
  would block publication for a state the warehouse already declares, so
  those need the accepted population declared first, which is a decision
  about each source rather than a measurement.
