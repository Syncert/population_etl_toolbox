---
id: every-declared-serving-expression-executes
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# Every declared serving expression is SQL the warehouse accepts

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13. **No live defect: the guard passed on the first run, which is the answer to the question it was written to ask.**)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/integration/api/test_dispatch_expressions_execute.py`

## Context

`apps/api/registry.OBSERVATION_DISPATCH` is a set of reviewed SQL fragments
per source: the relation for each scope, the identity column or lineage key,
the period and release expressions, the geography expressions, each published
dimension, each filter condition, and the paging order. A typo in any of them
is a statement PostgreSQL refuses, which the API answers as a sanitized 503
with the relation name in a log nobody is reading yet.

I went looking for what proves they run, and the honest answer was: only for
FRED.

- `test_real_database_contract` drives the neutral resource for the one
  seeded metric, which is FRED's.
- `test_catalog_serving_agreement` sweeps **every** source, but through the
  catalog: it asks each source for the metrics it publishes and then queries
  those. On a clean bootstrap six of the seven sources publish nothing, so
  the sweep passes over them with nothing to ask. Its own docstring says as
  much about the grain sweep ("the ACS fixture publishes exactly the grain it
  seeded, so the guard is exercised on every warehouse rather than only where
  content happens to exist") — that reasoning applies to one fixture, not to
  the other six sources' expressions.

So a broken dimension expression, order column or filter condition for CDC,
FBI UCR, USDA NASS, BLS, ACS or PEP would reach a deployment.

## Acceptance criteria

1. An integration module drives `list_neutral_observations`, the production
   path, for every source in `OBSERVATION_DISPATCH`, with only the catalog
   lookup stubbed, so each declared expression is executed by PostgreSQL.
2. Three kinds of node per source: both scopes; every declared filter, one
   at a time, read from the same `supported_filters()` the capability
   resource publishes; and both ranked reductions, where a source that
   refuses a reduction must explain rather than execute.
3. A broken expression of any kind fails the guard by name (proved by
   breaking a dimension expression and an order column).
4. The behaviour is `TESTING_CONTRACT.md` catalog row API-128 with CI
   ownership.

## What was changed

A new integration module drives the **production path** —
`neutral_observations_service.list_neutral_observations` — with only the
catalog lookup stubbed. The stub is a synthetic metric row shaped the way
each registry entry says that source's rows are identified: a metric-code
column for some, a lineage `key` for others, and the lineage's
`identity_columns` for the rest, each filled with a value no row can hold.

Every statement therefore runs against the real serving relations and answers
zero rows, which is exactly what proves each expression resolves. Three
nodes per source: both scopes, every declared filter one at a time (read
from the same `supported_filters()` the capability resource publishes), and
both ranked reductions — with a source that refuses a reduction asked for it
and required to explain rather than execute.

## Validation

28 nodes pass against the bootstrapped warehouse. Proved by breaking one
expression of each kind:

```text
# a dimension expression
E  psycopg2.errors.UndefinedColumn: column "subject_typo" does not exist
E  LINE 18:             subject_typo AS dim_subject_type,

# an order column
E  psycopg2.errors.UndefinedColumn: column "as_of_dat" does not exist
E  LINE 16:         ORDER BY observation_date, geo_id, as_of_dat, series...
E  HINT:  Perhaps you meant to reference the column "rpt_bls_observations.as_of_date".
```

Register: the behaviour is catalog row **API-128** (`Integration / api
database`) in `docs/reference/TESTING_CONTRACT.md`, owned by the
`api-integration` job per `docs/reference/CI_EVIDENCE_MAP.md`;
`python -m tests.support.catalog_evidence` grades it `FULL` (re-run
2026-09-14 on the review head: 459 rows, every one `FULL`).

## Deliberately not done

- **The values are not checked.** This asks whether each statement runs, not
  whether it returns the right rows; a metric code nothing can hold is what
  makes it independent of content, and that independence is the point. What a
  row should contain is `test_real_database_contract`'s question, and the
  seeded fixtures answer it per source as they are written.
- **The source-scoped routes are not swept.** They have their own SQL
  builders rather than dispatch entries, and each has an integration module
  against a seeded fixture (`test_usda_nass_api_contract`,
  `test_cdc_pipeline`). The registry is the surface where one typo is
  invisible because six sources share the mechanism.

## Remaining work

- None. Review is the remaining step.
