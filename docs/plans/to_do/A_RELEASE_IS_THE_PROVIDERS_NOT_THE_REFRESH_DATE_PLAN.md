---
id: a-release-is-the-providers-not-the-refresh-date
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/shared tests/unit/api -q
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A BLS or FRED release is the provider's publication, not the day the warehouse refreshed

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql`,
  `src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql`,
  `apps/api/registry.py`

## Context

The API guide says `release` and `as_of` "trace a row back to its
publication", that `scope=as_released` lists "every published release", and
`/observations/releases` pages the release identity a source carries. For
Census ACS that identity is `vintage_year`; for CDC it is the provider's
`release_watermark`. For BLS and FRED the dispatch entries publish
`as_of_date` (`registry.py:400-401`, `:641-642`), and `as_of_date` is:

```sql
CURRENT_DATE       AS as_of_date,      -- gold_bls.sql:95
CURRENT_DATE AS as_of_date,            -- gold_fred.sql:51
```

The serving refresh materialises that literal into `rpt_bls_observations`
and `rpt_fred_observations`. A "release" of a BLS series is therefore the
calendar day a chunk of it was last re-served.

## Findings

- The chunk driver re-serves only changed years. Re-serve 2019 on Monday and
  2020 on Tuesday and `GET /observations/releases?metric_code=BLS:LAU:UNEMP_RATE`
  lists two "published releases", each with a row count, and
  `newest_release_per_period=true` picks "the most recently re-served
  chunk" as the settled value. BLS published nothing between them. A full
  reserve collapses every release into one.
- Nothing in silver carries a provider release for BLS: the fact table has
  `ingested_at` and nothing else. The capture's `retrieved_at` is the
  closest honest stand-in for "the publication this row was read from", and
  it is stable across refreshes; `CURRENT_DATE` is not.
- FRED does carry one -- `realtime_start`/`realtime_end` -- and drops it in
  gold; that is its own plan (`fred-revision-identity-reaches-gold`), which
  depends on this one.
- No integration test refreshes the same chunk twice and compares
  `as_of_date`, which is why this has been served since the reporting layer
  was built.

## Acceptance criteria

1. For BLS and FRED, the served `as_of_date` is derived from the row's own
   ingestion evidence (the capture's `retrieved_at` date or the silver
   row's `ingested_at` date -- the plan records which and why), never from
   the refresh's clock. Two refreshes of one unchanged chunk serve the same
   `as_of_date`; a failing-first integration test proves it in
   `test_bls_silver_flow.py` and `test_fred_silver_flow.py`.
2. `/observations/releases` for a BLS metric lists one release per
   distinct ingestion, not per refresh; the catalog/serving agreement tier
   asserts the release count does not change across a refresh with no
   silver change.
3. `API_CONSUMER_GUIDE.md` says, per source, what `release` identifies, and
   for BLS says it is the warehouse's read of the series, not a BLS
   publication, because BLS publishes none in the response.
4. The migration path follows `sql/migrations/README.md`: a new migration
   redefines the two fact views, and the manifest applies it before the
   serving refresh phase.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DB-`
   identifier; DB-035 at authoring time).

## Non-goals

- Inventing a BLS publication calendar. If the provider does not publish a
  release identity in the response, the honest identity is the read.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
