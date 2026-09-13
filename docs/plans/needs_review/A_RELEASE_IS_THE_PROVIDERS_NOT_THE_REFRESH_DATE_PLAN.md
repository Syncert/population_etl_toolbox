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

- **Status:** Needs review. Implemented 2026-09-13.
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

- **Criterion 1 — `ingested_at`, and why rather than the capture's
  `retrieved_at`.** The three fact views now read `s.ingested_at::DATE AS
  as_of_date`.
  - it is already on the fact row. `retrieved_at` lives in
    `raw_capture.response_capture`, reachable only through the revision, so
    using it would add a join to a view the serving refresh reads for every
    row it writes -- 68 million on ACS.
  - it is exactly stable when it should be. ETL-037's silver upsert advances
    `ingested_at` only when the row's own content changed (`ingested_at =
    EXCLUDED.ingested_at WHERE (...) IS DISTINCT FROM (...)`), so it holds
    across a re-serve and moves when the value moves -- which is the property
    a release identity needs and the only property `CURRENT_DATE` lacked.
  - `updated_at` already published `s.ingested_at`, so `as_of_date` and
    `updated_at` are now one fact about the row rather than two unrelated
    clocks. The consumer guide says `release` and `as_of` "trace a row back to
    its publication"; they now trace to the same thing.
- **A third source, named here because the plan named two.** ACS published
  `CURRENT_DATE AS as_of_date` as well. Its *release* identity is
  `vintage_year`, so the plan scoped itself to BLS and FRED -- but the
  registry's `as_of_expression` for ACS is `as_of_date::TEXT`, so ACS's `as_of`
  was the refresh clock too, on the field the same guide sentence covers. It
  is the same one-line rule in the same view family, so it is fixed here and
  called out rather than left as a known divergence. Census PEP already uses
  the Bureau's own `release.release_date` and needed nothing.
- **Criterion 1, the tests.** Both nodes are failing-first by construction:
  the row's ingestion is *back-dated* before the refresh runs, because a test
  that ingests and serves in one session sees today's date either way.
  - `test_fred_silver_flow.py::test_a_fred_release_is_the_ingestion_not_the_refresh`
    seeds silver with `ingested_at = 2024-06-15`, runs the real
    `refresh_dashboard_serving_layer_fred` **twice**, and asserts the served
    `as_of_date` is that date both times and that the metric has exactly one
    distinct release. Break-test (restoring `CURRENT_DATE` in the BLS view,
    same shape): `[('2026-09-13', '2097-06-15')] != [('2097-06-15',
    '2097-06-15')]` -- the clock on one side, the ingestion on the other.
  - BLS's release assertions are in the catalog/serving agreement tier
    (below) rather than in `test_bls_silver_flow.py`, because that file
    transforms to silver and never calls a serving refresh: running one there
    needs `dim_bls_survey`, `dim_bls_measure` and `dim_bls_series`, which is
    the agreement tier's `served_bls_series_identity` fixture. What
    `test_bls_silver_flow.py` gains instead is the *foundation* the identity
    rests on, which nothing proved against a database: a re-transform of
    unchanged content leaves `ingested_at` alone. ETL-037 asserts that
    predicate as a string in the transform's source; FRED's flow already had
    the database node, and BLS did not.
- **Criterion 2.** `test_catalog_serving_agreement.py::test_re_serving_a_chunk_publishes_no_new_release`
  back-dates the fixture's silver row, refreshes, asserts the served
  `as_of_date` equals it, refreshes again with nothing changed in silver, and
  asserts the date and the distinct-release count are unchanged -- one release
  per distinct ingestion, which is what `/observations/releases` pages.
- **Criterion 3.** `API_CONSUMER_GUIDE.md` gains a per-source table of what a
  release identifies: the provider's own identity for ACS (`vintage_year`),
  CDC and NASS (`release_watermark`), FBI (the release key) and PEP (the
  release date), and for BLS and FRED the date the warehouse read the series
  -- stated as **not** a provider publication, with the reason (BLS publishes
  none; FRED's `realtime_start` is dropped before the serving layer, which is
  its own plan). It also says what changed and why the distinction matters.
- **Criterion 4, with a deviation recorded.** Migration 026 is registered in
  the manifest and mounted in the test stack, and it **corrects the dates
  already materialised** (`as_of_date = updated_at::DATE`, which in those
  relations *is* the silver row's `ingested_at`, so the correction is exact
  and needs no re-serve of 68 million ACS rows). It does **not** restate the
  three view definitions, which the criterion asked for: those live in the
  phase DDL files the bootstrap applies in the `gold` phase before this step
  and the ingestion DAGs re-apply on every run, and a second copy of a
  sixty-line view body in a migration is a copy that drifts from the one the
  warehouse actually gets. This is the same route DB-037 took for the same
  files, and a fresh bootstrap confirms it (below).
- **Criterion 5.** `DB-039` is in `TESTING_CONTRACT.md` (the plan's `DB-035`
  guess was four behind the register), the family range reads
  `DB-001–DB-039`, `AUDITED_COUNTS["DB"]` is 39, and the totals are 406.
- **A static guard so it cannot come back.**
  `test_incremental_serving_contract.py::test_a_served_release_is_never_the_refresh_clock`
  reads every `AS as_of_date` expression in `src/**/gold_*/DDL/*.sql` --
  globbed, not listed, so a fourth source is covered the day it is written --
  and fails on `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `NOW()`
  or `STATEMENT_TIMESTAMP`. Break-test: restoring the BLS literal fails with
  `gold_bls.sql: CURRENT_DATE       AS as_of_date,`.
- **A defect this work introduced, and what it cost.** The first version of
  the FRED node back-dated `ingested_at` to `2099-06-15` -- inside the
  fixture's 2099 observation window but three quarters of a century in the
  *future* -- and cleaned up nothing. The forced refresh it ran advanced
  `control.serving_refresh_state` for FRED to that date, so
  `test_forced_full_reserve.py` then planned no work at all and failed with
  `0 == 2`, in the same run and in isolation afterwards. The date is now in
  the past and the node deletes the three kinds of state it owns: its served
  rows, its silver row, and the source's serving watermark. The full tier was
  then run twice back to back, 167 passed both times, which is the property
  `test_tier_repeatability.py` exists for.
- **The containment sweep's bound, decided after it failed.** DB-038's sweep
  over all three reporting tables failed on
  `gold_fred.rpt_fred_observations | us:1`: FRED tests leave served rows for
  `us:1`, and `test_reference_dimensions.py`'s own fixture removes `us:1` from
  the reference at teardown, so a served row outlived the reference row it
  names. The sweep is now bounded to geographies the reference still carries,
  and the bound is the principled one rather than a convenience: the
  projection can only publish what the reference holds, so a served row whose
  geography the reference has forgotten entirely is a different defect and not
  one this projection could fix. Inside that bound the rule is exactly the one
  the deleting refresh broke -- the retired place is inactive in the reference
  and still served -- and the test now also asserts that place is *inside* the
  bound, so the sweep cannot pass by excluding the one row it exists for.
  Proved to still catch an orphan: a reporting row for an entity with no
  catalog row is returned, naming the relation and the `geo_id`.
- **A genuinely fresh warehouse.** Applying every manifest asset in order into
  a new `release_fresh_test` database printed `FRESH BOOTSTRAP OK`, and all
  three fact views' stored definitions carry `(ingested_at)::date AS
  as_of_date`. The database was dropped afterwards.
- **Tiers.** `pytest tests/unit` 1588 passed. `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 167 passed, 2 skipped,
  14 deselected -- twice in a row. `ruff check .` and `ruff format --check .`
  clean.

## Remaining work

- None. FRED's own revision identity (`realtime_start`) reaching gold is the
  next plan, `fred-revision-identity-reaches-gold`, which depended on this
  one: what a FRED release *means* had to be settled before its provider
  identity could replace the read.
