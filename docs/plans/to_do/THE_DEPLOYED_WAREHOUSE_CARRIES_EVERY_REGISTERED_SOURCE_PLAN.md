---
id: deployed-warehouse-carries-every-source
branch: claude/deployed-warehouse-carries-every-source
depends_on: []
parallel_safe: false
complexity: high
verify:
  - python -m scripts.apply_warehouse_manifest --dsn "$WAREHOUSE_URL" --check
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
---

# The deployed warehouse carries every registered source

## Plan status

- **Status:** Unclaimed. Authored 2026-09-18 from a measured assessment of the
  deployed warehouse made while implementing `second-wave-product-templates`.
  The glossary half of that assessment is already fixed; this is the half that
  needs an operator decision and a maintenance window.
- **Last updated:** 2026-09-18
- **Current milestone:** not started. **Do not start without the decision in
  "What has to be decided first".**

## Why

The deployed warehouse is behind the manifest, measured by diffing its
`information_schema` against a warehouse this repository's own applier builds:

> **77 relations deployed, 145 in a manifest build. 68 missing, 0 extra.**

The zero matters as much as the 68: the deployment carries nothing a manifest
build does not, so it is behind rather than divergent, and catching up is
addition rather than reconciliation.

What is missing, by schema:

| Schema | Missing | What it is |
|---|---|---|
| `silver_fbi`, `gold_fbi` | 20 | the entire FBI UCR stack |
| `silver_nass`, `gold_nass` | 12 | the entire USDA NASS stack |
| `silver_pep`, `gold_pep` | 13 | the entire Census PEP stack |
| `silver_cdc`, `gold_cdc` | 10 | the entire CDC stack |
| `control` | 8 | the data-quality run and result evidence tables |
| `app_api` | 3 | API-owned application storage (ADR-0003) |
| `gold_bls` | 2 | `dim_bls_measure`, `measure_export` |

Three consequences, in the order a reader meets them:

1. **Four of the seven registered sources cannot be served.** The API's
   registry dispatches seven; the warehouse can answer for three.
2. **`DQ-SHARED-004` cannot certify anything here.** It answers
   `not_applicable` — correctly, because no manifest asset is recorded — so
   the BLOCK rule that exists to gate publication gates nothing on the one
   warehouse that publishes.
3. **The packaged products state gaps they would not state elsewhere.** Of the
   second wave's 40 slots, 8 report a gap on this deployment, and every one is
   a source that is absent rather than a candidate that is wrong.

The evidence tables in `control` are the quietest of these and not the least
important: without them a certification run has nowhere to write its result.

## What has to be decided first

**Catching up is not one operation, and the expensive half is not the half
that looks expensive.**

*The schema half is cheap and was verified so.* Twelve manifest assets create
everything missing except `app_api` (`013`, `009`, `silver-pep`, `015`, `010`,
`011`, `012`, `014`, `019`, `025`, `022`, `gold-pep`). Every one was checked
against the deployed warehouse and **none touches a serving table**; the only
two row-rewrites they contain target `silver_pep.pep_dataset` and
`silver_fbi.agency_geography_relationship`, which do not exist there yet and
would be created empty. The constraint revalidations that
`manifest-reapply-populated-warehouse` warns about — `019`'s stratum shape and
`015`'s PEP contracts — validate tables that are empty or absent here, so they
are cheap on *this* warehouse and expensive on a loaded one.

*The two assets that are genuinely expensive here are not needed for it.*
`024_served_place_name.sql` and `026_released_date_is_not_the_refresh_clock.sql`
each run six `UPDATE`s across `rpt_acs_observations` (68.3M rows),
`mv_acs_latest` (4.65M), `rpt_bls_observations` (5.1M) and their FRED
counterparts — roughly 78M rows rewritten per migration, twice. That is a
maintenance window, a WAL budget, and a vacuum plan, and it is why
`serving-table-vacuum-hygiene`'s autovacuum thresholds landed first.

*And the schema half does not, by itself, change what anyone sees.* Creating
`gold_cdc` gives an empty publisher. `dim_metric_catalog` will still hold three
sources, and the eight gapped product slots will still gap, until each source
is actually ingested. **Ingestion is the real cost of this plan**: four sources,
their API credentials, their DAG runs, and their history.

So the decision is which of these is being asked for, and they should probably
be taken in this order:

1. **Schema parity only.** Apply the twelve. Low risk, no visible change,
   makes the rest possible and makes the evidence tables exist.
2. **Ingest the four sources.** The visible change. Needs credentials and
   scheduler time, and is the bulk of the work.
3. **The two heavy migrations, and a full recorded manifest apply.** Needs a
   window. Doing this is what lets `DQ-SHARED-004` answer `pass` instead of
   `not_applicable`, because the ledger is only complete when the applier has
   applied everything.

## Deliverables

### 1. Schema parity, applied through the recording applier

`scripts/apply_warehouse_manifest.py` against the deployment. Applying through
it rather than by hand is the point: each asset is recorded as it lands, so the
next reader can ask the warehouse what it carries instead of diffing it against
a container.

**Apply the whole manifest or none of it.** A partial application leaves a
partial ledger, and `DQ-SHARED-004` then reports every unrecorded asset as
missing — including the ones an older bootstrap really did apply. Today the
ledger is empty and the rule answers `not_applicable`, which is the honest
answer; a partial apply would replace an honest "unknown" with a misleading
"missing", which is worse than either.

### 2. The four sources ingested

Through the existing DAGs, in the dependency order
`BETA_RESET_REINGESTION.md` documents. Record which vintages and windows were
loaded, because a source that is present but thinly loaded looks the same as a
source that is absent from a product slot.

### 3. The catalog reflects it

A harvest after ingestion, so `dim_metric_catalog` carries seven sources. The
harvest itself is already proven on this deployment — see below.

### 4. The heavy migrations, in a window

`024` and `026`, with the vacuum expectations from
`BETA_RESET_REINGESTION.md` §7 applied afterwards.

## Acceptance criteria

- [ ] `apply_warehouse_manifest --check` against the deployment reports every
      manifest asset recorded and current.
- [ ] `DQ-SHARED-004` answers `pass` there rather than `not_applicable`.
- [ ] `gold_glossary.dim_metric_catalog` carries all seven registered sources.
- [ ] The second wave's eight gapped slots resolve, or the plan records which
      remain gapped and why — a source can be deployed and still not publish a
      given measure.
- [ ] The relation diff against a manifest-built warehouse is empty in both
      directions.

## What this plan deliberately does not do

- It does not change any DDL. Everything it applies is already reviewed and in
  the manifest; this is an operations plan.
- It does not touch `manifest-reapply-populated-warehouse`, which is about
  *testing* the reapply path. That plan should land first if the heavy
  migrations are going to be run against loaded serving tables, because
  nothing currently exercises them against data.

## Already done, 2026-09-18

Recorded here so this plan starts from the right place rather than repeating
work.

**The glossary was empty and is not any more.** All three deployed publishers
are harvested: `dim_metric_catalog` 17,788 rows (BLS 13,317, CENSUS_ACS 4,447,
FRED 24), `publisher_registry` 3, `publisher_harvest_state` 3 at `success`.

It could not run before. `harvest_publisher` reads
`publisher_harvest_state.last_content_fingerprint`, a column
`016_publisher_harvest_fingerprint.sql` adds and this warehouse had never
received, so every harvest failed on `UndefinedColumn` and wrote nothing. That
migration is two `ADD COLUMN IF NOT EXISTS` against a table that held zero
rows; it was applied on its own and the harvest then succeeded in under twenty
seconds for all three sources.

It was applied as plain SQL rather than through the recording applier, on
purpose, for the reason deliverable 1 gives: one ledger row would have turned
an honest `not_applicable` into a misleading "42 assets missing".
