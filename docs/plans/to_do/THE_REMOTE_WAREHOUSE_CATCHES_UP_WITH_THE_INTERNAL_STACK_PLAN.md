---
id: remote-warehouse-catches-up
branch: claude/remote-warehouse-catches-up
depends_on: []
parallel_safe: false
complexity: high
verify:
  - python -m scripts.apply_warehouse_manifest --dsn "$WAREHOUSE_URL" --check
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
---

# The remote warehouse catches up with the internal stack

## Plan status

- **Status:** Unclaimed and **deliberately deferred.** The development target
  is the internal stack; the remote warehouse at `192.168.50.16` is not being
  deployed to until development is at a confident point. Authored 2026-09-18
  from a measured assessment, kept so the measurement is not repeated.
- **Last updated:** 2026-09-18
- **Current milestone:** none. Do not start this before the internal stack is
  where it should be.

## Which warehouse this is about

**Not the one anything is developed against.** Two warehouses exist and only
one matters day to day:

| | Internal stack | Remote |
|---|---|---|
| Where | this machine, `infra/docker/docker-compose.yml` | `192.168.50.16` |
| Started by | `scripts/deploy_stack.py --mode internal` | unknown, older |
| Sources | **all seven**, ingesting | three |
| Relations | 142 | 77 |
| Catalog | 18,198 metrics, 7 sources | 17,788, 3 sources |

Everything is developed and verified against the internal stack. This plan is
the remote one's backlog, for whenever it is next deployed to.

**Do not use the remote warehouse to verify anything.** It is behind in ways
that produce confident wrong answers, and it already produced one: a plan was
filed claiming BLS could not publish a geography-independent measure identity,
because that warehouse is missing `gold_bls.dim_bls_measure` and
`gold_bls.measure_export` and therefore publishes only raw series. The internal
stack publishes seven such identities. The ticket was withdrawn. A warehouse
missing a relation cannot refute a claim about what that relation emits.

## What it is missing

Measured by diffing its `information_schema` against a warehouse this
repository's applier builds: **77 relations against 145. Sixty-eight missing,
none extra** -- behind rather than divergent, so catching up is addition.

| Schema | Missing | What it is |
|---|---|---|
| `silver_fbi`, `gold_fbi` | 20 | the entire FBI UCR stack |
| `silver_nass`, `gold_nass` | 12 | the entire USDA NASS stack |
| `silver_pep`, `gold_pep` | 13 | the entire Census PEP stack |
| `silver_cdc`, `gold_cdc` | 10 | the entire CDC stack |
| `control` | 8 | the data-quality run and result evidence tables |
| `app_api` | 3 | API-owned application storage (ADR-0003) |
| `gold_bls` | 2 | `dim_bls_measure`, `measure_export` |

Those last two are small and disproportionately important: without them BLS
publishes no measure identity a product template can name.

## What it would take, and in what order

*The schema half is cheap, and was verified so.* Twelve manifest assets create
everything except `app_api` (`013`, `009`, `silver-pep`, `015`, `010`, `011`,
`012`, `014`, `019`, `025`, `022`, `gold-pep`). Each was checked against that
warehouse and **none touches a serving table**; their only two row-rewrites
target `silver_pep.pep_dataset` and `silver_fbi.agency_geography_relationship`,
which do not exist there and would be created empty. The constraint
revalidations `manifest-reapply-populated-warehouse` warns about validate
tables that are empty or absent there.

*Two assets are genuinely expensive and are not needed for it.*
`024_served_place_name.sql` and `026_released_date_is_not_the_refresh_clock.sql`
each run six `UPDATE`s over `rpt_acs_observations` (68.3M rows),
`mv_acs_latest` (4.65M), `rpt_bls_observations` (5.1M) and the FRED pair --
roughly 78M rows rewritten per migration. That is a maintenance window.

*Schema parity changes nothing visible.* An empty `gold_cdc` publishes nothing.
Ingesting the four sources is the real cost.

Suggested order: schema parity (whole manifest, through the recording applier);
then ingestion; then the two heavy migrations in a window.

**Apply the whole manifest or none of it.** A partial application leaves a
partial ledger, and `DQ-SHARED-004` then reports every unrecorded asset as
missing -- including ones an older bootstrap really did apply. Its ledger is
empty today and the rule answers `not_applicable`, which is the honest answer;
a partial apply replaces an honest unknown with a misleading "missing".

## Acceptance criteria

- [ ] `apply_warehouse_manifest --check` reports every asset recorded and
      current.
- [ ] `DQ-SHARED-004` answers `pass` there rather than `not_applicable`.
- [ ] Its catalog carries all seven sources.
- [ ] The relation diff against a manifest-built warehouse is empty both ways.

## Already done, 2026-09-18

Its glossary was empty -- `dim_metric_catalog`, `publisher_registry` and
`publisher_harvest_state` all at zero -- and is not any more: all three of its
deployed publishers are harvested, 17,788 metrics.

The harvest could not run before. `harvest_publisher` reads
`publisher_harvest_state.last_content_fingerprint`, a column
`016_publisher_harvest_fingerprint.sql` adds and that warehouse had never
received, so every harvest failed on `UndefinedColumn` and wrote nothing. That
migration is two `ADD COLUMN IF NOT EXISTS` against a table holding zero rows.
It was applied as plain SQL rather than through the recording applier, for the
reason given above: one ledger row would have turned an honest
`not_applicable` into a misleading "42 assets missing".
