---
id: acs-catalog-metric-code-mismatch
branch: fix/acs-catalog-metric-code
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
---

# Every ACS metric the catalog advertises is unresolvable through the API

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. ACM-001 through ACM-004 delivered with evidence below; the forced full re-serve completed on the development stack on 2026-09-12 and every acceptance check passed.)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/census_acs/gold_census/DDL/`,
  `src/data_ingestion_toolbox/glossary/harvest.py`, `apps/api/registry.py`,
  `tests/integration/database/`
- **Depends on:** nothing open. Found on 2026-09-12 while validating the ACS
  geography re-serve in `completed/FORCED_FULL_RESERVE_SCALE_PLAN.md`; the
  defect is older than that work and independent of it.

## Implementation checkpoint

**Last updated:** 2026-09-12

**Current milestone:** complete. Every phase has inspectable evidence and the
development warehouse verification is recorded under "ACM-004 on the
development stack".

**Next pickup:** none.

### Completed in the current slice

- [x] ACM-001 decide which spelling is canonical and record why
- [x] ACM-002 make the two sides agree
- [x] ACM-003 a guard that fails when a catalog code cannot be served
- [x] ACM-004 re-serve or re-harvest as the decision requires

## Objective

Make a metric code taken from the catalog answerable by the API. Today an ACS
code read from `/api/v1/catalog/metrics` returns zero rows from
`/api/v1/census/observations/latest`, because the catalog and the serving
layer spell the same metric two different ways. The catalog is the published
discovery surface — a consumer that follows it correctly gets nothing back, and
nothing in the stack reports that.

## Evidence gathered 2026-09-12

Against the development warehouse, immediately after a clean forced full
re-serve of `CENSUS_ACS` (20/20 chunks `COMPLETE`, 0 failed).

The catalog publishes the code with the source code as its first segment:

```
SELECT metric_code FROM gold_glossary.dim_metric_catalog
WHERE source_code = 'CENSUS_ACS' LIMIT 1;
-> CENSUS_ACS:acs1:B01001_001
```

The serving layer publishes the same metric with `ACS` as its first segment:

```
SELECT metric_code FROM gold_census.mv_acs_latest
WHERE geo_level = 'NATIONAL' LIMIT 1;
-> ACS:acs1:B01001_001
```

So the catalog's own code resolves to nothing, and the serving layer's code —
which no published surface advertises — is the one that works:

```
GET /api/v1/census/observations/latest?metric_code=CENSUS_ACS:acs1:B01001_001&geo_level=NATIONAL
-> {"total": 0, "items": []}

GET /api/v1/census/observations/latest?metric_code=ACS:acs1:B01001_001&geo_level=NATIONAL
-> {"total": 1, ... "value": "340110990.0"}
```

### The scale, and the two sources that are healthy

Counting catalog rows whose `metric_code` exists in the source's `mv_*_latest`:

| Source | Catalog rows | Resolvable | Note |
| --- | --- | --- | --- |
| `CENSUS_ACS` | 4,447 | **0** | every advertised code is dead |
| `BLS` | 13,324 | 63 | correct — 63 current, 13,261 deliberately retired |
| `FRED` | 24 | 24 | correct |

BLS and FRED are the control: their publisher emits the same `source_code` that
their refresh procedure builds the code from, so the two sides agree by
construction. Only ACS disagrees, which is why this reads as a defect in ACS
rather than a design question about the catalog.

`CENSUS_PEP` is the most important control, because it is the other
`CENSUS_*` source and the one that could have shared the pattern. It does not:
`gold_pep.mv_pep_latest` serves `CENSUS_PEP:`-prefixed codes, matching its
catalog rows. So ACS is a single instance, not a family convention, and this
plan closes the whole pattern rather than one of two cases.

Re-confirmed on 2026-09-12 before implementation, on the same warehouse:
4,447 `current` ACS catalog rows, 0 resolvable; `gold_census.mv_acs_latest`
held 4,646,720 rows, all `ACS:`-prefixed; `gold_census.rpt_acs_observations`
held 68,302,467 rows.

### The two lines that disagree

- [`publisher.sql:3`](../../../src/data_ingestion_toolbox/census_acs/gold_census/DDL/publisher.sql#L3)
  emits `'CENSUS_ACS'::TEXT AS source_code`, and the harvest composes
  `metric_code` as `source_code || ':' || source_object_key`.
- [`gold_acs.sql:299`](../../../src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql#L299)
  built the served code as `'ACS:' || ao.dataset_code || ':' || v.variable_code`
  (now `'CENSUS_ACS:' || ...`).

Blamed to `298b73d` (2026-08-19, the capture-first cutover), so this has been
true of every ACS row the warehouse has ever served. It was invisible because
no test and no page crosses the two surfaces: the web explorer reads metrics
and observations from the same endpoint family and never round-trips a catalog
code, and the API's own tests build their fixtures from one side or the other.

## Decisions

1. **This is a real defect, not a naming preference.** The catalog is
   documented as the discovery surface for metric codes. A code it publishes
   that returns nothing is a broken contract regardless of which spelling wins.
2. **Do not fix it by teaching the API to translate.** A prefix-rewriting shim
   in `apps/api` would make the symptom disappear while leaving two identities
   for one metric in the warehouse, which is exactly the condition
   `completed/GLOSSARY_HARVEST_IDENTITY_CHANGE_PLAN.md` exists to prevent.
   One of the two producers changes.
3. **The choice of canonical spelling is the implementer's to make and record.**
   ACM-001 requires it in writing before any code moves, because the two
   options have very different costs — see the phase.

### ACM-001 decision record (2026-09-12)

**Serving follows the catalog. `CENSUS_ACS:<dataset>:<variable>` is the
canonical spelling, and `gold_census.refresh_rpt_acs_observations` is the
producer that changes.** Reasons, in order of weight:

1. **The glossary composes every catalog code generically.**
   `harvest.py` writes `f"{source_code}:{item['source_object_key']}"` for
   every publisher. Producing `ACS:` would require either renaming the source
   (`CENSUS_ACS` → `ACS` across the glossary, the dispatch registry, the DAGs,
   capture control, and quality evidence) or adding a per-source prefix
   override to a composition that is deliberately source-agnostic. Both
   introduce a second identity concept into the glossary — the condition the
   identity-change plan exists to prevent, and what decision 2 above forbids.
2. **The published identity is already `CENSUS_ACS:` everywhere a consumer is
   told to look:** the catalog, `gold_glossary.dim_metric`, saved-analysis
   validation, and the neutral `/observations` resource. `ACS:` existed only
   inside the two serving relations and in the registry bridge
   (`lineage_key_prefix="ACS:"`) that existed solely to span the gap. Choosing
   `CENSUS_ACS:` deletes the bridge; choosing `ACS:` would make it permanent,
   which is the translation layer decision 2 rules out.
3. **`CENSUS_PEP` already serves `CENSUS_PEP:`.** Choosing `ACS:` would put the
   two Census sources permanently at odds.
4. **The expected breakage of stored consumer codes does not exist, and the
   alternative would have created it.** `app_api.saved_analysis_configuration`
   documents are validated against the glossary on write and on read
   (`saved_analysis_service._require_metric` → `resolve_metric` over
   `gold_glossary.dim_metric`), and the glossary has only ever held
   `CENSUS_ACS:` codes, so a configuration holding an `ACS:` code was refused
   at write time and cannot be stored valid. Under this decision every stored
   valid ACS code keeps working; under the alternative every stored valid ACS
   code would break. On the development stack the table has never been
   provisioned in either the analytics or the service database, so there is
   nothing to migrate at all.
   Holders that did carry `ACS:` codes: (a) repository constants and
   fixtures — `apps/web/lib/productTemplates.ts` (whose ACS candidates could
   never have matched a catalog row), `apps/web/app/page.js`,
   `apps/web/app/articles/page.js`, `tests/sql/martin_seed.sql`,
   `tests/sql/frontend_smoke_seed.sql`, and the frontend unit/browser
   fixtures — all migrated in ACM-002; (b) per-browser `localStorage` saved
   views (`apps/web/lib/savedCharts.js`), which are unversioned against the
   catalog and already degrade to "metric unavailable" when a code stops
   resolving.
5. **The cost is accepted deliberately:** a `metric_code` rewrite of
   `gold_census.rpt_acs_observations` (68.3 million rows) through the forced
   full re-serve, measured at 4h36m tuned in
   `BETA_RESET_REINGESTION.md` section 7 and resumable per year. Cheapness is
   not a tiebreaker when the cheap option's price is a permanent exception in
   the identity model.

Consequence for the registry: the ACS dispatch entry moves from the
`lineage_key_column` + `lineage_key_prefix` strategy to `metric_code_column`,
exactly as BLS and FRED declare, because the serving relation now carries the
glossary's composed code.

## Non-goals

- No change to BLS, FRED, or PEP metric identity. They agree already.
- No new API surface, and no alias parameter.
- No change to the catalog's retirement semantics. Codes retired under the old
  spelling stay retired; this plan does not delete catalog history.

## Implementation phases

### ACM-001 — Decide which spelling is canonical

Deliverables:

- A recorded decision, in this plan, choosing one of:
  - **Serving follows the catalog** (`CENSUS_ACS:`). Consistent with every
    other source, and the catalog needs no change. Costs a `metric_code`
    rewrite of `gold_census.rpt_acs_observations` — 68.3 million rows, a
    forced full re-serve, measured at roughly 4h36m on a tuned box (see
    `BETA_RESET_REINGESTION.md` section 7). Every stored ACS code changes, so
    any consumer holding a saved `ACS:`-prefixed code breaks.
  - **Catalog follows serving** (`ACS:`). Cheap — a publisher change and one
    harvest. But it makes ACS the only source whose catalog code does not begin
    with its `source_code`, which the harvest composes generically, so it needs
    either a per-source override or a documented exception, and it puts ACS at
    odds with `CENSUS_PEP`, which already serves `CENSUS_PEP:`.

Acceptance:

- The decision and its reasoning are written into this plan before ACM-002
  opens, including which consumers hold stored codes and how they are migrated.
  **Met** — see the decision record above.

### ACM-002 — Make the two sides agree

Deliverables:

- The single producer chosen in ACM-001 changed, with no translation layer
  anywhere between them.
- If serving moves: the `metric_code` expression in `gold_acs.sql` and every
  place that reproduces it, including the affected-keys temp tables that key on
  `metric_code`.
- If the catalog moves: the publisher view, plus whatever the harvest needs so
  the exception is explicit rather than emergent.

Acceptance:

- Every `CENSUS_ACS` catalog row's `metric_code` exists in
  `gold_census.mv_acs_latest`, except rows legitimately `retired`.
  **Met** in the repository (the refresh composes the catalog's code, and the
  end-to-end integration guard proves it on a bootstrapped warehouse) and on
  the development warehouse (4447/4447 after ACM-004's re-serve).

### ACM-003 — A guard that fails when a catalog code cannot be served

Deliverables:

- A test that, for every source with a serving contract in
  `apps/api/registry.py`, takes a `current` catalog code and requires the API
  to answer it with at least one row. Source-agnostic, so a fourth source
  cannot reintroduce the defect.
- A `TESTING_CONTRACT.md` row for catalog/serving code agreement, mapped in
  `CI_EVIDENCE_MAP.md`.

Acceptance:

- Reverting ACM-002 makes the guard fail, naming `CENSUS_ACS` and the code that
  did not resolve. **Met** — see the evidence record.

### ACM-004 — Re-serve or re-harvest as the decision requires

Deliverables:

- If serving moved: a forced full re-serve of `CENSUS_ACS` per
  `BETA_RESET_REINGESTION.md` section 7, with `acs_ingest` paused for its
  duration, then a forced `glossary_reconciliation` so retirement of the old
  codes proceeds.
- If the catalog moved: a forced `glossary_reconciliation` scoped to
  `gold_census`, and confirmation that the old `CENSUS_ACS:`-prefixed codes
  retire rather than lingering as `current`.
- Either way: `docs/reference/BETA_RESET_REINGESTION.md` gains this as a worked
  example of an identity change, since it is precisely the class of change
  section 7 describes.

Acceptance:

- ACM-003's guard passes against the development warehouse, and the catalog
  reports no `current` ACS code that the API cannot answer.

Note on retirement under the recorded decision: the catalog's published codes
do not change, so nothing retires. The forced reconciliation is still run, as
confirmation that the publisher emits the same keys and that the harvest state
records the run; the analogue that matters is that no serving row survives
under the abandoned `ACS:` spelling, which DB-026 checks.

## Test plan

| Layer | Tier | What it proves | Catalog ID |
| --- | --- | --- | --- |
| Code composition | `unit` | Every serving relation's composed `metric_code` prefix equals the `source_code` its schema's publisher publishes, and the dispatch registry declares no rewriting prefix | ARC-005 |
| Catalog/serving agreement | `integration` | An ACS metric published through the real refresh and the real harvest is stored under the catalog's own code; every registered source's `current` codes answer | DB-025 |
| API round-trip | `api` | A code read from `/catalog/metrics` answers from `/observations` | API-067 |
| Abandoned spelling | `integration` | No serving row survives under `ACS:` after a re-serve | DB-026 |

The original row "codes under the abandoned spelling reach `retired`" applied
to the catalog-moves branch of ACM-001; under the recorded decision the catalog
never held the abandoned spelling, so DB-026 tests the serving-side analogue.

## Risks and mitigations

- **A 68-million-row rewrite is the expensive option.** ACM-001 exists so that
  cost is accepted deliberately rather than discovered in ACM-002. The forced
  re-serve path is now measured and resumable, which is what makes the
  expensive option viable at all.
- **Stored consumer codes break either way.** Saved analysis configurations
  (`gold.analysis_configuration`) may hold ACS metric codes. ACM-001 must
  enumerate them and say how they migrate; a silent break there is worse than
  the defect. *Resolved in the decision record: the store validates against
  the glossary, so no valid stored configuration can hold the abandoned
  spelling.*
- **A sibling source could share the defect.** Checked and ruled out on
  2026-09-12: `CENSUS_PEP` serves `CENSUS_PEP:` and agrees with its catalog.
  ACM-003's guard is what keeps a future source from reintroducing it.
- **The guard could be written to pass vacuously** if it skips sources with no
  catalog rows. It must fail, not skip, when a source in the registry has a
  serving contract and no resolvable catalog code. *Handled: the sweep fails
  when any source with `current` catalog codes has one that does not answer,
  and separately fails when no source was exercised at all; the seeded ACS
  fixture guarantees at least one source is exercised on any warehouse,
  including an otherwise empty CI one.*
- **A live deployment is split between two spellings until the re-serve
  finishes.** The API container mounts the working tree and imports the
  dispatch registry at startup, so a running process keeps whichever registry
  it started with. The re-serve orchestration restarts the API only after the
  warehouse is fully re-served, and the scheduled ingest is paused so no
  changed-year refresh interleaves.

## Open questions for the reviewer

1. ACM-001's core question — consistency with the other sources
   (`CENSUS_ACS:`) versus the cheap publisher-side fix — is answered in the
   decision record above. The reviewer's call is whether the reasoning holds;
   the plan no longer takes no default.

## Implementation evidence

### Changes

- `src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql` — the
  refresh composes `'CENSUS_ACS:' || dataset || ':' || variable`, with the
  reason recorded beside it. The affected-keys temp tables read `metric_code`
  from the reporting relation, so they follow without change.
- `apps/api/registry.py` — the ACS dispatch entry uses
  `metric_code_column="metric_code"`; the `lineage_key_prefix="ACS:"` rewrite
  is gone and the docstring states that a non-empty prefix may only be the
  glossary's own `<source_code>:` composition.
- `tests/unit/shared/test_catalog_serving_identity.py` (ARC-005) — static,
  source-agnostic: maps every `metric_publisher` schema to its published
  `source_code`, attributes every composed `'<PREFIX>:' ||` literal in
  warehouse SQL to the schema that writes it, and requires agreement; also
  requires that no composed prefix is unpublished and that no dispatch entry
  declares a rewriting prefix.
- `tests/integration/api/test_catalog_serving_agreement.py` (DB-025, DB-026,
  API-067) — publishes one ACS metric end to end (silver rows → real refresh
  procedures → real publisher view → real glossary harvest) and requires the
  catalog's own code to answer from `/api/v1/observations`; sweeps every
  source in `OBSERVATION_DISPATCH` for `current` catalog codes that do not
  answer; requires no `ACS:` residue in either serving relation.
- Fixtures and consumers migrated to the one spelling:
  `tests/sql/martin_seed.sql`, `tests/sql/frontend_smoke_seed.sql`,
  `tests/integration/api/test_real_database_contract.py`,
  `tests/integration/database/test_acs_gold_refresh.py`,
  `tests/e2e/test_census_bls_pipeline.py`, `tests/e2e/test_martin_api_join.py`,
  `tests/unit/api/test_source_observations.py`, the ACS binding assertions in
  `tests/unit/api/test_comparison.py` and
  `tests/unit/api/test_neutral_observations.py`, `apps/web/lib/productTemplates.ts`,
  `apps/web/app/page.js`, `apps/web/app/articles/page.js`, the explanatory
  comments in `apps/web/lib/explorerSources.ts`, and 21 frontend unit/browser
  fixture files.
- `docs/reference/TESTING_CONTRACT.md` — rows ARC-005, DB-025, DB-026,
  API-067 and the implementation-status ranges; `tests/support/catalog_evidence.py`
  audited counts and the evidence-register total (275 → 279);
  `docs/reference/CI_EVIDENCE_MAP.md` — a row assigning the contract to the
  existing `etl-unit`, `api-unit`, `postgres-integration`, and `frontend` jobs.
- `docs/reference/BETA_RESET_REINGESTION.md` — section 7 gains the worked
  example of this identity change.

### ACM-003 acceptance: the guard fails on a reverted ACM-002

With the SQL prefix reverted to `'ACS:'` and the warehouse rebuilt (2026-09-12,
disposable PostGIS):

```
FAILED test_a_catalog_code_is_answerable_by_the_observation_resource
  assert 0 >= 1  where 0 = _answers(<TestClient>, 'CENSUS_ACS:acs5:B99997_160B0954E')
FAILED test_the_serving_relation_stores_the_code_the_catalog_publishes
  the catalog publishes CENSUS_ACS:acs5:B99997_00480936E but gold_census.mv_acs_latest holds no row under that code
FAILED test_no_acs_serving_row_survives_under_the_abandoned_spelling
  gold_census.mv_acs_latest still holds 1 row(s) under the abandoned 'ACS:' spelling
FAILED test_every_registered_source_answers_each_current_catalog_code
  CENSUS_ACS publishes current catalog code 'CENSUS_ACS:acs5:B99997_2641CE23E', which /api/v1/observations answers with no rows
```

and the static guard names the line:

```
src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql:293 composes 'ACS:', which no publisher publishes as a source_code
CENSUS_ACS declares lineage_key_prefix 'ACS:', which is neither empty nor the glossary composition 'CENSUS_ACS:'
```

### Validation commands and results (2026-09-12, this host)

| Command | Result |
| --- | --- |
| `pytest tests/unit --basetemp=…` (unit + api tiers) | 1315 passed |
| `pytest -m "integration and not e2e" tests/integration` on a fresh scratch database (`acs_catalog_test`, PostGIS 16 test service) | first run 134 passed, 2 skipped, 1 failed — this plan's own fixture left `CENSUS_ACS` in `gold_glossary.publisher_registry` and a later DB-024 emptiness check named it; the fixture now removes exactly the registration rows the harvest created. Rerun on a rebuilt database: **135 passed, 2 skipped** |
| `pytest -m dag tests/dags` inside `docker-airflow-scheduler-1` | 117 passed, 5 skipped |
| `pytest -m "e2e and not martin" tests/e2e` on the scratch database | 8 passed, 1 failed: `test_bls_fixture_flows_raw_to_gold_and_replays_identically` (`assert 0 == 1` on the BLS timeseries total). Pre-existing and unrelated: the same test fails identically from an untouched `origin/main` (`6c96b77`) worktree on its own fresh database, and this plan touches no BLS code |
| `npm run test:unit` / `npm run lint` / `npm run typecheck` / `npm run test:browser` (`apps/web`) | 198 passed / clean / clean / 41 passed |
| `ruff check` + `ruff format --check` on changed Python | clean |

### ACM-004 on the development stack (2026-09-12)

Sequence as run, unattended, from an orchestration script: pause `acs_ingest`
(03:20 UTC) → wait for the in-flight ingest run (finished 04:36; its
`ensure_gold_census_schema` task had already installed the changed procedure
from the mounted working tree, confirmed by inspecting `pg_proc`) → trigger
`serving_full_reserve` with `{"source_code": "CENSUS_ACS"}` (run
`acm004_reserve_20260912T043628Z`, 04:36) → forced `glossary_reconciliation`
with `{"force": true, "schemas": ["gold_census"]}` (run
`acm004_reconcile_20260912T162354Z`, success 16:25) → `docker restart
docker-api-1` → verify → unpause `acs_ingest` (16:26).

Verification, against the warehouse and the restarted live API:

| Check | Result |
| --- | --- |
| `current` ACS catalog codes with a row in `gold_census.mv_acs_latest` | **4447 / 4447** |
| Rows under the abandoned `ACS:` spelling, `rpt_acs_observations` / `mv_acs_latest` | **0 / 0** |
| Row counts after the re-serve, `rpt_acs_observations` / `mv_acs_latest` | 68,302,467 / 4,646,720 (unchanged from before, as expected for an identity-only rewrite) |
| ACS catalog rows by `freshness_state` | `current` 4447; nothing stale or retired, as the decision predicted |
| `publisher_harvest_state` for `CENSUS_ACS` | `success`, `last_harvest_forced = true`, completed 16:23:58 |
| `GET /api/v1/observations?metric_code=CENSUS_ACS:acs1:B01001_001&limit=1` | `total: 932` |
| `GET /api/v1/census/observations/latest?metric_code=CENSUS_ACS:acs1:B01001_001&geo_level=NATIONAL` | `total: 1` (the request that answered `total: 0` in the evidence section) |
| Registry sweep through the live API: first `current` catalog code of every source in `OBSERVATION_DISPATCH` | BLS 1, CDC 2310, CENSUS_ACS 932, FBI_UCR 3216, FRED 1, USDA_NASS 183 rows; CENSUS_PEP answered a 503 on one sample and is recorded in the notes below |

The re-serve took 42,444 s wall clock (11h47m) instead of the measured
4h36m, and the reason is worth keeping. A catalog count query from another
session had been running for nine hours, and its snapshot stopped vacuum from
reclaiming the rows each year's delete left behind: by year 2015
`gold_census.mv_acs_latest` carried 54.7 million dead rows against 8.9 million
live, per-year time had climbed from 4 minutes to 60, and 2016's latest-view
rebuild hit the chunk driver's two-hour statement timeout after 7,202 s.
Cancelling that session and running a manual `VACUUM (ANALYZE, PARALLEL 0)`
on both serving relations (the container's 64 MB `/dev/shm` cannot host a
parallel index vacuum at the configured 8 GB maintenance memory) cleared the
bloat in 48 minutes; Airflow's own retry resumed at 2016 and finished it in
1,718 s, and 2017 to 2024 ran at 1,404 to 1,931 s each. Per-year timings are
in `docs/reference/BETA_RESET_REINGESTION.md` section 7.

Notes:

- `acs_ingest` was unpaused through the Airflow UI by `admin` at 04:25 UTC,
  before the re-serve started, so the script's pause did not hold. It was
  harmless here because the DAG's schedule is monthly and the next run could
  not start before 2026-10-01, but an operator following section 7 should
  confirm the pause held rather than assume it.
- The CENSUS_PEP sample in the live sweep returned `503 Database service is
  temporarily unavailable` on three consecutive tries. The API container log
  shows the cause: the neutral read for `CENSUS_PEP:BIRTHS` is cancelled by
  the API's own statement timeout (`psycopg2.errors.QueryCanceled`), so the
  PEP latest relation on this warehouse answers too slowly for the configured
  budget. Nothing in this plan touches PEP identity, dispatch, or relations,
  and the catalog resolves the code normally, so this is reported to the
  reviewer as a separate PEP serving-performance defect rather than
  investigated here. The repository sweep (DB-025) passes for every source on
  a bootstrapped warehouse.
