---
id: warehouse-data-quality
branch: feat/warehouse-data-quality
depends_on:
  - geography-reference
  - cicd-actions
parallel_safe: false
complexity: high
verify:
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 integration
---

# Warehouse data-quality assessment plan

## Plan status

- **Status:** Accepted by human review on 2026-08-31; complete
- **Next pickup:** None. Every DQ ticket is delivered, both recorded residuals are closed, and the plan was accepted into `completed/` on 2026-08-31.
- **Resolved (ledger semantics for superseded runs, decided 2026-08-31):** The user decided that **aborted runs finalize their control rows before assessments**, rather than teaching each ledger rule to assess only the latest run per partition. The second option would have given every rule its own notion of supersession and left the control plane permanently inconsistent with itself. Implemented and verified — see the "Aborted-run finalization" evidence entry below.
- **Resolved (DQ-004 fact-level injections, 2026-08-31):** PEP and FBI fact-level defects are now fixture-driven in `tests/integration/database/test_fact_quality_injections.py`. The separate pre-existing observation stands and is unrelated to this plan: the database integration tier is not repeatable across two consecutive sessions against one persistent local database, because several suites leak committed control rows; CI's fresh disposable container never hits it, and every run recorded here started from a freshly created warehouse.
- **Completed evidence:**
  - DQ-004 fact-level injections (2026-08-31): `tests/integration/database/test_fact_quality_injections.py` builds real PEP and FBI state from the reviewed fixtures through the production pipeline, proves each rule passes on it, then injects one defect and proves the rule fails naming the exact offending row. PEP: a fact beneath an `incomplete` release load and a fact with no load at all (DQ-PEP-002); a fact claiming an unregistered release vintage, and a registered published release with no complete load, in both directions (DQ-PEP-003). FBI: a publishable observation whose participation row was removed (DQ-FBI-002); an absent month carrying a number and its mirror, a reported month that lost its value (DQ-FBI-003); a duplicated effective-dated agency relationship fanning the area filter out (DQ-FBI-004). Where a DDL CHECK or foreign key already enforces the invariant, the injection drops it or disables referential triggers inside the always-rolled-back test transaction — deliberately, because a rule that only ever runs behind an intact constraint is untested: it exists for the case where the constraint is relaxed, where a later migration writes through a path it does not cover, or where data arrives from a restore.
  - DQ-FBI-002 made falsifiable (2026-08-31): writing the injection above showed the rule could never fail. It compared `gold_fbi.crime_observation` against `gold_fbi.reporting_coverage`, but the gold view *inner joins* participation — an observation that loses its coverage row does not appear uncovered, it disappears from the view entirely, so the rule reported `pass` while published rows silently vanished. It now measures at silver, where the defect can exist, and reconciles the publishable silver population against what gold actually serves, so silent loss at the serving boundary fails with both the offending record id and the count gap. The rule's declared objects already covered both layers, so no inventory change was needed.
  - DQ-006 certified baselines (2026-08-31): plausibility baselines are restricted to history a promotable release certification covered, closing the recorded residual. `plausibility.load_certified_scope` reads the newest `release` run that is promotable on the same terms `certify_release` applies (finished, no BLOCK/QUARANTINE failure), plus the objects a blocking rule currently reports as failing. An observation ingested at or before that certification joins the baseline; one ingested after is scored *against* it rather than joining it. With nothing certified, or with the baseline's object currently failing a blocking rule, the rule reports `not_applicable` with the reason rather than inventing a baseline — a silent monthly sweep means "not certified", never "nothing anomalous", and `DATA_QUALITY_OPERATIONS.md` tells operators to run `certify_release` after a re-ingestion or beta reset. Each warning now carries `certified_commit=<sha>` so a reviewer can see which certification defined the baseline. `tests/integration/database/test_quality_assessment.py` proves the failure mode the restriction exists for: twelve uncertified values at an anomalous level would have become the majority of a naive baseline and silenced the alarm they caused, while the certified baseline still warns with `observed_count` equal to the certified history alone. It also proves the uncertified and blocking-failure refusals, and that a certification stays valid when a later sweep finds a blocking failure — it was true when it ran; the material is simply no longer fit to teach a baseline.
  - Aborted-run finalization (2026-08-31): implements the decided ledger semantics. `capture.finalize_run_requests` brings a stopped run's unfinished requests to a terminal status in the same transaction that stops the run, and `CaptureControl.finish_run` invokes it for the aborted statuses (`failed`, `cancelled`, `partial`) only — a `success` run holding unfinished requests stays a reportable defect. A request that already holds durable bytes finishes as `captured`, because a committed, checksummed payload is provider evidence that must never be discarded to make a check pass; a request that produced nothing finishes as `failed` with a fixed, secret-free reason. `quality/finalization.py` adds the sweep for runs stopped by something other than the capture control: `find_aborted_runs` and `finalize_aborted_runs` touch only runs already in a terminal aborted status (so live work is never cancelled), retire USDA NASS slices left `preflighted` to `skipped` while leaving `over_limit` and `partial` slices as the quarantine evidence they are, and return and log everything they changed so a repair is inspectable rather than silent. The ACS, BLS, and FRED ledgers are deliberately excluded: they carry no run linkage and are declarative registries of configured work, so a `planned` row there means the warehouse genuinely still owes that slice — which makes the recorded DQ-BLS-002 finding (49 quota-deferred chunks) correct behaviour rather than a false alarm, cleared by the BLS DAG's designed 23h retry. The `warehouse_data_quality` DAG gains a `finalize_aborted_runs` task upstream of `run_assessment`, kept a separate task so the repair is visible in the graph and its own log while the assessment stays a read-only measurement. `tests/integration/database/test_run_finalization.py` reproduces the live finding on real PostgreSQL — an aborted run leaving `running` requests, a capture whose request never reached `captured`, and a `preflighted` NASS slice — and proves DQ-SHARED-002, DQ-SHARED-003, and DQ-NASS-002 go green after finalization, that a second sweep is a no-op, that a `success` run and a run still in flight are never repaired, and that an `over_limit` slice survives untouched. `tests/unit/quality/test_run_finalization.py` pins the statements and the decision boundaries without a warehouse. One existing injection in `test_layer_reconciliation.py` was repaired: it created "lost work" by calling `finish_run(status='partial')`, which now finalizes, so it writes the terminal run row directly — the state a process killed mid-run actually leaves. Commands: `pytest tests/integration/database -m "integration and database"` on a fresh warehouse -> 81 passed (host, excluding the credentialed `legacy/` live-pull tier and the Airflow-importing module Windows cannot collect); the same selection in the pinned Airflow 2.9.3 image -> 84 passed; `pytest -m dag tests/dags` in the image -> 117 passed, 1 skipped; `pytest tests/unit` -> 1060 passed (33 files blocked by a host tmpdir ACL, all passing in the container); `pytest -m "unit and api" tests/unit/api` -> 129 passed; ruff clean. A run stuck in `running` because its process was killed outright is deliberately not swept — nothing distinguishes it from live work — and `DATA_QUALITY_OPERATIONS.md` documents stopping it explicitly with `finish_run(status='cancelled')`.
  - Containerized execution (2026-08-31): the `warehouse_data_quality` DAG ran for the first time inside the internal Compose stack during the full live pipeline validation. Two gaps closed: (1) the assessment's required commit SHA is now wired through the deployment contract — all three compose files pass `DATA_QUALITY_COMMIT_SHA` into the Airflow environment and `scripts/deploy_stack.ps1` defaults it from `git rev-parse HEAD` when the host does not set it (conf-driven `code_commit_sha` also verified); (2) the assessment then executed the full daily sweep against the freshly loaded warehouse (~104M facts across all seven sources) and failed honestly on real findings: DQ-BLS-002 (49 slices still `planned` — quota-deferred work the BLS DAG's designed 23h retry ingests on its next run) and DQ-NASS-002/DQ-SHARED-002/DQ-SHARED-003 (non-terminal control-plane rows — `over_limit`/`preflighted`/`failed` slices, orphaned captures, and failed run rows — left behind by aborted backfill iterations, while the warehouse content itself was verified complete at every aggregation level). The findings confirm the ledger rules do their job; they also expose the design residual below.
  - DQ-005/006/007 (2026-08-29): `quality/assessment.py` adds cadence-driven scheduled assessment (daily control sweep; weekly full configured-scope reconciliation; monthly + WARN-only plausibility) with source/rule targeting for repair verification, and `certify_release` — one release assessment tied to a 40-hex commit SHA with rule totals by severity/result and a promotability verdict (no BLOCK/QUARANTINE failure, run finished). `quality/plausibility.py` implements robust per-series baselines (median/MAD, minimum history, no global threshold), the FRED change executor, and `record_warning_review` (the sole evidence mutation the trigger permits). Migration 013 gained the operator views `control.data_quality_latest_result` and `control.data_quality_source_status`; `docs/reference/DATA_QUALITY_OPERATIONS.md` documents operator queries, the repair/reassessment workflow, the warning lifecycle, promotion criteria, and release certification. The new `warehouse_data_quality` DAG (daily 11:00 UTC, weekly/monthly escalation, conf-driven targeting) is registered in the DAG inventory contracts and executes in the orchestrated pipeline tier. Evidence: 1080 unit tests; 79 database-integration tests on fresh bootstrap; 111 DAG structural/callable tests plus the full orchestrated pipeline (all 11 production DAGs including the quality assessment) under real Airflow 2.9.3 locally — 3 passed in 127s; ruff clean. Catalog DQ-005–DQ-007 registered (evidence register 202 rows); CI_EVIDENCE_MAP maps quality contracts onto the existing authoritative jobs.
  - DQ-004 (2026-08-29): `quality/sources.py` implements 15 deterministic source executors keyed by rule id (`SOURCE_EXECUTORS`): ACS/BLS/FRED slice-ledger accounting, FRED configured-series metadata reconciliation, PEP release completeness + registry reconciliation + frozen-sentinel conformance, CDC watermark monotonicity + suppression-never-a-number, FBI participation coverage + reported-vs-absent + aggregation-grain, NASS slice-ledger (preflight drift, advanced partial/over-limit slices) + full suppression vocabulary via `SYMBOL_STATUS`, shared-reference resolution coherence, and publisher-registry liveness. All treat an empty warehouse as `not_applicable` (valid emptiness, never a false alarm). The runner now also rejects an outcome naming an object its rule does not declare, which caught and fixed a DQ-SHARED-003 declaration gap. Tests: `tests/integration/database/test_source_quality_checks.py` (6 tests: empty-warehouse validity plus injected ledger drift, backward CDC watermark, NASS preflight mismatch and advanced partial slice, PEP sentinel misclassification, incoherent geography resolution, dangling publisher registry row — each failing with exact bounded evidence). Commands: fresh-bootstrap DB tier 73 passed, 1 skipped; `pytest tests/unit` 1072 passed; lint/format clean. Catalog: DQ-004 row registered; evidence register 199 rows.
  - DQ-003 (2026-08-29): `quality/reconciliation.py` implements the shared lineage executors (bounded checksum recomputation over recent captures, captured-request/capture agreement in both directions, finished-run request accounting, quarantine accounting), the generic `compare_identity_sets` comparator (EXCEPT in both directions, so equal counts cannot conceal replacement), and `evaluate_publication_gate`, which persists an inline assessment and refuses publication unless the run passes or only warns. CDC is the reference layer-reconciliation wiring (control release row count vs silver facts+quarantine vs gold projection per watermark). Acceptance proven on real PostgreSQL (`tests/integration/database/test_layer_reconciliation.py`): injected payload corruption, orphan lineage, and lost work each fail with exact bounded evidence; injected loss (2/3) and duplication (4/3) on an unpublished CDC release refuse the gate while the prior published release keeps serving through `gold_cdc.latest_release_observation`; repairing the release reopens the gate and it publishes. The CDC fixture-release helper moved to `tests/support/cdc_release.py` for reuse. Commands: full DB tier 67 passed, 1 skipped (fresh bootstrap, legacy credentialed tier excluded); `pytest tests/unit` 1072 passed; lint/format clean. Catalog: DQ-003 row registered; evidence register 198 rows.
  - DQ-002 (2026-08-29): migration `sql/migrations/013_data_quality_evidence.sql` adds `control.data_quality_run` and `control.data_quality_result` through the warehouse manifest and the test compose bootstrap. Results are append-only, enforced by a trigger that permits only a warning's `review_status` to change; results are unique per (run, rule, object, partition). `quality/runner.py` provides the source-neutral executor interface: run rows finalize exactly once, a failing BLOCK/QUARANTINE rule fails the run, non-blocking severities only warn, executor errors finalize as `error` with a bounded secret-safe summary, and savepoints keep one broken rule from corrupting the rest of the run's evidence. `rule_set_version()` fingerprints the declared rule contract into every run row. Commands: `pytest tests/unit` -> 1072 passed; fresh-bootstrap `pytest tests/integration/database -m "integration and database"` (excluding the credentialed legacy live-pull tier, which requires BLS_API_KEY unavailable in this environment) -> 65 passed, 1 skipped, on local PostgreSQL 16 + PostGIS; `ruff check`/`format` clean. Catalog: DQ-002 row registered; evidence register now 197 rows.
  - DQ-001 (2026-08-29): `src/data_ingestion_toolbox/quality/inventory.py` catalogs all 136 manifest-created relations with layer, owner, grain, lineage, expected-scope method, cadence, and empty behavior, and declares 63 rules with stable ids and severities across all seven sources plus the shared reference and glossary. `validate_inventory` enforces the acceptance criterion (every published object has an owner, grain, scope method, and a deterministic rule). Tests: `tests/unit/quality/test_quality_inventory.py` (10 tests, including an exact inventory-vs-manifest cross-check so the catalog cannot drift). Catalog id DQ-001 registered in `docs/reference/TESTING_CONTRACT.md` and the behavioral evidence register (196 rows). Commands: `python -m pytest tests/unit/quality tests/unit/shared tests/unit/tooling` -> 222 passed; `ruff check`/`format --check` clean.
- **Last updated:** 2026-08-31
- **Primary owner:** Shared warehouse reliability
- **Depends on:** Capture/control foundation, shared geography, and source-specific silver and gold contracts (all satisfied: `geography-reference` and `cicd-actions` are in `completed/`, and all seven source pipelines were accepted 2026-08-28)
- **Source scope:** Every implemented source — Census ACS, BLS, FRED, Census PEP, CDC, FBI UCR Crime, and USDA NASS Crop — plus the shared geography reference and glossary. The plan was first drafted against ACS/BLS/FRED only; it was rescoped on 2026-08-29 at user direction to cover all seven, and any source accepted later joins this scope automatically.
- **Co-delivery requirement:** Every implementation ticket must update the relevant GitHub Actions, scheduled checks, and release evidence defined by the CI/CD migration plan; resolve its current workflow location through the [plan index](../README.md).

## Objective

Establish repeatable evidence that warehouse data is complete for its declared scope, structurally valid, traceable to captured provider responses, correctly conformed, temporally coherent, and safe to publish under its documented source semantics.

The system must answer four separate questions:

1. Did the expected provider material arrive?
2. Was every accepted payload parsed and conformed without silent loss or duplication?
3. Does the published warehouse preserve provider grain, identity, revision, geography, unit, and missing-value meaning?
4. Is the resulting coverage fresh and statistically plausible relative to explicit source-aware expectations?

Statistical surprise is not automatically corruption. Structural failures may block publication; plausible-but-unusual values should normally produce reviewable warnings.

## Current-state assessment

The repository already provides:

- append-only response captures with checksums and request/run lineage;
- request, slice, retry, and quarantine state in `control`;
- deterministic raw-capture replay into source-shaped silver revisions;
- source-specific uniqueness and semantic checks in `utility/gold_quality.py`;
- geography resolution and unresolved-observation handling;
- fresh-bootstrap, integration, API, and bounded-volume tests; and
- publisher/glossary lineage checks.

The current checks do not yet provide:

- persisted, queryable results for every rule execution;
- configured-scope versus loaded-scope reconciliation;
- capture-to-silver-to-gold row and identity reconciliation;
- complete historical partition coverage;
- frequency-aware missing-period and freshness checks;
- revision and late-arrival monitoring;
- source-aware value-domain and relationship rules;
- warning baselines and anomaly review state; or
- release-level certification that distinguishes complete, partial, quarantined, and stale data.

## Quality dimensions

| Dimension | Question | Preferred evidence |
| --- | --- | --- |
| Capture integrity | Can the exact successful response be retrieved and checksum-verified? | Capture checksum, payload size, request fingerprint, and HTTP metadata |
| Completeness | Did every configured dataset, series, period, and geography slice run? | Configuration/source catalog reconciled to ledgers and observations |
| Conformance | Did parsing preserve values, nulls, sentinels, footnotes, units, and identity? | Offline replay and capture-to-silver reconciliation |
| Uniqueness | Is each table unique at its declared grain? | Database constraints plus partitioned duplicate checks |
| Referential integrity | Do observations resolve to valid metadata, metric lineage, and geography? | Exact-key joins and unresolved/quarantine counts |
| Temporal integrity | Are periods valid, ordered, frequency-compatible, and within source availability? | Frequency-aware period checks and watermarks |
| Freshness | Is each product current relative to its publication cadence? | Source-specific expected-availability windows |
| Revision integrity | Are revisions retained and current projections selected deterministically? | Revision history and latest-selection reconciliation |
| Reconciliation | Do row counts and stable identities agree across capture, silver, and gold? | Release/partition reconciliation summaries |
| Plausibility | Are values and changes unusual enough to review? | Non-blocking, source-aware warning rules |

## Rule outcomes and publication behavior

Every rule has one explicit severity and scope.

| Severity | Meaning | Required behavior |
| --- | --- | --- |
| `BLOCK` | Deterministic contract violation that makes publication unsafe | Fail before the affected gold partition is published |
| `QUARANTINE` | A capture or record cannot be safely interpreted | Preserve the capture, isolate the material, and prevent silent publication |
| `WARN` | Data may be valid but needs review | Publish only when blocking rules pass; persist warning evidence |
| `INFO` | Coverage, freshness, or revision observation | Persist for trending without changing publication state |

Blocking examples include duplicate declared keys, missing capture lineage, checksum mismatch, unresolved required geography, incomplete required paging, and loss between accepted silver identities and gold identities.

Conditions that must not automatically block include a recession-scale change, a provider-published null, an ACS1 county omitted because it is outside publication eligibility, a BLS footnote, or a FRED series whose cadence does not include every calendar month.

## Quality result contract

Quality definitions remain version-controlled in Python or SQL. Mutable business interpretations and dashboard policy do not move into the warehouse.

Persist operational evidence in `control` using two relations.

### `control.data_quality_run`

One row per assessment execution:

- quality run ID;
- source code and optional ingestion/publication run ID;
- assessment type (`inline`, `scheduled`, `release`, or `manual`);
- code commit SHA and rule-set version;
- started/finished timestamps and overall status;
- evaluated watermark or partition range; and
- bounded, sanitized failure summary.

### `control.data_quality_result`

One row per rule and evaluated object/partition:

- quality run ID and stable rule ID;
- layer, schema/object, source, and partition identity;
- severity and result (`pass`, `fail`, `warn`, or `not_applicable`);
- observed and expected counts or bounded numeric measures;
- source watermark and latest capture ID where applicable;
- bounded evidence/sample identifiers, never credentials or full payloads;
- evaluation timestamp and duration; and
- optional review status for warnings.

Results are append-only evidence. Re-running a rule creates new evidence rather than rewriting history.

## Core warehouse checks

### Raw capture and control

- Verify every `response_capture.payload_checksum` matches its immutable payload.
- Require one valid request/run lineage chain for every capture.
- Reconcile successful/captured requests to captures and empty requests to explicit empty outcomes.
- Reject successful ingestion runs with no configured work unless the planner records an approved no-op reason.
- Track failed, quarantined, abandoned-running, and retry-exhausted requests by source.
- Detect duplicate successful request fingerprints within one immutable source watermark when the source contract does not permit them.

### Silver revisions

- Require every revision row to reference an existing capture and valid source row/column identity.
- Reconcile parsed source rows/cells to captured payloads through source-specific parsers.
- Enforce declared revision-table grain and valid value-status domains.
- Prove that blank, absent, sentinel, invalid, zero, and valid numeric values remain distinguishable.
- Verify current-revision selection is deterministic under out-of-order replay.
- Report captures that produced no silver rows without an explicit empty or quarantined outcome.

### Shared reference data

- Enforce canonical identity uniqueness and exact FIPS/code formatting.
- Check active nation/state/county/place coverage against the selected source vintage.
- Validate required geography resolution coverage for every observation source and vintage.
- Validate geometry SRID, type, validity, non-emptiness, and entity linkage.
- Track additions, retirements, name changes, boundary changes, and cross-county-place relationships between vintages.

### Gold and publisher contracts

- Enforce one published row per declared source-specific grain.
- Reconcile stable observation identities and accepted values from silver to gold.
- Require source, metric, geography, unit, period, capture, and revision lineage where the product contract requires them.
- Validate publisher manifests and glossary lineage without making source publication depend on glossary availability.
- Detect partial partition replacement and mixed-watermark publication.
- Verify bounded serving/API projections reconcile to their gold contracts.

## Source-specific checks

### Census ACS

- Reconcile configured `acs1`/`acs5` dataset-years to `raw_census.acs_datasets` and slice ledgers.
- Require the configured variable fingerprint and expected estimate/MOE variables for every available table/year.
- Reconcile U.S., state, and county-parent slices, including Puerto Rico, while treating source-confirmed ACS1 geographic absence as valid emptiness.
- Validate Census sentinel/null interpretation and retain exact source text.
- Require geography resolution for every publishable observation.
- Check estimate/MOE pairing where the source publishes both, without fabricating a missing counterpart.
- Monitor year-over-year changes as warnings, stratified by variable, dataset, and geography level.
- Never compare overlapping ACS vintages as though they were independent annual point samples.

### BLS

- Require every configured program and curated series/measure to resolve through synchronized metadata.
- Reconcile request-sized series/year chunks so a partial backfill cannot appear complete.
- Validate series-ID grammar, program ownership, period codes, annual-average handling, footnotes, and source missing values.
- Check expected frequency and published observation range per series rather than requiring every calendar month universally.
- Reconcile LAUS state/county-equivalent coverage, including Puerto Rico, to published series metadata.
- Track revision and benchmark changes without overwriting prior captured revisions.
- Treat provider `No Data Available` as explicit emptiness only when tied to the exact requested series/range.

### FRED

- Require metadata for every configured series and exactly one configured domain owner.
- Validate observation dates against each series frequency and source observation range.
- Preserve the FRED missing marker separately from numeric zero.
- Reconcile configured series, requested date ranges, captures, silver revisions, and published current observations.
- Track revised observations and metadata changes over time.
- Apply freshness windows by frequency; daily, weekly, monthly, quarterly, and annual series cannot share one threshold.
- Detect unusually large changes as warnings, not automatic invalidation.

### Census PEP

- Reconcile the registered datasets and vintages in `silver_pep.pep_dataset`/`silver_pep.pep_release` to captures, `silver_pep.release_load` completeness, and published observations.
- Require every published observation to come from a `release_load` row with `completeness_status = 'complete'`; an incomplete load must carry its recorded reason and never feed gold.
- Validate the frozen Census null-sentinel set and `value_status` semantics (`valid`/`blank`/`sentinel`/`invalid`); only `valid` rows carry a number.
- Keep `release_vintage` distinct from `observation_year`: verify prior vintages are retained, current-revision selection within a vintage is by capture recency, and latest-vintage selection is a separate projection.
- Check component-of-change sign rules (estimates non-negative; components such as net migration may be negative) and the July-1 estimate-date convention.
- Reconcile geography-level and summary-level coverage per dataset against the registry's declared levels, treating registry-excluded levels as valid emptiness.
- Monitor vintage-over-vintage revisions to the same `(metric, geography, year)` as warnings, never as automatic invalidation.

### CDC (CDI and PLACES)

- Reconcile every `control.cdc_dataset_release` decision (`unchanged`, `ingest`, or a quarantine) to captures, silver revisions, quarantine rows, and release status; a release must not publish while `complete` is false.
- Require watermark monotonicity per asset: a backward `release_watermark`, schema change, or dataset replacement quarantines rather than overwrites, and every retained `(asset_id, release_watermark)` stays queryable.
- Preserve suppression semantics exactly: CDI suppression is the absence of `datavalue` with footnotes retained; PLACES `suppressed` requires a null value plus a suppression footnote, otherwise `missing`. Suppressed and missing are never zero.
- Verify fact uniqueness at `(asset_id, release_watermark, source_record_id)` and that every `source_record_id` traces to its exact provider row payload.
- Check confidence-interval ordering and PLACES percent-domain quarantine behavior against the declared parser contract versions.
- Reconcile geography resolution (`resolved`/`unmapped`/`unsupported`) per asset's declared levels; PLACES county basis is 2020 Census counties, CDI is US/state.
- Evaluate freshness from each asset's declared cadence (CDI irregular, PLACES annual; metadata checked weekly), not one shared threshold.

### FBI UCR

- Reconcile every `control.fbi_ucr_release` to its directory and observation slice counts, silver revisions, quarantines, and participation coverage before publication.
- Enforce the agency aggregation boundary: agency-grain observations must never be summed into county or city totals, and county/place attribution flows only through `exact_state_code` or reviewed crosswalks with recorded confidence.
- Keep `reported` and `not_reported` distinct: a published zero is a value, an absent month is NULL, and no check may conflate them.
- Require every published observation to join a `fact_reporting_participation` row — no crime observation without a coverage interpretation — and verify `coverage_basis` is recorded rather than imputed.
- Verify measure identity separation (`counted_entity_basis`: offense, clearance, arrest, incident, victim never share a measure) and that only `absolute_total` measures carry the additive-within-subject characteristic.
- Validate release identity from provider `refresh_date`/`max_data_month`; `/LATEST` is capture input, not release identity, and a backward refresh quarantines.
- Reconcile the registered product/offense/state/agency scope in `fbi_ucr/registry.py` to expected periods, treating unsupported state codes (`FS`, `GM`) as declared exclusions rather than gaps.
- Exclude `ambiguous` and `unsupported` geography from gold while keeping the withheld evidence queryable.

### USDA NASS

- Reconcile every `control.usda_nass_release` and its per-slice ledger (`control.usda_nass_slice`) so preflight counts, captured row counts, and slice statuses (`captured`/`empty`/`over_limit`/`partial`/`skipped`) agree before publication.
- Require the over-limit and partial-slice quarantine paths to hold: a slice that exceeded the provider record limit or captured fewer rows than preflighted must never advance the published watermark.
- Preserve the full Quick Stats suppression vocabulary: `(D)` withheld, `(S)` insufficient reports, `(X)` not applicable, `(NA)` not available, `(Z)` below rounding unit, `(H)`/`(L)` quality flagged — each mapped to its own `value_status`, applied independently to CV values, with exact provider text retained.
- Verify fact uniqueness at `(product_id, release_watermark, source_record_id)` — the complete Quick Stats grain, never commodity/year alone.
- Reconcile the registry allowlist (five registered products, declared year window, national/state/county aggregation levels) to slices and observations, treating registry-excluded aggregation levels and geographies as valid emptiness.
- Distinguish survey products (revised until final) from the census product (periodic final) in revision and freshness expectations; recent-window pulls and full-reconciliation sweeps must reconcile to the same retained releases.
- Check `additive_behavior` propagation into series and publisher characteristics; `not_established` must remain visibly unknown, never defaulted to additive.

### Shared geography reference and glossary

- Verify every source's observation geography resolves through `silver_ref` or is explicitly recorded as unresolved/unsupported/quarantined by that source's contract.
- Validate reference identity and relationship integrity (state/county/place hierarchies, boundary vintages, overlap weights) after any geography reload.
- Reconcile `gold_glossary.publisher_registry` to each source's `metric_publisher` view: every published source has exactly one registry row and every registry row resolves to a live publisher.

## Completeness and reconciliation model

Expected scope is derived in this order:

1. reviewed repository configuration;
2. synchronized provider metadata or release manifest;
3. deterministic planner output; and
4. documented source-availability exceptions.

For each source partition, persist a reconciliation record containing:

```text
expected requests
captured requests
explicit empty requests
quarantined requests
expected source identities
parsed silver identities
published gold identities
unresolved geography identities
latest source and publication watermarks
```

Counts alone are insufficient. Where practical, compare stable identity hashes so an equal row count cannot conceal replacement or loss.

## Plausibility and anomaly policy

- Start with deterministic domains and relationships before statistical detection.
- Establish baselines only from quality-certified historical partitions.
- Segment baselines by source, metric/series, geography level, frequency, unit, and revision status.
- Use robust change measures and minimum-history requirements; do not apply one global percentage threshold.
- Persist observed value, baseline window, score, and relevant identifiers for every warning.
- Keep anomaly rules at `WARN` until reviewed evidence supports promotion to a blocking source contract.
- Never silently modify, winsorize, interpolate, or delete a provider value because it is anomalous.

## Execution model

### Inline release gates

Run bounded structural, lineage, uniqueness, geography, and partition-reconciliation checks before publishing each affected gold partition. A blocking failure leaves the prior published partition intact.

### Scheduled warehouse audit

Add an independent `warehouse_data_quality` DAG that:

- runs daily for freshness, failed/quarantined work, and newly published partitions;
- runs weekly for full configured-scope and historical-gap reconciliation;
- runs monthly for revision trends, geography-vintage changes, and anomaly baselines;
- can target one source, rule, or partition for repair verification; and
- does not mutate source observations.

### Deployment certification

After bootstrap or a major migration, execute a full bounded-history certification and store a release evidence artifact. Deployment is not certified merely because all DAGs are green.

## CI/CD evidence

### Pull-request checks

- Unit-test every pure rule with valid, invalid, empty, sentinel, duplicate, revision, and boundary fixtures.
- Validate stable rule IDs, severities, ownership, and object references.
- Exercise capture-to-silver-to-gold reconciliation in disposable PostgreSQL.
- Prove a blocking check prevents partial publication and warning checks do not rewrite data.
- Include data-quality SQL/assets in wheel, source distribution, and bootstrap/package parity checks.

### Scheduled checks

- Run credentialed source-contract probes separately from required pull-request checks.
- Record source availability and provider schema drift without conflating provider outages with repository regressions.
- Run expanded warehouse reconciliation and bounded anomaly evaluation.

### Release evidence

- Record candidate commit SHA, migration manifest checksum, quality rule-set version, assessed watermarks, rule totals by severity/status, unresolved quarantines, and approved exceptions.
- A release with blocking failures or unexplained missing configured scope is not promotable.

## Implementation tickets

### DQ-001 — Inventory grains, scopes, and quality contracts

- Catalog every raw-capture, control, silver, reference, gold, publisher, and serving object.
- Record declared grain, stable identity, required lineage, expected-scope source, freshness cadence, and valid empty behavior.
- Assign stable rule IDs and severities for every implemented source — ACS, BLS, FRED, PEP, CDC, FBI UCR, and USDA NASS — plus shared geography and the glossary.

**Acceptance:** Every published object has an owner, declared grain, expected-scope method, and at least one deterministic integrity rule.

### DQ-002 — Implement quality evidence and runner foundation

- Add `control.data_quality_run` and `control.data_quality_result` through the warehouse manifest.
- Implement a source-neutral rule/result interface with bounded evidence and secret-safe errors.
- Support partition-targeted, idempotent evaluation without mutating observations.

**Acceptance:** A fresh bootstrap can execute and persist pass/fail/warn/not-applicable results for a fixture source.

### DQ-003 — Implement lineage and layer reconciliation

- Add raw checksum/request reconciliation, capture-to-silver identity reconciliation, and silver-to-gold publication reconciliation.
- Add incomplete-run, orphan, duplicate, quarantine, and mixed-watermark checks.
- Gate affected partition publication on blocking rules.

**Acceptance:** Injected loss, duplication, orphan lineage, and partial publication each fail with exact bounded evidence while the prior gold partition remains available.

### DQ-004 — Implement source-specific coverage and validity

- Add ACS dataset/year/variable/geography checks.
- Add BLS program/series/request-chunk/period checks.
- Add FRED series/domain/frequency/freshness checks.
- Add PEP dataset/vintage/release-completeness/sentinel checks.
- Add CDC asset/release-watermark/suppression/quarantine checks.
- Add FBI UCR release/participation-coverage/aggregation-boundary/reported-vs-absent checks.
- Add USDA NASS release/slice-ledger/suppression-vocabulary/grain checks.
- Add shared-reference identity, relationship, resolution, and geometry checks, and publisher-registry reconciliation.

**Acceptance:** Complete and intentionally incomplete fixtures distinguish valid emptiness from missing configured work.

### DQ-005 — Add scheduled assessment and observability

- Add the independent warehouse-quality DAG and source/partition targeting.
- Publish queryable summaries for backlog, freshness, quarantine, historical gaps, and warning trends.
- Add documented operator queries and repair/reassessment workflow.

**Acceptance:** Operators can identify the failing rule, affected source/partition, latest good publication, and next repair action without reading worker logs.

### DQ-006 — Add plausibility baselines and review lifecycle

- Create source-aware warning baselines from certified history.
- Persist anomaly evidence and warning review state separately from source observations.
- Document promotion criteria before any anomaly rule becomes blocking.

**Acceptance:** Known extreme-but-valid fixtures warn without mutation or rejection, while deterministic invalid values still block or quarantine.

### DQ-007 — CI and deployment certification

- Add unit, disposable-database, DAG, packaging, scheduled-contract, and release-evidence coverage alongside implementation.
- Certify the beta warehouse after reset/re-ingestion using the candidate commit and complete configured scope.
- Record approved exceptions with owner, reason, affected scope, and expiration.

**Acceptance:** GitHub-hosted evidence and beta certification agree on rule-set version and contain no unresolved blocking result.

## Initial success criteria

- Every configured source has nonempty synchronized metadata and an explicit expected scope.
- Every successful capture is checksum-valid and traceable through silver; every published observation is traceable to an accepted capture.
- Required table grains have zero duplicates.
- No configured partition silently disappears between control, silver, and gold.
- Required observation geographies resolve, or affected records are explicitly quarantined.
- Freshness is evaluated using source-specific cadence.
- Provider nulls, sentinels, footnotes, suppression, zero, and valid values remain distinguishable.
- Anomalies are visible and reviewable but never silently corrected.
- A clean bootstrap followed by configured-history ingestion produces a stored quality certification tied to one immutable commit SHA.

## Completion gate (2026-08-31)

Every implementation ticket DQ-001 through DQ-007 is delivered, and both
residuals recorded against them are closed. The initial success criteria above
each map to a registered rule with executable evidence:

| Criterion | Rule evidence |
|---|---|
| Configured scope and synchronized metadata | DQ-ACS/BLS/FRED-002, DQ-PEP-003, DQ-FRED-002 registry reconciliation |
| Checksum-valid, traceable captures | DQ-SHARED-001, DQ-SHARED-002, DQ-SHARED-003 |
| Zero duplicates at required grains | per-source uniqueness rules plus DQ-FBI-004 |
| No partition silently disappears | DQ-003 layer reconciliation and the DQ-FBI-002 silver-to-gold count check |
| Geographies resolve or quarantine explicitly | DQ-SHARED-004 reference resolution accounting |
| Source-specific freshness cadence | DQ-005 cadence-driven assessment |
| Provider nulls, sentinels, suppression, and zero stay distinct | DQ-PEP-004, DQ-CDC-003, DQ-NASS-003, DQ-FBI-003 |
| Anomalies visible, never silently corrected | DQ-006, WARN-only with the review lifecycle |
| Certification tied to one immutable commit SHA | DQ-007 `certify_release` |

### Validation commands and results

Host: Windows 11, Python 3.11, disposable PostGIS 16 + PostGIS 3.5 on port
55432. Container: the pinned Airflow 2.9.3 + Python 3.11 image against the same
disposable service. Every database run started from a freshly created database.

| Command | Result |
|---|---|
| `pytest tests/integration/database -m "integration and database"`, excluding `legacy/` | 90 passed |
| the same selection in the pinned Airflow image | 93 passed |
| `pytest -m dag tests/dags` in the image | 117 passed, 1 skipped |
| `pytest tests/unit` on the host | 1060 passed, 33 errors — see below |
| `pytest -m "unit and api" tests/unit/api` | 129 passed |
| `ruff check .` and `ruff format --check .` | clean; 389 files already formatted |

Environment limitations, recorded rather than claimed as passing:

- The host's 33 unit errors are all `PermissionError: [WinError 5]` on the
  pytest temporary-directory root, an OS ACL on this machine that denies even
  removing the directory. Every affected file passes in the pinned container.
- `tests/integration/database/legacy/` was excluded: it performs live BLS and
  FRED pulls, the BLS daily quota is exhausted on the available key, and the
  live FRED pull populates `raw_fred` and makes the empty-warehouse quality
  checks fail. This exclusion matches every prior evidence entry in this plan.
- The required CI (`dag-parse`, `postgres-integration`, `etl-unit`, `coverage`,
  `lint`, `scheduler-image`) re-verifies on the pull request; none of it has
  been dispatched from this branch.

### Reviewer notes

Two behavior changes are worth a reviewer's explicit attention:

1. `CaptureControl.finish_run` now finalizes an aborted run's unfinished
   requests. Any code that relied on inspecting `planned`/`running` requests
   *after* a failed run will see terminal statuses instead. One in-repo
   injection depended on that and was repaired.
2. The monthly plausibility sweep reports `not_applicable` until a promotable
   release certification exists. On a warehouse that has never been certified
   this is a visible change from warnings to silence, and it is intentional —
   run `certify_release` to restore verdicts.

## Explicit non-goals

- Declaring differently defined provider series equal because their labels look similar.
- Treating cross-source disagreement as proof that one source is wrong.
- Inventing missing source observations through interpolation or aggregation.
- Storing mutable dashboard preferences or subjective business definitions in warehouse quality tables.
- Blocking publication solely because a value is statistically unusual.

