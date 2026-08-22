# Warehouse data-quality assessment plan

## Plan status

- **Status:** Approved planning artifact; implementation not started
- **Last updated:** 2026-08-19
- **Primary owner:** Shared warehouse reliability
- **Depends on:** Capture/control foundation, shared geography, and source-specific silver and gold contracts
- **Co-delivery requirement:** Every implementation ticket must update the relevant GitHub Actions, scheduled checks, and release evidence in the [CI/CD migration plan](./CICD_GITHUB_ACTIONS_MIGRATION_PLAN.md).

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
- Assign stable rule IDs and severities for ACS, BLS, FRED, and shared geography.

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
- Add shared-reference identity, relationship, resolution, and geometry checks.

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

## Explicit non-goals

- Declaring differently defined provider series equal because their labels look similar.
- Treating cross-source disagreement as proof that one source is wrong.
- Inventing missing source observations through interpolation or aggregation.
- Storing mutable dashboard preferences or subjective business definitions in warehouse quality tables.
- Blocking publication solely because a value is statistically unusual.

