# Data-layer design investigation and remediation tickets

## Decision summary

The investigation confirms two design defects and one boundary decision that should be made explicit before another source is added.

| Concern | Verdict | Why it matters before adding sources |
| --- | --- | --- |
| Shared `gold_glossary` objects and catalog values are repeated per pipeline | Confirmed | Four files can define the same shared objects, every source refresh rewrites catalog policy, and source/type constraints enumerate today's providers. A new source increases drift and deployment-order risk. |
| Subjective dashboard and aggregation rules live in gold | Confirmed as a separation-of-concerns problem | Gold is currently documented and implemented as an analytical serving layer, so business semantics in gold are not inherently a medallion-pattern violation. The defect is mixing source-derived metadata, semantic judgment, governance, and product policy in one contract and denormalizing that policy into observation tables. |
| Raw ingestion does not preserve source responses as received | Confirmed | All three observation pipelines coerce, reshape, derive, and replace records before raw persistence. The original observation responses cannot be replayed under corrected transformation logic. |

These tickets assume the desired local convention is:

```text
source API -> immutable raw response -> validated/conformed silver -> data-derived gold
                                                                  |
                                                                  v
                                                 independent glossary harvest
                                                                  |
                                                                  v
                                                            catalog/API

ingestion execution state ----------------------> control schema

analytics definitions --------------------> documentation site/repository
                                            (no ETL runtime dependency)
```

The schema names are less important than enforcing these ownership boundaries. In particular, ingestion ledgers are useful and should remain, but they are control-plane data rather than raw source data.

“Hands-off” means contract-driven discovery, harvesting, validation, and reconciliation. It does not mean guessing business meaning from column names. A source adapter must still expose a small, standard machine-readable contract, and interpretations that cannot be supported by source evidence must remain unknown or be authored outside the data layer.

## Repository evidence

### 1. Shared glossary ownership and provider coupling

- `sql/gold_contract/002_gold_glossary_schema.sql` calls itself the single source of truth, but each of `gold_acs.sql`, `gold_bls.sql`, and `gold_fred.sql` also creates shared `gold_glossary` tables and replaces the shared geography refresh procedure.
- Each source DDL seeds all three current source systems. Each source DAG applies only its source DDL at runtime, so the effective definition of a shared object depends on which source schema ran most recently.
- `dim_metric_catalog.source_object_type` is constrained to `ACS_VARIABLE`, `BLS_SERIES`, `FRED_SERIES`, and `COMPOSITE_VIEW`. This makes the shared contract require a DDL change for a new provider.
- ACS, BLS, and FRED each implement a separate `_seed_*_metric_catalog` statement. All three hardcode `owner_team = 'data-eng'` and `recommended_aggregation = 'LAST'`; FRED labels every metric `PUBLIC_SAFE`.
- The catalog upserts update policy fields on every source refresh, so a reviewed/manual policy change can be silently overwritten by ETL.

### 2. Mixed data and policy semantics

- `gold_glossary.dim_metric_catalog` combines identity and lineage with `dashboard_suitability`, `comparability_group`, `do_not_compare_with`, `recommended_aggregation`, `owner_team`, and `is_active`.
- Per-source report tables copy these catalog fields onto every observation during refresh.
- The API and web app filter directly on `dashboard_suitability`, which makes the database catalog a product feature-flag store as well as a data catalog.
- A single `recommended_aggregation` value is not expressive enough. Aggregability can differ across time and geography, and a safe default can differ by use case. `LAST` is currently assigned to ACS, BLS, and FRED regardless of measure semantics.
- Several gold enrichments are valid data-derived attributes and should not be discarded merely because they are calculated: normalized grain, duration, source identity, units, value type, and lineage can remain in the data product when they have deterministic source evidence.

### 3. Raw fidelity and mutability

- Census reshapes the API's wide response to long form, constructs `geo_id`, normalizes null sentinels, casts values to floating point, and derives `table_id` and `measure_type` before loading `raw_census.acs_long`.
- BLS casts year, value, and `latest`, serializes footnotes, and derives LAUS geography from the series identifier before loading `raw_bls.bls_long`.
- FRED parses dates, converts the source value string (including `.`) to numeric/null plus `is_missing`, and discards the original value representation before loading `raw_fred.fred_long`.
- All three loaders delete matching natural keys before copying replacements. This supports current-state idempotency, but it is not immutable raw history.
- BLS and FRED metadata sometimes retain `raw_metadata`; observation responses do not. Census observation and metadata paths do not provide an equivalent complete response archive.
- This contradicts the README contract that raw is both unmodified and immutable.


## Proposed tickets

### ARCH-001 — Adopt and enforce explicit layer contracts

**Priority:** P0  
**Size:** M  
**Blocks:** all other tickets and onboarding the next source

**Problem**

The repository uses “raw,” “silver,” and “gold” inconsistently. Without a written, executable contract, moving individual columns will only shift the ambiguity to the next source.

**Scope**

- Record an architecture decision defining:
  - raw data-plane storage: append-only, lossless source responses plus capture metadata;
  - control-plane storage: requests, runs, slices, retries, watermarks, and errors;
  - silver: parsing, typing, null interpretation, reshaping, deduplication/current-revision selection, conformance, and derived identifiers;
  - gold: deterministic, data-derived products built from silver;
  - semantic/governance policy: reviewed interpretation and consumer behavior;
  - serving: API- or dashboard-specific projections and defaults.
- Define which operational metadata may accompany a raw payload (`capture_id`, retrieval time, request fingerprint, checksum, source endpoint, HTTP status, and schema/version identifiers).
- Update the README and new-source checklist to use the decision consistently.
- Add repository contract tests that reject:
  - source DDL that creates or alters shared glossary objects;
  - a new raw observation table without a lossless payload or equivalent lossless source-shaped representation;
  - new dashboard-policy columns in gold data-product tables.

**Acceptance criteria**

- One approved decision document contains a field-classification matrix with representative ACS, BLS, and FRED fields.
- README claims match deployed behavior; “unmodified” and “immutable” are not claimed until the migration tickets deliver them.
- A documented exception process exists for deterministic decoding needed to persist a payload safely (for example transport decompression); exceptions must preserve the original bytes or logically equivalent payload.
- CI enforces the three boundary rules above.

**Non-goals**

- Renaming every existing schema solely to match another medallion vocabulary.
- Rebuilding the pipelines in this ticket.

### ARCH-002 — Build an independent, provider-extensible glossary harvest pipeline

**Priority:** P0  
**Size:** L  
**Depends on:** ARCH-001

**Problem**

Shared glossary DDL and source registry seeds are duplicated across source packages. Source pipelines also write directly to the shared catalog. `CREATE TABLE IF NOT EXISTS` hides definition drift, shared procedures use last-writer-wins replacement, and the shared catalog enumerates current provider object types. This creates a circular ownership model: source gold depends on glossary rows while source refreshes are also responsible for maintaining them.

**Scope**

- Make ordered migrations under one shared component the only owner of `gold_glossary` objects.
- Remove `CREATE`, `ALTER`, `DROP`, procedure replacement, and cross-provider seed statements for shared objects from source-specific DDL.
- Remove all source-pipeline writes to `gold_glossary`; source jobs must finish successfully without the glossary being present or current.
- Create a separate glossary DAG/job, deployment component, migration state, service account, and operational state. Each successful source-gold publication emits a publisher-ready event that triggers a harvest for that source only; the source job does not wait for, retry, or roll back because of the glossary run. Also run a periodic all-publisher reconciliation to recover missed events and detect staleness.
- Include `source_code`, publisher contract version, source-gold watermark, source run ID, and publication time in the trigger. The glossary must compare that watermark with its last successfully harvested watermark and treat duplicate or out-of-order triggers as idempotent no-ops.
- Define one versioned publisher contract that every `gold_*` source exposes, such as a standard catalog export view with stable keys, source-backed labels, units, grains, lineage, schema version, and source update watermark.
- Let the glossary job discover registered publishers and harvest their export contracts. Database introspection may supplement the contract for physical schema/lineage facts, but must not infer business meaning from names.
- Validate each publisher independently. A malformed or unavailable source export is quarantined/marked stale without rolling back other catalog updates or any source-gold pipeline.
- Reconcile catalog lifecycle safely: upsert by stable source key, record provenance and harvest watermarks, and mark missing entities stale/retired only after an explicit grace rule rather than immediately deleting them.
- Replace the closed `source_object_type` provider enumeration with a provider-neutral entity type, or a referenced extensible type registry.
- Derive source registry entries from the validated publisher contract. Do not make a source seed itself or every other source into the glossary.
- Keep source-specific bridge tables only where they are genuinely required. Prefer a generic source-object mapping if it preserves referential integrity without provider-specific schema changes.
- Add contract and integration tests for arbitrary fourth-source discovery, independent/idempotent source bootstraps, partial glossary failure, staleness, and eventual reconciliation.

**Refresh and scheduling model**

The glossary is not a monthly batch that waits for every provider. It is an independently operated projection of the latest successfully published contract from each source:

```text
ACS gold succeeds  ---- event(source=ACS,  watermark=A17) ---> harvest ACS only
BLS gold succeeds  ---- event(source=BLS,  watermark=B42) ---> harvest BLS only
FRED gold succeeds ---- event(source=FRED, watermark=F09) ---> harvest FRED only

periodic reconciliation ------------------------------------> inspect all publishers
```

For the repository's current Airflow 2.9 deployment, prefer a durable provider-neutral outbox in the control plane over a DAG definition that statically enumerates ACS, BLS, and FRED datasets. The final gold publication transaction appends a publisher-ready event; a frequently scheduled glossary DAG claims pending events and harvests only the named publishers. It marks an event processed only after that publisher's harvest commits. This lets a fourth registered publisher use the same path without changing the glossary DAG schedule. Dataset-aware scheduling or an external event bus may replace the polling trigger later without changing the publisher or harvest contracts.

This is orchestration coupling only: the event communicates that a new publisher version is available, but source gold is committed and available before the glossary processes it. Glossary retries and failures have their own status and never change the completed source publication. The periodic all-publisher reconciliation remains necessary as a backstop for operational repair and freshness checks; it is not the primary refresh path.

Harvest state is maintained per publisher, not as one global glossary watermark. Concurrent triggers for different sources may run independently. Runs for the same source must be serialized or protected by a source-scoped advisory lock, and each source's catalog changes must commit in its own transaction. Consequently, the glossary may legitimately show ACS harvested at 06:45, BLS at 08:10, and FRED from the prior run. Consumers see those per-source timestamps and freshness states rather than a misleading claim that the whole glossary is one atomic snapshot. If a consumer needs a coordinated cross-source release, that is a separate serving-snapshot concern and is not imposed on ingestion or glossary refresh.

**Acceptance criteria**

- Searching source-specific DDL finds no shared-object definition statements targeting `gold_glossary`.
- Searching source DAGs and transforms finds no inserts, updates, or deletes targeting `gold_glossary`.
- Shared migration hashes and ordering are tracked independently from ACS, BLS, and FRED migration hashes.
- Running source schema bootstraps in every permutation leaves the same source schemas and does not mutate the glossary.
- A fixture `gold_*` publisher that implements the contract is discovered and cataloged without modifying glossary code, base shared-table DDL, or another source package.
- Every harvested entity exposes publisher schema version, source watermark, harvest time, provenance, and freshness/staleness state.
- One unavailable or invalid publisher does not prevent valid publishers from refreshing and never affects source-gold availability.
- Re-running a harvest with unchanged publishers is idempotent; changed and retired entities reconcile deterministically.
- A source-gold DAG never depends on or waits for the glossary. The one-way availability flow is `gold_* publication -> glossary harvest -> catalog consumers`; glossary failure cannot change the successful status or availability of source gold.
- An ACS-only publication triggers or is reconciled by an ACS-only harvest without rewriting BLS or FRED catalog rows. The same behavior applies to every registered publisher.
- Duplicate and out-of-order publisher-ready events do not regress a source's harvested watermark, and concurrent events for different sources cannot roll back one another.
- Existing source codes and metric identifiers remain stable, or a compatibility migration and rollback plan is included.

**Non-goals**

- Eliminating the small publisher contract. Fully automatic semantic inference would be unreliable; automation begins after a source satisfies the standard contract.
- Moving presentation policy; that is ARCH-003.

### ARCH-003 — Separate data-derived metric metadata from semantic, governance, and serving policy

**Priority:** P0  
**Size:** L  
**Depends on:** ARCH-001, coordinated with ARCH-002

**Problem**

`dim_metric_catalog` is simultaneously a lineage catalog, semantic glossary, ownership register, publication approval list, and dashboard behavior configuration. Source ETL overwrites all of those concerns, and serving tables duplicate them per observation. Keeping analytical definitions in runtime data schemas also makes data availability depend on locally authored interpretation and expands the number of changes capable of breaking a data refresh.

**Scope**

- Define a core metric entity containing deterministic identity and source-derived properties only, such as source object key, display label from source, units, observed/supported grains, measure kind, and lineage.
- Harvest aggregation characteristics only when they are explicitly supported by source metadata. Represent unsupported characteristics as unknown; do not calculate or guess them from labels.
- Remove locally authored interpretation (`business_definition`, curated caveats, comparability rules), governance commentary, and product policy (`dashboard_suitability`, default visualization/aggregation) from data-layer tables.
- Create a separate, version-controlled analytics documentation area and publishing workflow for definitions, comparison guidance, dashboard recommendations, and ownership. Documents may link to stable catalog identifiers but are not inputs to raw, silver, gold, or glossary refreshes.
- If an application needs those definitions at runtime, publish a documentation/semantic API or cache as a separately deployable, failure-isolated concern. Data APIs must continue serving source-derived results when that optional enrichment is unavailable.
- Stop copying mutable policy fields into per-observation report tables. Join them at the serving boundary or expose a versioned serving view.
- Preserve API compatibility during migration, then update API models, query builders, frontend filters, and tests to use the new serving contract.

**Acceptance criteria**

- A source or glossary refresh contains no dependency on analytics documentation and cannot change locally authored analytical guidance.
- The warehouse contains no mutable locally authored business-definition, approval, dashboard-policy, or user-preference records; only stable identifiers and harvested source facts remain there.
- No per-observation gold table stores dashboard suitability, owner team, or a consumer default aggregation.
- Unknown aggregation behavior is exposed as unknown; it never silently defaults to `LAST` or another operation.
- Automated tests cover source-backed aggregation characteristics, unknown characteristics, and optional documentation enrichment failure.
- The application can display, propose, review, and publish a definition without granting the user warehouse or Git access.
- Personal and team preferences are stored outside both the warehouse and the versioned global-definition registry.
- API responses remain backward compatible for a documented deprecation window, or the breaking version is explicitly released and documented.
- Analytical documentation has review ownership and change history, while its publication or availability cannot fail a data-layer job.

**Non-goals**

- Removing deterministic gold derivations such as duration, normalized grains, or source-backed units.
- Claiming all semantic metadata is subjective; the ticket distinguishes source evidence from local judgment.
- Automatically generating authoritative analytical definitions from physical schema metadata.

#### Business-definition operating model

Business definitions are intentionally outside the data warehouse. The warehouse retains only stable catalog identifiers and automatically harvested, source-supported facts. Frequently changing definitions, interpretations, approvals, presentation defaults, and user preferences use a separate lifecycle and failure domain.

```text
gold_* publisher contracts
          |
          v
automatic gold_glossary harvest --------> stable metric IDs + source facts
          |
          v
generated definition drafts
          |
          v
curator workflow/UI ---------------------> versioned semantic documentation
                                                   |
                                                   v
                                      documentation site / semantic API artifact

team and personal preferences ----------> application configuration store
```

The semantic documentation system of record should be structured Markdown or YAML in version control, or an equivalent versioned content service. The application may provide forms that generate and submit these changes so users do not need database or Git access. A validation/publishing workflow produces documentation and, if needed, a read-only JSON/API artifact. Failure of that workflow must not block raw, silver, gold, glossary, or source-derived data API refreshes.

Each business definition should support:

- stable `metric_id` linking it to the harvested glossary entity;
- plain-language name, definition, intended use, and interpretation;
- limitations and comparison guidance;
- source citations and evidence for any analytical constraint;
- lifecycle state such as `draft`, `approved`, `deprecated`, or `needs_review`;
- owner, reviewer, version, effective date, and last-reviewed date.

The application should expose three distinct classes of fields:

| Class | Examples | Mutability and owner |
| --- | --- | --- |
| Harvested source facts | source identity, units, frequency, supported grains, physical lineage | Read-only; updated by the glossary harvest |
| Reviewed business definitions | meaning, intended use, limitations, comparison guidance | Curator-authored and approved in semantic documentation |
| User/team preferences | aliases, tags, collections, formatting, chart defaults, favorites, saved filters | Immediately editable in the application configuration store |

Reasonable self-service options include personal/team aliases, tags, collections, display formatting, default chart selection, saved filters, notes, favorites, and choosing among source-supported operations. Governed calculated metrics may be added later as separately versioned semantic objects with explicit formulas, dependencies, ownership, and tests.

End users must not be able to overwrite source identity, lineage, units, supported grains, or stable metric IDs. They also must not globally approve definitions, bypass analytical safety constraints, or change comparison guidance without the curator workflow. Arbitrary SQL and unrestricted aggregation overrides are not self-service configuration.

Resolution follows a strict precedence rule:

```text
harvested source facts > approved business definition > team preference > personal preference
```

Later layers may enrich display and workflow behavior but cannot rewrite earlier-layer facts. When no reviewed business definition exists, the application presents the harvested source description with a visible `not reviewed` status. It does not invent a definition or silently choose an aggregation.

For the first release, implement approved definitions, review status, aliases, tags, favorites, formatting, saved filters, and constrained operation choices. Defer collaborative calculated metrics until semantic versioning, dependency tracking, validation, and approval are proven.

### ARCH-004 — Add an immutable raw capture and control-plane foundation

**Priority:** P0  
**Size:** L  
**Depends on:** ARCH-001

**Problem**

There is no durable, lossless observation-response boundary. Corrected parsers require another network call, revisions replace prior raw rows, and operational ledgers share the raw namespace.

**Scope**

- Introduce a shared capture contract implemented per source, containing:
  - immutable response payload (JSON/bytes as appropriate);
  - source, endpoint, request parameters/fingerprint, retrieval timestamp, HTTP metadata, checksum, and load/run identifiers;
  - payload schema/media type and optional source revision markers.
- Persist a successful response before parsing it into silver.
- Make captures append-only. Define idempotency using request fingerprint plus payload checksum without erasing distinct revisions or retrieval events required for audit.
- Move or logically separate ingestion slices, run status, retries, watermarks, hashes, and errors into a control schema. Provide compatibility views while DAGs migrate.
- Define quarantine behavior for payloads that were captured successfully but fail validation/transformation.
- Provide reusable capture, replay, and lineage utilities for new source adapters.

**Concrete model and examples**

This ticket separates three records that are currently mixed together:

| Record | Example | Mutability and purpose |
| --- | --- | --- |
| Raw capture | Exact FRED HTTP response body plus request URL/parameters, headers, retrieval time, checksum, and run ID | Append-only evidence of what the provider returned |
| Silver data | Parsed observation date, numeric value, missing-value interpretation, and selected revision | Rebuildable when parser or normalization rules change |
| Control state | Slice status, attempt count, retry time, watermark, error summary, and quarantine status | Mutable orchestration state; not source data |

Today, a FRED response is parsed before persistence, `.` is interpreted, dates and numbers are coerced, and existing `raw_fred.fred_long` rows can be deleted and replaced. Under ARCH-004, the flow becomes:

```text
HTTP response
    -> append and commit capture envelope + untouched payload
    -> parse that capture into silver
    -> publish gold
```

For example, suppose FRED returns value `"3.1"` for January on Monday and revises it to `"3.2"` on Friday. Both response captures remain available with distinct checksums and retrieval times. Silver's documented revision rule selects Friday's value as current, while a replay can reproduce either historical interpretation. If a later code fix changes how missing value `"."` is handled, silver can be rebuilt from the captures without calling FRED again.

As a failure example, suppose BLS returns valid JSON but changes an observation field to an unexpected shape. The response is still committed to raw capture. Parsing then fails, a sanitized control/quarantine record points to the capture ID, and the prior silver/gold publication remains available. After the parser is fixed, an operator replays that capture; no provider request is required.

The current `raw_*.{source}_long` tables are therefore not the target immutable raw boundary: they contain already parsed, typed, long-form rows and behave like staging/silver data. During migration they may remain behind compatibility views, but new adapters must distinguish capture storage from mutable ingestion ledgers such as `*_ingestion_slices`.

**Acceptance criteria**

- An integration test captures a fixture response, disables network access, and rebuilds its silver output solely from the capture.
- Byte-for-byte or canonical logical equivalence to the fixture is demonstrable after retrieval from raw storage.
- Re-ingesting a changed response retains both versions and permits deterministic selection of a current revision in silver.
- Raw capture DML has no update/delete path in normal application roles.
- A parser failure retains the payload and records a sanitized quarantine/control event.
- A successful HTTP response is durably committed before source-specific parsing begins; a parser transaction cannot roll back its capture.
- Run/retry/watermark updates occur in the control plane and cannot mutate raw payload bytes or logical JSON content.
- Capture retention, access control, payload-size limits, and sensitive-data handling are documented before use by a new source.

**Non-goals**

- Migrating all existing source parsers; those are separate tickets.
- Treating request logs and retry state as source data.

### ARCH-005 — Migrate Census ACS normalization from raw ingestion to silver

**Priority:** P1  
**Size:** L  
**Depends on:** ARCH-004

**Scope**

- Store complete Census response arrays and request context in immutable raw capture.
- Move wide-to-long reshaping, null-sentinel interpretation, numeric typing, `geo_id` construction, and `table_id`/`measure_type` derivation into the Census silver transform.
- Preserve source strings and distinguish absent, blank, Census sentinel, and invalid numeric values through validation/quarantine evidence.
- Repoint gold and metadata flows to the migrated silver contract.
- Backfill captures where original payloads exist; explicitly label legacy normalized rows that cannot be reconstructed as lossless raw.

**Acceptance criteria**

- Representative and malformed Census fixtures replay from raw capture to the same intended silver business keys and values.
- Tests prove raw capture retains the original header order, value strings, and null sentinels.
- No Census raw-capture write path performs unpivoting, numeric conversion, or derived geography/table parsing.
- Cutover includes row-count, key, null/sentinel, and value reconciliation plus a rollback procedure.

### ARCH-006 — Migrate BLS normalization from raw ingestion to silver

**Priority:** P1  
**Size:** L  
**Depends on:** ARCH-004

**Scope**

- Store complete BLS API responses and request context in immutable raw capture.
- Move year/value/boolean coercion, footnote normalization, and LAUS geography parsing into the BLS silver transform.
- Retain the source `value`, `latest`, footnotes, and series payload representations for replay and future parser changes.
- Select current revisions in silver without deleting raw history.
- Backfill and label legacy limitations as in ARCH-005.

**Acceptance criteria**

- Representative fixtures replay without network access and reconcile to expected silver keys and values.
- A revised BLS observation leaves both captures queryable while silver deterministically exposes the selected revision.
- Geography parser changes can be replayed without another BLS request.
- No BLS raw-capture write path coerces observation fields or derives geography.

### ARCH-007 — Migrate FRED normalization from raw ingestion to silver

**Priority:** P1  
**Size:** L  
**Depends on:** ARCH-004

**Scope**

- Store complete FRED observation responses and request context in immutable raw capture.
- Move date parsing, numeric conversion, missing-sentinel interpretation, and current-vintage selection into the FRED silver transform.
- Retain exact `value`, `realtime_start`, and `realtime_end` source representations.
- Preserve all vintages/revisions and make silver's revision-selection rule explicit.
- Backfill and label legacy limitations as in ARCH-005.

**Acceptance criteria**

- Representative fixtures replay without network access and reconcile to expected silver outputs.
- The source `.` missing marker remains observable in raw while silver represents its interpreted state explicitly.
- Multiple realtime vintages remain queryable in raw and the selected silver vintage is deterministic and tested.
- No FRED raw-capture write path parses observation dates or numeric values.

## Delivery order and expansion gate

1. Complete ARCH-001 first so later migrations implement one agreed contract.
2. ARCH-002, ARCH-003, and ARCH-004 can then proceed independently, with ARCH-002 and ARCH-003 coordinating the removal of policy fields from the current catalog/API contract.
3. Complete ARCH-005 through ARCH-007 after the shared capture foundation is stable. The source migrations can be delivered one at a time.
4. Do not implement the next three sources until ARCH-002 through ARCH-004 are complete and at least one existing source migration has proven the adapter pattern end to end.
5. Require every new source to use the shared migration owner, raw capture/replay contract, silver normalization boundary, and separate serving-policy contract. Do not permit a “temporary” legacy path that would create a fourth migration later.

## Required migration SQL deliverables

Implementation must be performed through checked-in, ordered migration SQL rather than manual database edits. Each schema-changing ticket delivers a forward migration under `sql/migrations/` and, where rollback is safe, a companion rollback script. Expected artifacts are:

- `{sequence}_gold_glossary_decoupling.sql` for ARCH-002;
- `{sequence}_semantic_policy_extraction.sql` for ARCH-003;
- `{sequence}_raw_capture_control_foundation.sql` for ARCH-004;
- one source cutover migration for each of ARCH-005 through ARCH-007.

Each forward migration must include or coordinate:

- explicit precondition checks and the expected starting schema version;
- additive creation of replacement objects before consumer cutover;
- deterministic backfill with stable identifier preservation;
- row-count, key-uniqueness, null/sentinel, and relationship validation queries;
- compatibility views or a documented versioned API transition where needed;
- grants for ETL, glossary-harvest, API-readonly, and migration roles;
- migration-state recording and idempotent rerun behavior;
- deferred destructive cleanup in a later migration after the compatibility window;
- a rollback procedure or an explicit explanation when rollback would lose newly captured history.

ARCH-003 additionally requires a validated export of existing locally authored definitions before its SQL removes them from the active warehouse contract. The export becomes versioned semantic documentation; it is not re-imported as a runtime warehouse dependency. No production migration may rely on an operator copying ad hoc SQL from this plan into a console.

## Suggested completion checks

- Static architecture tests pass.
- Unit and integration suites pass for the migrated source.
- Offline raw-to-silver replay is demonstrated from repository fixtures.
- Old and new silver outputs reconcile for a representative window, with explained differences for corrected behavior.
- API contract tests pass through the policy split and compatibility period.
- Fresh bootstrap and upgrade-from-current-schema paths are both tested.
- Forward and rollback migration SQL are exercised against a copy of the current schema, including an interrupted/rerun scenario.
- Documentation no longer describes operational control tables as immutable source data.
