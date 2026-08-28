# ADR-0001: Data-layer ownership boundaries

- **Status:** Accepted
- **Date:** 2026-08-17
- **Accepted:** 2026-08-17
- **Decision owners:** Data engineering and data-product maintainers
- **Related work:** ARCH-001 through ARCH-007 in the [data-layer remediation plan](../plans/completed/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md)

## Context

The current source pipelines use the terms raw, silver, and gold inconsistently. In particular, the existing `raw_*.{source}_long` relations contain parsed and derived rows, mutable ingestion ledgers live beside source data, and source-specific gold DDL owns shared glossary objects and consumer policy. These are documented legacy boundaries, not the target contract.

## Decision

New work and migrations use these ownership boundaries:

| Boundary | Owns | Must not own |
| --- | --- | --- |
| Raw data plane | Append-only, lossless response payloads and capture metadata | Parsed observations, orchestration state, mutable watermarks |
| Control plane | Requests, runs, attempts, slices, retries, watermarks, errors, and quarantine state | Source payload content |
| Silver | Parsing, typing, null interpretation, reshaping, validation, deduplication/current-revision selection, conformance, and derived identifiers | Consumer policy or locally authored business meaning |
| Gold | Deterministic, data-derived products built from silver | Dashboard defaults, approvals, preferences, or locally authored governance commentary |
| Semantic/governance | Reviewed definitions, interpretations, caveats, comparison guidance, and ownership | ETL runtime dependencies or source facts that can be harvested |
| Serving | API/dashboard projections, compatibility fields, and consumer-specific defaults | Authority over raw, silver, or gold facts |

Raw capture metadata is limited to `capture_id`, retrieval time, request fingerprint, checksum, source identifier, endpoint, request parameters, HTTP status and selected headers, media type, payload schema/version, load/run identifiers, and source revision markers. Credentials and sensitive request headers are never captured.

A successful response must be committed to raw storage before source-specific parsing. Captures are append-only. Repeated request fingerprints may share content identity when checksums match, but a changed checksum is a distinct source response and must not erase previous content.

## Field-classification examples

| Source field or value | Raw capture | Silver interpretation | Gold/source-fact use | Control/semantic/serving use |
| --- | --- | --- | --- | --- |
| ACS response header order and value strings | Preserve exactly in the response array | Unpivot variables, interpret sentinels, type numeric values | Publish deterministic measure/grain facts | Request slice and retry state are control data |
| ACS `state`/`county` strings | Preserve exactly | Derive canonical `geo_id` | Publish normalized geography grain | Comparison guidance belongs in semantic docs |
| BLS `year`, `period`, `value`, `latest`, footnotes | Preserve original JSON representations | Type dates/values/booleans and normalize footnotes | Publish source-supported units and frequency | Attempt count and watermark are control data |
| BLS series identifier | Preserve exactly | Decode LAUS geography where the adapter contract supports it | Publish stable series identity and lineage | Display aliases are serving/preferences data |
| FRED `date`, `value`, `realtime_start`, `realtime_end` | Preserve exact strings, including `.` | Parse dates, interpret missing values, select the documented vintage | Publish deterministic duration and source lineage | Default chart or aggregation is serving policy |
| Source labels, units, and frequency metadata | Preserve the containing response | Validate and conform | Harvest when explicitly source-backed | Locally authored definitions remain semantic documentation |
| `dashboard_suitability`, `owner_team`, default aggregation | Not source payload metadata | Not allowed | Not allowed in data-product tables | Semantic/governance or serving configuration only |

## Legacy transition

`raw_census.acs_long`, `raw_bls.bls_long`, and `raw_fred.fred_long` are legacy parsed staging relations rather than immutable raw capture. Source execution ledgers are owned by the `control` schema; new adapters must use the shared capture/control foundation and may not copy the legacy parsed-raw layout.

Until ARCH-002 and ARCH-003 complete, the three existing source gold DDL files are an explicit legacy exception for shared glossary definitions and policy columns. Contract tests freeze that exception to those files; they reject expansion to a fourth source or a new policy-bearing gold DDL.

This repository is currently a beta prototype with no external end users. A full warehouse reset and source re-ingestion is an acceptable and preferred cutover when it is simpler than preserving legacy staging data or compatibility objects. Append-only means capture rows are not mutated during normal operation; it does not prohibit intentionally destroying and rebuilding a beta environment.

## Exception process

An exception is permitted only when the payload cannot be persisted safely without deterministic decoding, such as transport decompression. The change must:

1. document the source, endpoint, exact transformation, risk, owner, and expiry/remediation ticket in an ADR amendment;
2. preserve the original bytes or demonstrate canonical logical equivalence with an automated fixture test;
3. retain checksum and media/schema metadata for both the received and persisted representations when they differ; and
4. receive review from a data engineering maintainer before merge.

An exception may not authorize semantic inference, destructive replacement of an earlier capture, credential persistence, or control state inside the source payload.

## Enforcement

Repository architecture tests reject new source-specific DDL that defines shared `gold_glossary` objects, new raw DDL without an immutable lossless capture contract, and policy columns in new gold data-product DDL. The existing violations are narrowly allowlisted and are removed by ARCH-002 through ARCH-007.

## Consequences

Replay and parser correction are possible without another provider request, source jobs no longer own shared catalog policy, and consumer semantics can evolve independently of ingestion. The beta cutover removed parsed-raw compatibility relations after the explicit legacy inventory was exhausted.
